# Building a Redis Clone in Go — Full Guide

This is the companion guide to the `redisclone` project. It covers why
this project is worth building, how it's put together, how to build it
yourself from scratch (in the right order), how to test and benchmark it,
what's deliberately left out, where to take it next, and how to actually
talk about it in an interview.

---

## 1. Why this project

A Redis clone is one of the highest-density projects you can put on a
resume for an undecided-role SWE candidate, because a *working* one
touches almost every CS fundamental in a single, coherent codebase:

- **Networking** — raw TCP, a custom wire protocol, connection handling
- **Concurrency** — many clients hitting shared state at once, safely
- **Data structures** — hash tables, linked-list-like sequences, sets, all
  implemented (or backed) by you, not a library
- **Systems/storage** — crash recovery, durability, the fsync trade-off
- **Distributed systems** (if you extend it) — replication, consensus

Every one of those is legible to *any* interviewer, whether they're
backend, infra, platform, or even a generalist. And because it's
incremental, you can stop at whatever depth matches your remaining time
and let your own interest — not a syllabus — decide where to go deeper.

---

## 2. Architecture overview

```
                     ┌─────────────────────────┐
   TCP clients  ───▶ │   internal/server        │
 (redis-cli, apps)   │   - Accept loop           │
                     │   - Command dispatch      │
                     │   - Pub/Sub delivery      │
                     └────────────┬─────────────┘
                                  │
                     ┌────────────▼─────────────┐
                     │   internal/resp           │
                     │   Reader / Writer          │
                     │   (wire protocol only)     │
                     └────────────┬─────────────┘
                                  │
                     ┌────────────▼─────────────┐
                     │   internal/store           │
                     │   RWMutex-protected map    │
                     │   strings/lists/hashes/sets │
                     │   TTL + active expiry sweep │
                     └────────────┬─────────────┘
                                  │
                     ┌────────────▼─────────────┐
                     │   internal/persistence     │
                     │   AOF: log + replay          │
                     └───────────────────────────┘
```

Each layer only knows about the one below it. `internal/resp` has zero
knowledge of Redis commands — it only knows how to encode/decode RESP
values. `internal/store` has zero knowledge of networking — it's a plain
Go data structure with a mutex. This separation is what let each layer be
unit tested in isolation before anything was wired together, and it's
the same separation you should be able to point to in an interview when
asked "how is this organized and why."

---

## 3. Build order — and why this order

Building it in this sequence means every stage is testable before you
add the next one, so bugs get caught close to their source instead of
surfacing three layers later as "the client got a weird reply."

### Stage 1 — RESP protocol (`internal/resp`)

Start here because *nothing* else can be tested without it. RESP is
simple: every value on the wire is one type-prefix byte followed by its
payload.

```
+OK\r\n                              simple string
-ERR unknown command\r\n             error
:1000\r\n                            integer
$5\r\nhello\r\n                      bulk string (length-prefixed)
$-1\r\n                              nil
*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n     array of bulk strings
```

Real clients always send commands as an **array of bulk strings** — e.g.
`SET foo bar` becomes `*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n`.
That's the one shape your reader must parse correctly; everything else
(simple strings, integers, nil) is only ever something *you* write back.

Write `Reader.ReadCommand()` and a `Writer` with one method per RESP
type, then unit test them against literal byte strings — no networking,
no store, just "does this byte sequence decode to this Go value and vice
versa." This is the cheapest possible place to catch off-by-one errors in
length prefixes, which is exactly the kind of bug that's a nightmare to
debug once it's hiding behind a live TCP connection.

### Stage 2 — the store (`internal/store`)

This is the heart of the project and the part worth spending the most
design thought on: **what's your concurrency model?**

Real Redis is single-threaded — one event loop, no locks, ever. It gets
away with this because it's fast enough that one core rarely bottlenecks,
and it completely sidesteps data races by construction. The trade-off is
that one slow operation (e.g. `KEYS *` on millions of keys) blocks every
other client until it finishes.

This project instead uses Go's native model: **one goroutine per
connection**, with a single `sync.RWMutex` guarding the entire keyspace
(`internal/store/store.go`). Reads take a read lock (many can proceed
concurrently); every write takes the exclusive write lock. It's simpler
to reason about than fine-grained locking, and Go's goroutines make
"one per connection" cheap enough that it doesn't matter at the scale
this project runs at.

The natural middle ground — and a great next step if you want to go
deeper — is **sharding**: split the keyspace across N maps, each with its
own mutex, and route each key to a shard via `hash(key) % N`. That
shrinks write contention roughly N-fold at the cost of no longer being
able to atomically lock the *whole* keyspace at once (which matters for
commands like `FLUSHALL` or `KEYS`).

Build the data types in this order, since each is genuinely independent
and testable on its own:
1. **Strings** — `SET`/`GET`/`INCR`, including the `NX`/`XX`/`EX` modifiers
2. **Expiry** — a `time.Time` field on each entry, checked lazily on
   every read (**passive expiry**) plus a background goroutine that
   sweeps the whole map periodically (**active expiry**) so keys nobody
   ever reads again still eventually free their memory
3. **Lists, Hashes, Sets** — same pattern each time: get-or-create the
   entry, type-check it (return a `WRONGTYPE` error if it's the wrong
   kind), mutate under the lock

Write `go test -race` for this package **before** moving on. Spin up 50
goroutines all hammering the same key and assert the final state is
correct — this is the single test most likely to catch a real bug, and
it's the test an interviewer is most likely to ask "did you write this?"

### Stage 3 — server + dispatch (`internal/server`)

Now wire a TCP accept loop to the store: `net.Listen`, then one goroutine
per accepted connection running a loop of `ReadCommand` → dispatch →
write reply. The dispatch table is just a `map[string]command` from
command name to handler function — genuinely no cleverer than that.

A subtlety worth knowing cold: **the writer needs its own mutex**, not
just the store. Once you add Pub/Sub (Stage 5), a `PUBLISH` from one
client's goroutine has to write directly into a *different* client's
socket — concurrently with that client's own goroutine possibly writing
a reply to something else. Two goroutines writing to the same
`net.Conn` at once will interleave bytes mid-message and corrupt the
protocol stream. The fix is a per-connection `sync.Mutex` that every
write path takes before touching the socket (see `clientConn.writeMu` in
`server.go`).

### Stage 4 — persistence (`internal/persistence`)

The elegant trick here: **log commands in the exact same RESP format
clients send them in.** `AOF.Append` just calls the same
`Writer.WriteStringArray` used to reply to clients, pointed at a file
instead of a socket. Replay on startup then just runs the file through
the same `Reader.ReadCommand()` used to parse live client input, calling
each command's effect directly against the store. One protocol, two
consumers — no separate serialization format to design or maintain.

Durability trade-off, made concrete: this project calls `fsync` after
*every* write (`appendfsync always`, in Redis's terminology) for maximum
durability and code simplicity. The cost is real — see the benchmark
numbers in Section 5. The standard production alternative is
`appendfsync everysec`: buffer writes and fsync once a second in the
background, trading "lose at most ~1 second of writes on a hard crash"
for an order of magnitude more throughput. That's a genuinely good
extension to implement yourself (see Section 7).

### Stage 5 — Pub/Sub (`internal/server/handlers_pubsub.go`)

`SUBSCRIBE key` registers the calling connection in a
`map[string]map[*clientConn]struct{}`; `PUBLISH` looks up that map and
writes the message directly to each subscriber's socket (through the
writer mutex from Stage 3). The one thing worth being deliberate about:
copy the subscriber list out from under the lock, then release the lock
*before* writing to sockets — you don't want a slow or stalled client
connection holding up every other subscriber's delivery, and you
definitely don't want to be holding a lock while doing blocking I/O.

### Stage 6 — tests and benchmarks

Two kinds of tests are worth having, and this project has both:
- **Unit tests** per package, testing the Go API directly (`store_test.go`,
  `aof_test.go`) — fast, precise, easy to pinpoint a failure
- **Integration tests** that dial a real TCP connection into a real
  running server and assert on exact reply *bytes*
  (`internal/server/server_test.go`) — slower, but they're the only
  tests that would catch a protocol-encoding bug, since unit tests on
  the Go API never touch the wire format at all

And once it's correct, benchmark it — both to have real numbers to talk
about, and because performance work only makes sense once correctness is
established. `redis-benchmark` (the real Redis project's own load-testing
tool) works against this server as-is, since it's a real RESP-speaking
client.

---

## 4. How to build, run, and test it yourself

```bash
# from the project root
go build -o bin/redisclone-server ./cmd/server

# start it (persistence on, writing to data.aof)
./bin/redisclone-server -addr=:6379 -aof=data.aof

# in another terminal — any real Redis client works, e.g. redis-cli
redis-cli -p 6379 SET foo bar
redis-cli -p 6379 GET foo
redis-cli -p 6379 RPUSH mylist a b c
redis-cli -p 6379 LRANGE mylist 0 -1
redis-cli -p 6379 SUBSCRIBE news &        # run in background
redis-cli -p 6379 PUBLISH news "hello"

# run the whole test suite, with the race detector
go test ./... -race
go vet ./...
gofmt -l .          # should print nothing
```

To prove persistence actually works, not just that the code compiles:
write some data, kill the server (`Ctrl-C` or `kill`), restart it with
the same `-aof` path, and confirm the data is still there. That's a far
more convincing demo than reading the code, and it's a two-minute thing
to do live in an interview if asked.

---

## 5. Benchmark results

Measured with `redis-benchmark` (the real Redis project's benchmarking
tool) against this server, 50 concurrent clients:

| Command | With AOF (`fsync` every write) | Without persistence |
|---|---|---|
| `SET` | ~5,500 req/sec | ~82,000 req/sec |
| `GET` | ~84,000 req/sec | ~90,000 req/sec |
| `LPUSH` | ~5,000 req/sec | *(not measured)* |

The **~15x drop in write throughput** with `fsync`-per-write persistence
enabled is the fsync/durability trade-off from Section 3, made concrete
with real numbers rather than just asserted. `GET` is barely affected
either way, because reads never touch the AOF at all — only writes do.
This table is worth having memorized in rough shape for an interview: it
turns "I know there's a durability/throughput trade-off" into "I
measured a 15x difference on my own implementation," which is a very
different (and much stronger) claim.

Your own numbers will vary with hardware — rerun `redis-benchmark -p
<port> -t set,get -n 20000 -c 50 -q` on your machine to get numbers you
can speak to directly rather than quoting these.

---

## 6. Known limitations

Being able to state these precisely — not just "it's not finished" — is
itself a signal of understanding. Worth knowing cold:

- **AOF logs relative-time commands verbatim.** If you `EXPIRE foo 100`
  and the server later replays the AOF (say, an hour after that write),
  a naive replay would give `foo` *another* 100 seconds from replay time
  — not the 100 seconds originally intended from the original write time.
  Real Redis solves this by rewriting relative-time commands to their
  absolute-time equivalent (`PEXPIREAT`, an absolute Unix timestamp)
  before logging. This project sidesteps the bug by simply not replaying
  `EXPIRE`/`PERSIST` (see `applyToStore` in `cmd/server/main.go`) rather
  than replaying it incorrectly — a deliberate, documented trade-off, not
  an oversight. Implementing the `PEXPIREAT` rewrite properly is a good,
  self-contained extension.
- **One global lock**, not sharded — see Section 3's concurrency
  discussion.
- **No RDB snapshotting** — only AOF. A large AOF file replays slowly on
  startup; real Redis mitigates this with periodic binary snapshots plus
  a smaller AOF since the last snapshot.
- **No replication, no cluster mode** — this is a single node with no
  fault tolerance beyond "restart it and replay the AOF."
- **No `MULTI`/`EXEC` transactions, no Lua scripting, no sorted sets, no
  streams, no `AUTH`/ACLs, no `maxmemory` eviction policy.**

---

## 7. Where to take it next

Roughly ordered by learning value for an undecided-role candidate. Pick
based on which one you're most curious about — that's a better signal of
fit than picking the "most impressive" one.

1. **`appendfsync everysec`** — buffer AOF writes, fsync on a ticker
   instead of per-write. Small change, and you get to re-run the
   benchmark from Section 5 and show the throughput recovering.
2. **Sharded locking** — replace the single `RWMutex` with N
   mutex-protected shards keyed by `hash(key) % N`. Forces you to think
   about which commands (`FLUSHALL`, `KEYS`) genuinely need a global lock
   and which don't.
3. **`PEXPIREAT` rewrite on AOF write** — fixes the limitation above
   properly, and is a nice narrow, well-defined bug to fix.
4. **RDB-style snapshotting** — periodically serialize the whole
   keyspace to a compact binary file, and truncate/compact the AOF after
   each snapshot (this is what real Redis's `BGREWRITEAOF` does).
5. **Sorted sets (`ZADD`/`ZRANGE`/`ZSCORE`)** — implement with a skip
   list, which is what real Redis itself uses. Great, self-contained
   data-structures deep-dive.
6. **Primary-replica replication** — a replica connects, receives the
   AOF as an initial sync, then a live stream of every subsequent write.
   This is the on-ramp into distributed systems: once you're here, `LLEN`
   on a replica raises real consistency questions (how stale can it be?).
7. **`maxmemory` + LRU eviction** — cap memory use and evict least-
   recently-used keys when full. Small in scope, but it's exactly the
   kind of "systems tax" real production caches have to pay.

---

## 8. Talking about this in interviews

Interviewers who see "Redis clone" on a resume tend to probe in a
specific, predictable order. Here's what to expect and where in this
codebase the honest answer lives:

**"Walk me through what happens when a client sends `SET foo bar`."**
Accept loop hands the connection to its own goroutine
(`server.go:handleConn`) → `resp.Reader.ReadCommand()` parses the RESP
array into `["SET", "foo", "bar"]` → `dispatch()` looks up `"SET"` in
`commandTable` → `cmdSet` parses any `EX`/`NX`/etc. modifiers and calls
`store.Set()` → if AOF is enabled and the command is marked as a write,
`dispatch()` appends it to the log → the handler writes `+OK\r\n` back
through the connection's writer.

**"How do you handle concurrent clients writing to the same key?"**
This is the concurrency-model question from Section 3 — have the
single-mutex-vs-sharding trade-off ready, and be honest that you chose
the simpler one and know what the more scalable one would look like.

**"What happens if the server crashes mid-write?"**
Depends on the fsync policy. With `appendfsync always` (what this
project does), the write is fsynced to disk before the client's reply is
even sent, so a crash immediately after can lose at most the in-flight
write that hadn't replied yet. This is also exactly the moment to
mention the `EXPIRE`/`PEXPIREAT` replay subtlety from Section 6 — it
shows you've thought about replay correctness, not just "does the file
get written."

**"Why Go instead of C/C++/Rust?"**
Goroutines make the "one goroutine per connection" model cheap enough
to not need an event loop or manual epoll/kqueue calls, which let the
project focus its complexity budget on the store's concurrency and the
persistence design rather than on connection-multiplexing mechanics. Be
upfront that this is also *why* it's not a fully fair comparison to
single-threaded, hand-tuned real Redis — different language, different
trade-offs, and that's fine to say plainly.

**"What would you do differently at scale?"** — this is your cue to walk
through Section 7 in priority order, and to explain *why* that's the
order (sharding before replication, because replication only matters
once a single node's throughput is actually the bottleneck).

The common thread across all of these: an interviewer isn't checking
whether you memorized Redis internals. They're checking whether you can
reason about the trade-off *you* made, out loud, with a concrete example
from code you actually wrote and tested — which is exactly what building
this incrementally, with tests at every stage, gives you the material to
do.

---

## 9. Full command reference

| Command | Arity | Notes |
|---|---|---|
| `PING` `[msg]` | 1–2 | replies `PONG` or echoes `msg` |
| `ECHO msg` | 2 | |
| `DEL key [key ...]` | 2+ | returns count actually deleted |
| `EXISTS key [key ...]` | 2+ | returns count that exist |
| `TYPE key` | 2 | `string`/`list`/`hash`/`set`/`none` |
| `KEYS pattern` | 2 | glob: `*` and `?` supported |
| `FLUSHALL` | 1 | wipes everything |
| `DBSIZE` | 1 | count of live keys |
| `EXPIRE key seconds` | 3 | relative TTL |
| `TTL key` | 2 | seconds left, `-1` no TTL, `-2` no key |
| `PERSIST key` | 2 | removes TTL |
| `GET key` | 2 | |
| `SET key val [EX s\|PX ms] [NX\|XX]` | 3+ | |
| `INCR key` / `INCRBY key n` | 2 / 3 | errors on non-integer values |
| `APPEND key val` | 3 | returns new length |
| `STRLEN key` | 2 | |
| `LPUSH`/`RPUSH key val [val ...]` | 3+ | returns new length |
| `LPOP`/`RPOP key [count]` | 2–3 | |
| `LRANGE key start stop` | 4 | negative indices supported |
| `LLEN key` | 2 | |
| `HSET key field val [field val ...]` | 4+ (even) | returns fields newly created |
| `HGET key field` | 3 | |
| `HDEL key field [field ...]` | 3+ | |
| `HGETALL key` | 2 | flat `[field, val, ...]` array |
| `HEXISTS key field` | 3 | |
| `HLEN key` | 2 | |
| `SADD key member [member ...]` | 3+ | returns members newly added |
| `SREM key member [member ...]` | 3+ | |
| `SMEMBERS key` | 2 | |
| `SISMEMBER key member` | 3 | |
| `SCARD key` | 2 | |
| `SUBSCRIBE channel [channel ...]` | 2+ | |
| `UNSUBSCRIBE [channel ...]` | 1+ | no args = leave all |
| `PUBLISH channel message` | 3 | returns subscriber count reached |
