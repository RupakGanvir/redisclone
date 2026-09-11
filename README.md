# redisclone

A Redis server clone written from scratch in Go, using only the standard
library — no external dependencies. It implements the real RESP wire
protocol (a real `redis-cli` can talk to it), a concurrent in-memory store
with four data types and TTL expiry, AOF-based crash-recovery persistence,
and Pub/Sub.

Built as a learning project — see **GUIDE.md** for the full walkthrough of
how and why it's built this way, what's deliberately left out, and how to
talk about it in a systems-design interview.

## Quick start

```bash
go build -o bin/redisclone-server ./cmd/server
./bin/redisclone-server -addr=:6379 -aof=data.aof
```

Then, in another terminal, talk to it with any real Redis client:

```bash
redis-cli -p 6379 SET foo bar
redis-cli -p 6379 GET foo
redis-cli -p 6379 RPUSH mylist a b c
redis-cli -p 6379 LRANGE mylist 0 -1
```

Flags:
- `-addr` — address to listen on (default `:6380`)
- `-aof` — path to the append-only log file; pass `-aof=""` to disable
  persistence and run purely in-memory

## Project layout

```
cmd/server/            main.go — flags, startup, AOF replay on boot
internal/resp/         RESP protocol reader + writer (the wire format)
internal/store/        the in-memory keyspace: strings, lists, hashes,
                        sets, TTL/expiry, glob-pattern KEYS
internal/server/        TCP accept loop, command dispatch table,
                        per-type command handlers, Pub/Sub
internal/persistence/  AOF: append-on-write logging + startup replay
```

## Running the tests

```bash
go test ./...            # all unit + integration tests
go test ./... -race      # same, with Go's data-race detector
go vet ./...
gofmt -l .                # should print nothing
```

The `internal/server` package has end-to-end tests that spin up a real
server on an ephemeral port and drive it over an actual TCP socket using
the same RESP encoding a real client library would — not mocks.

## Supported commands

| Category | Commands |
|---|---|
| Connection | `PING`, `ECHO` |
| Generic | `DEL`, `EXISTS`, `TYPE`, `KEYS`, `FLUSHALL`, `DBSIZE`, `EXPIRE`, `TTL`, `PERSIST` |
| Strings | `GET`, `SET` (`EX`/`PX`/`NX`/`XX`), `INCR`, `INCRBY`, `APPEND`, `STRLEN` |
| Lists | `LPUSH`, `RPUSH`, `LPOP`, `RPOP` (with count), `LRANGE`, `LLEN` |
| Hashes | `HSET`, `HGET`, `HDEL`, `HGETALL`, `HEXISTS`, `HLEN` |
| Sets | `SADD`, `SREM`, `SMEMBERS`, `SISMEMBER`, `SCARD` |
| Pub/Sub | `SUBSCRIBE`, `UNSUBSCRIBE`, `PUBLISH` |

## What this is not

This isn't a drop-in Redis replacement — no RDB snapshots, no
replication, no cluster mode, no Lua scripting, no transactions
(`MULTI`/`EXEC`), no sorted sets, no `AUTH`/ACLs, no `maxmemory`
eviction. All of these are deliberately out of scope for a learning
project and are covered as extension ideas in GUIDE.md.
