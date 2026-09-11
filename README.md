# RedisClone

A Redis-compatible in-memory data store built from scratch in Go.

I built RedisClone as a hands-on project to understand how a Redis-like server works internally instead of treating Redis as a black box. It implements the Redis Serialization Protocol (RESP), multiple in-memory data structures, TTL-based expiration, Append-Only File (AOF) persistence, and Pub/Sub.

## Features

- Redis-compatible RESP protocol
- Concurrent TCP server
- In-memory key-value store
- Strings, Lists, Hashes, and Sets
- TTL and key expiration
- Active expiration in a background goroutine
- Append-Only File (AOF) persistence
- AOF replay on startup
- Pub/Sub
- Thread-safe store using `sync.RWMutex`
- Unit and integration tests
- Race detector and `go vet` support

## Architecture

```text
                         TCP Clients
                             │
                             ▼
                    ┌─────────────────┐
                    │  Server Layer   │
                    │   TCP + Commands│
                    └────────┬────────┘
                             │
                             ▼
                    ┌─────────────────┐
                    │   RESP Layer    │
                    │ Reader / Writer │
                    └────────┬────────┘
                             │
                             ▼
                    ┌─────────────────┐
                    │   Store Layer   │
                    │ Strings / Lists │
                    │ Hashes / Sets   │
                    │ TTL / Expiry   │
                    └────────┬────────┘
                             │
                  ┌──────────┴──────────┐
                  ▼                     ▼
        ┌─────────────────┐   ┌─────────────────┐
        │   Persistence   │   │    Pub/Sub      │
        │       AOF       │   │  Subscriptions  │
        └─────────────────┘   └─────────────────┘
```

The server accepts TCP connections, parses commands using RESP, dispatches them to command handlers, and operates on the thread-safe in-memory store. Mutating commands can also be written to the AOF for persistence.

## Project Structure

```text
redisclone/
├── cmd/
│   └── server/
│       └── main.go
├── internal/
│   ├── persistence/
│   │   ├── aof.go
│   │   └── aof_test.go
│   ├── resp/
│   │   ├── reader.go
│   │   ├── writer.go
│   │   └── resp_test.go
│   ├── server/
│   │   ├── dispatch.go
│   │   ├── handlers_generic.go
│   │   ├── handlers_hash.go
│   │   ├── handlers_list.go
│   │   ├── handlers_pubsub.go
│   │   ├── handlers_set.go
│   │   ├── handlers_string.go
│   │   ├── server.go
│   │   └── server_test.go
│   └── store/
│       ├── glob.go
│       ├── hash.go
│       ├── list.go
│       ├── set.go
│       ├── store.go
│       ├── store_test.go
│       └── string.go
├── .gitignore
├── go.mod
└── README.md
```

## Getting Started

### Requirements

- Go 1.27+
- `redis-cli` for interactive testing

### Clone

```bash
git clone https://github.com/<your-username>/redis-clone-go.git
cd redis-clone-go
```

### Build

```bash
go build -o bin/redisclone-server ./cmd/server
```

On Windows:

```powershell
go build -o bin/redisclone-server.exe ./cmd/server
```

### Run

Start the server:

```bash
go run ./cmd/server -addr=:6379
```

To enable AOF persistence:

```bash
go run ./cmd/server -addr=:6379 -aof=data.aof
```

The server should start with:

```text
redisclone listening on :6379
```

## Using RedisClone

Connect using `redis-cli`:

```bash
redis-cli -h 127.0.0.1 -p 6379
```

Then try:

```text
127.0.0.1:6379> PING
PONG

127.0.0.1:6379> SET name rupak
OK

127.0.0.1:6379> GET name
"rupak"

127.0.0.1:6379> SET age 25
OK

127.0.0.1:6379> GET age
"25"
```

If `redis-cli` is running inside WSL while RedisClone is running on Windows, use the Windows host address instead of assuming `127.0.0.1` points to the Windows host.

## Supported Commands

### Connection

- `PING`

### Generic

- `DEL`
- `EXISTS`
- `EXPIRE`
- `PERSIST`
- `TTL`
- `KEYS`

### Strings

- `SET`
- `GET`
- `MGET`
- `MSET`
- `INCR`
- `DECR`
- `APPEND`
- `STRLEN`

### Lists

- `LPUSH`
- `RPUSH`
- `LPOP`
- `RPOP`
- `LRANGE`
- `LLEN`

### Hashes

- `HSET`
- `HGET`
- `HGETALL`
- `HDEL`
- `HEXISTS`
- `HLEN`

### Sets

- `SADD`
- `SREM`
- `SISMEMBER`
- `SMEMBERS`
- `SCARD`

### Pub/Sub

- `SUBSCRIBE`
- `UNSUBSCRIBE`
- `PUBLISH`

## TTL and Expiration

Keys can be assigned a time-to-live:

```text
SET session abc123
EXPIRE session 30
TTL session
```

RedisClone checks expiration when keys are accessed and also performs active expiration in the background.

Example:

```text
127.0.0.1:6379> SET temporary hello
OK

127.0.0.1:6379> EXPIRE temporary 10
(integer) 1

127.0.0.1:6379> TTL temporary
(integer) 10
```

After the TTL expires, the key is removed.

## AOF Persistence

RedisClone supports Append-Only File persistence.

Start the server with:

```bash
go run ./cmd/server -addr=:6379 -aof=data.aof
```

Mutating commands are appended to the AOF. When the server starts again with the same AOF file, the commands are replayed to rebuild the in-memory state.

For example:

```text
SET name rupak
SET age 25
```

Restart the server with the same AOF:

```bash
go run ./cmd/server -addr=:6379 -aof=data.aof
```

The values can then be queried again:

```text
GET name
GET age
```

Runtime AOF files are excluded from Git through `.gitignore`.

## Pub/Sub

RedisClone implements basic Redis-style Pub/Sub.

In one terminal:

```bash
redis-cli -h 127.0.0.1 -p 6379
```

Subscribe to a channel:

```text
SUBSCRIBE news
```

In another terminal:

```bash
redis-cli -h 127.0.0.1 -p 6379
```

Publish a message:

```text
PUBLISH news "hello from redisclone"
```

The subscribed client receives the published message.

## Concurrency

Each client connection is handled concurrently using goroutines.

The shared in-memory store is protected with `sync.RWMutex`, allowing concurrent reads while protecting mutations.

Pub/Sub also coordinates concurrent subscriptions, publications, and client writes.

## Testing

Run the complete test suite:

```bash
go test ./...
```

Run tests with the race detector:

```bash
go test ./... -race
```

Run static analysis:

```bash
go vet ./...
```

The repository includes tests for the RESP layer, store operations, persistence, and server behavior.

## Benchmarks

Some local benchmark results from the implementation:

| Operation |      With AOF |   Without AOF |
| --------- | ------------: | ------------: |
| `SET`     |  ~5,500 req/s | ~82,000 req/s |
| `GET`     | ~84,000 req/s | ~90,000 req/s |
| `LPUSH`   |  ~5,000 req/s |             — |

The results show the cost of synchronous AOF writes, particularly for write-heavy workloads. Reads are affected much less because they do not require an AOF write.

These are implementation-level benchmark results, not production-level Redis performance claims.

## Known Limitations

RedisClone is a learning-focused Redis implementation, not a production replacement for Redis.

Currently, it does not implement features such as:

- RDB snapshots
- Replication
- Redis Cluster
- Transactions such as `MULTI` / `EXEC`
- Lua scripting
- Sorted Sets
- Streams
- Authentication / ACL
- Memory limits such as `maxmemory`

There is also a known limitation around replaying `EXPIRE` / `PERSIST` operations from the AOF because relative TTL information cannot simply be replayed as if it were still valid after a restart.

## Future Improvements

Some areas I would like to explore further:

- More Redis data structures and commands
- Improved persistence semantics
- More extensive benchmarking
- Better command parsing and error handling
- Graceful server shutdown
- Connection and resource management improvements
- More comprehensive integration testing
- Additional Redis compatibility

## Why I Built This

I wanted to understand how an in-memory database works underneath a familiar interface.

Instead of using Redis as a black box, this project gave me an opportunity to work through:

- TCP networking
- Protocol parsing
- Command dispatch
- In-memory data structures
- Concurrency and synchronization
- TTL and expiration
- Persistence
- Pub/Sub
- Testing concurrent systems

The goal was not to recreate every part of Redis. It was to understand the engineering ideas behind a Redis-like server by implementing the important pieces myself.

## License

This project is licensed under the MIT License.
