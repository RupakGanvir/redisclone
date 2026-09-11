// Package persistence implements Append-Only File (AOF) persistence: every
// write command is logged to disk in RESP format (the exact same wire
// format clients speak), and on startup the file is replayed command-by-
// command to reconstruct the in-memory state.
//
// This is deliberately the *simpler* of Redis's two persistence strategies
// (the other being RDB point-in-time binary snapshots). AOF is easier to
// reason about — "the file is just a recording of every write we ever
// received" — and it's what this project implements. The guide describes
// RDB snapshotting as a follow-up extension.
//
// Durability trade-off: we fsync after every single write for simplicity
// and maximum durability (equivalent to Redis's `appendfsync always`).
// Real deployments usually use `appendfsync everysec` (fsync once a second
// in the background) because fsync-per-write caps throughput at your disk's
// fsync latency, often just a few thousand ops/sec on spinning disks. See
// the guide for how to add that as an extension.
package persistence

import (
	"bufio"
	"os"
	"sync"

	"redisclone/internal/resp"
)

type AOF struct {
	mu   sync.Mutex
	file *os.File
	w    *resp.Writer // reused for its array/bulk-string encoding, not for replies
}

func Open(path string) (*AOF, error) {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}
	return &AOF{
		file: f,
		w:    resp.NewWriter(bufio.NewWriter(f)),
	}, nil
}

// Append logs one command as a RESP array of bulk strings, then fsyncs.
func (a *AOF) Append(args []string) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if err := a.w.WriteStringArray(args); err != nil { // also flushes the bufio.Writer
		return err
	}
	return a.file.Sync()
}

func (a *AOF) Close() error {
	return a.file.Close()
}

// Load replays every command previously logged to path, calling apply for
// each one. If path doesn't exist yet (first run), it's not an error.
func Load(path string, apply func(args []string) error) error {
	f, err := os.Open(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	defer f.Close()

	r := resp.NewReader(bufio.NewReader(f))
	for {
		args, err := r.ReadCommand()
		if err != nil {
			break // EOF (or a truncated last write) — stop replaying
		}
		if len(args) == 0 {
			continue
		}
		if err := apply(args); err != nil {
			// A single bad command shouldn't stop the whole replay; log
			// and continue would be ideal, but this is a training project
			// so we keep it visible by returning — callers can decide.
			return err
		}
	}
	return nil
}
