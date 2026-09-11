// Package persistence implements Append-Only File (AOF) persistence.
// Commands are stored in RESP format and replayed on startup to rebuild
// the in-memory state.

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

// Append writes a command in RESP format and syncs it to disk.
func (a *AOF) Append(args []string) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if err := a.w.WriteStringArray(args); err != nil {
		return err
	}
	return a.file.Sync()
}

func (a *AOF) Close() error {
	return a.file.Close()
}

// Load replays commands from the AOF and applies them one by one.
// A missing file is treated as an empty AOF.
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
			break
		}
		if len(args) == 0 {
			continue
		}
		if err := apply(args); err != nil {
			return err
		}
	}
	return nil
}
