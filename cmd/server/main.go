// Command server is the redisclone entry point: it loads any existing AOF
// file to rebuild state, then starts accepting client connections.
package main

import (
	"flag"
	"log"
	"strconv"
	"strings"

	"redisclone/internal/persistence"
	"redisclone/internal/server"
	"redisclone/internal/store"
)

func main() {
	addr := flag.String("addr", ":6379", "address to listen on")
	aofPath := flag.String("aof", "redisclone.aof", "path to the append-only file (empty string disables persistence)")
	flag.Parse()

	st := store.New()
	defer st.Close()

	var aof *persistence.AOF
	if *aofPath != "" {
		// Replay the existing AOF before accepting client connections.
		if err := replayAOF(*aofPath, st); err != nil {
			log.Fatalf("failed to replay AOF: %v", err)
		}
		var err error
		aof, err = persistence.Open(*aofPath)
		if err != nil {
			log.Fatalf("failed to open AOF for writing: %v", err)
		}
		defer aof.Close()
		log.Printf("persistence enabled: %s", *aofPath)
	} else {
		log.Printf("persistence disabled (running in-memory only)")
	}

	srv := server.New(st, aof)
	if err := srv.ListenAndServe(*addr); err != nil {
		log.Fatalf("server error: %v", err)
	}
}

// Replay persisted commands directly against the store during startup.
func replayAOF(path string, st *store.Store) error {
	count := 0
	err := persistence.Load(path, func(args []string) error {
		count++
		return applyToStore(st, args)
	})
	if err == nil && count > 0 {
		log.Printf("replayed %d commands from %s", count, path)
	}
	return err
}

// applyToStore applies persisted commands directly to the store. It is kept separate from the network command handlers.
func applyToStore(st *store.Store, args []string) error {
	if len(args) == 0 {
		return nil
	}
	name := strings.ToUpper(args[0])
	switch name {
	case "SET":
		if len(args) >= 3 {
			st.Set(args[1], args[2], store.SetOpts{})
		}
	case "DEL":
		if len(args) >= 2 {
			st.Del(args[1:]...)
		}
	case "INCR":
		if len(args) == 2 {
			_, err := st.IncrBy(args[1], 1)
			return err
		}
	case "INCRBY":
		if len(args) == 3 {
			delta, err := strconv.ParseInt(args[2], 10, 64)
			if err != nil {
				return err
			}
			_, err = st.IncrBy(args[1], delta)
			return err
		}
	case "APPEND":
		if len(args) == 3 {
			_, err := st.Append(args[1], args[2])
			return err
		}
	case "LPUSH":
		if len(args) >= 3 {
			_, err := st.LPush(args[1], args[2:]...)
			return err
		}
	case "RPUSH":
		if len(args) >= 3 {
			_, err := st.RPush(args[1], args[2:]...)
			return err
		}
	case "LPOP":
		if len(args) >= 2 {
			_, _, err := st.LPop(args[1], 1)
			return err
		}
	case "RPOP":
		if len(args) >= 2 {
			_, _, err := st.RPop(args[1], 1)
			return err
		}
	case "HSET":
		if len(args) >= 4 {
			pairs := make(map[string]string)
			for i := 2; i+1 < len(args); i += 2 {
				pairs[args[i]] = args[i+1]
			}
			_, err := st.HSet(args[1], pairs)
			return err
		}
	case "HDEL":
		if len(args) >= 3 {
			_, err := st.HDel(args[1], args[2:]...)
			return err
		}
	case "SADD":
		if len(args) >= 3 {
			_, err := st.SAdd(args[1], args[2:]...)
			return err
		}
	case "SREM":
		if len(args) >= 3 {
			_, err := st.SRem(args[1], args[2:]...)
			return err
		}
	case "FLUSHALL":
		st.FlushAll()
	case "EXPIRE", "PERSIST":
		// Relative expiry commands are skipped during AOF replay because their original TTL cannot be reconstructed safely after a restart.
	}
	return nil
}
