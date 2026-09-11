package server

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"redisclone/internal/store"
)

func cmdGet(s *Server, c *clientConn, args []string) error {
	if len(args) != 2 {
		return fmt.Errorf("wrong number of arguments for 'get' command")
	}
	v, ok, err := s.Store.GetString(args[1])
	if err != nil {
		return err
	}
	if !ok {
		c.replyNil()
		return nil
	}
	c.replyBulkString(v)
	return nil
}

// cmdSet handles SET key value with optional EX, PX, NX, or XX modifiers.
func cmdSet(s *Server, c *clientConn, args []string) error {
	if len(args) < 3 {
		return fmt.Errorf("wrong number of arguments for 'set' command")
	}
	key, value := args[1], args[2]
	opts := store.SetOpts{}

	for i := 3; i < len(args); i++ {
		switch strings.ToUpper(args[i]) {
		case "EX":
			if i+1 >= len(args) {
				return fmt.Errorf("syntax error")
			}
			secs, err := strconv.Atoi(args[i+1])
			if err != nil {
				return fmt.Errorf("value is not an integer or out of range")
			}
			opts.HasTTL = true
			opts.TTL = time.Duration(secs) * time.Second
			i++
		case "PX":
			if i+1 >= len(args) {
				return fmt.Errorf("syntax error")
			}
			ms, err := strconv.Atoi(args[i+1])
			if err != nil {
				return fmt.Errorf("value is not an integer or out of range")
			}
			opts.HasTTL = true
			opts.TTL = time.Duration(ms) * time.Millisecond
			i++
		case "NX":
			opts.OnlyIfNX = true
		case "XX":
			opts.OnlyIfXX = true
		default:
			return fmt.Errorf("syntax error")
		}
	}

	ok := s.Store.Set(key, value, opts)
	if !ok {
		c.replyNil() // NX/XX condition not met
		return nil
	}
	c.replySimpleString("OK")
	return nil
}

func cmdIncr(s *Server, c *clientConn, args []string) error {
	if len(args) != 2 {
		return fmt.Errorf("wrong number of arguments for 'incr' command")
	}
	v, err := s.Store.IncrBy(args[1], 1)
	if err != nil {
		return err
	}
	c.replyInteger(v)
	return nil
}

func cmdIncrBy(s *Server, c *clientConn, args []string) error {
	if len(args) != 3 {
		return fmt.Errorf("wrong number of arguments for 'incrby' command")
	}
	delta, err := strconv.ParseInt(args[2], 10, 64)
	if err != nil {
		return fmt.Errorf("value is not an integer or out of range")
	}
	v, err := s.Store.IncrBy(args[1], delta)
	if err != nil {
		return err
	}
	c.replyInteger(v)
	return nil
}

func cmdAppend(s *Server, c *clientConn, args []string) error {
	if len(args) != 3 {
		return fmt.Errorf("wrong number of arguments for 'append' command")
	}
	n, err := s.Store.Append(args[1], args[2])
	if err != nil {
		return err
	}
	c.replyInteger(int64(n))
	return nil
}

func cmdStrLen(s *Server, c *clientConn, args []string) error {
	if len(args) != 2 {
		return fmt.Errorf("wrong number of arguments for 'strlen' command")
	}
	n, err := s.Store.StrLen(args[1])
	if err != nil {
		return err
	}
	c.replyInteger(int64(n))
	return nil
}
