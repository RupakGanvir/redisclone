package server

import (
	"fmt"
	"strconv"
	"time"
)

func cmdPing(s *Server, c *clientConn, args []string) error {
	if len(args) >= 2 {
		c.replyBulkString(args[1])
	} else {
		c.replySimpleString("PONG")
	}
	return nil
}

func cmdEcho(s *Server, c *clientConn, args []string) error {
	if len(args) != 2 {
		return fmt.Errorf("wrong number of arguments for 'echo' command")
	}
	c.replyBulkString(args[1])
	return nil
}

func cmdDel(s *Server, c *clientConn, args []string) error {
	if len(args) < 2 {
		return fmt.Errorf("wrong number of arguments for 'del' command")
	}
	n := s.Store.Del(args[1:]...)
	c.replyInteger(int64(n))
	return nil
}

func cmdExists(s *Server, c *clientConn, args []string) error {
	if len(args) < 2 {
		return fmt.Errorf("wrong number of arguments for 'exists' command")
	}
	n := s.Store.Exists(args[1:]...)
	c.replyInteger(int64(n))
	return nil
}

func cmdType(s *Server, c *clientConn, args []string) error {
	if len(args) != 2 {
		return fmt.Errorf("wrong number of arguments for 'type' command")
	}
	c.replySimpleString(s.Store.Type(args[1]))
	return nil
}

func cmdKeys(s *Server, c *clientConn, args []string) error {
	if len(args) != 2 {
		return fmt.Errorf("wrong number of arguments for 'keys' command")
	}
	c.replyStringArray(s.Store.Keys(args[1]))
	return nil
}

func cmdFlushAll(s *Server, c *clientConn, args []string) error {
	s.Store.FlushAll()
	c.replySimpleString("OK")
	return nil
}

func cmdDBSize(s *Server, c *clientConn, args []string) error {
	c.replyInteger(int64(s.Store.DBSize()))
	return nil
}

func cmdExpire(s *Server, c *clientConn, args []string) error {
	if len(args) != 3 {
		return fmt.Errorf("wrong number of arguments for 'expire' command")
	}
	secs, err := strconv.Atoi(args[2])
	if err != nil {
		return fmt.Errorf("value is not an integer or out of range")
	}
	ok := s.Store.Expire(args[1], time.Duration(secs)*time.Second)
	if ok {
		c.replyInteger(1)
	} else {
		c.replyInteger(0)
	}
	return nil
}

func cmdTTL(s *Server, c *clientConn, args []string) error {
	if len(args) != 2 {
		return fmt.Errorf("wrong number of arguments for 'ttl' command")
	}
	c.replyInteger(s.Store.TTL(args[1]))
	return nil
}

func cmdPersist(s *Server, c *clientConn, args []string) error {
	if len(args) != 2 {
		return fmt.Errorf("wrong number of arguments for 'persist' command")
	}
	if s.Store.Persist(args[1]) {
		c.replyInteger(1)
	} else {
		c.replyInteger(0)
	}
	return nil
}
