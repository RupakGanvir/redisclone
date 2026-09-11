package server

import (
	"fmt"
	"strconv"
)

func cmdLPush(s *Server, c *clientConn, args []string) error {
	if len(args) < 3 {
		return fmt.Errorf("wrong number of arguments for 'lpush' command")
	}
	n, err := s.Store.LPush(args[1], args[2:]...)
	if err != nil {
		return err
	}
	c.replyInteger(int64(n))
	return nil
}

func cmdRPush(s *Server, c *clientConn, args []string) error {
	if len(args) < 3 {
		return fmt.Errorf("wrong number of arguments for 'rpush' command")
	}
	n, err := s.Store.RPush(args[1], args[2:]...)
	if err != nil {
		return err
	}
	c.replyInteger(int64(n))
	return nil
}

func cmdLPop(s *Server, c *clientConn, args []string) error {
	return popHandler(s, c, args, s.Store.LPop)
}

func cmdRPop(s *Server, c *clientConn, args []string) error {
	return popHandler(s, c, args, s.Store.RPop)
}

// popHandler factors out the shared LPOP/RPOP logic: both take an optional
// count and reply with either a single bulk string, an array, or nil.
func popHandler(s *Server, c *clientConn, args []string, pop func(string, int) ([]string, bool, error)) error {
	if len(args) < 2 || len(args) > 3 {
		return fmt.Errorf("wrong number of arguments")
	}
	count := 1
	explicitCount := len(args) == 3
	if explicitCount {
		n, err := strconv.Atoi(args[2])
		if err != nil || n < 0 {
			return fmt.Errorf("value is not an integer or out of range")
		}
		count = n
	}
	popped, ok, err := pop(args[1], count)
	if err != nil {
		return err
	}
	if !ok {
		if explicitCount {
			c.replyStringArray([]string{})
		} else {
			c.replyNil()
		}
		return nil
	}
	if explicitCount {
		c.replyStringArray(popped)
	} else {
		c.replyBulkString(popped[0])
	}
	return nil
}

func cmdLRange(s *Server, c *clientConn, args []string) error {
	if len(args) != 4 {
		return fmt.Errorf("wrong number of arguments for 'lrange' command")
	}
	start, err := strconv.Atoi(args[2])
	if err != nil {
		return fmt.Errorf("value is not an integer or out of range")
	}
	stop, err := strconv.Atoi(args[3])
	if err != nil {
		return fmt.Errorf("value is not an integer or out of range")
	}
	items, err := s.Store.LRange(args[1], start, stop)
	if err != nil {
		return err
	}
	c.replyStringArray(items)
	return nil
}

func cmdLLen(s *Server, c *clientConn, args []string) error {
	if len(args) != 2 {
		return fmt.Errorf("wrong number of arguments for 'llen' command")
	}
	n, err := s.Store.LLen(args[1])
	if err != nil {
		return err
	}
	c.replyInteger(int64(n))
	return nil
}
