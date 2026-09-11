package server

import "fmt"

func cmdHSet(s *Server, c *clientConn, args []string) error {
	if len(args) < 4 || len(args)%2 != 0 {
		return fmt.Errorf("wrong number of arguments for 'hset' command")
	}
	pairs := make(map[string]string, (len(args)-2)/2)
	for i := 2; i+1 < len(args); i += 2 {
		pairs[args[i]] = args[i+1]
	}
	n, err := s.Store.HSet(args[1], pairs)
	if err != nil {
		return err
	}
	c.replyInteger(int64(n))
	return nil
}

func cmdHGet(s *Server, c *clientConn, args []string) error {
	if len(args) != 3 {
		return fmt.Errorf("wrong number of arguments for 'hget' command")
	}
	v, ok, err := s.Store.HGet(args[1], args[2])
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

func cmdHDel(s *Server, c *clientConn, args []string) error {
	if len(args) < 3 {
		return fmt.Errorf("wrong number of arguments for 'hdel' command")
	}
	n, err := s.Store.HDel(args[1], args[2:]...)
	if err != nil {
		return err
	}
	c.replyInteger(int64(n))
	return nil
}

func cmdHGetAll(s *Server, c *clientConn, args []string) error {
	if len(args) != 2 {
		return fmt.Errorf("wrong number of arguments for 'hgetall' command")
	}
	m, err := s.Store.HGetAll(args[1])
	if err != nil {
		return err
	}
	c.replyFlatPairs(m)
	return nil
}

func cmdHExists(s *Server, c *clientConn, args []string) error {
	if len(args) != 3 {
		return fmt.Errorf("wrong number of arguments for 'hexists' command")
	}
	ok, err := s.Store.HExists(args[1], args[2])
	if err != nil {
		return err
	}
	if ok {
		c.replyInteger(1)
	} else {
		c.replyInteger(0)
	}
	return nil
}

func cmdHLen(s *Server, c *clientConn, args []string) error {
	if len(args) != 2 {
		return fmt.Errorf("wrong number of arguments for 'hlen' command")
	}
	n, err := s.Store.HLen(args[1])
	if err != nil {
		return err
	}
	c.replyInteger(int64(n))
	return nil
}
