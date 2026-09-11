package server

import "fmt"

func cmdSAdd(s *Server, c *clientConn, args []string) error {
	if len(args) < 3 {
		return fmt.Errorf("wrong number of arguments for 'sadd' command")
	}
	n, err := s.Store.SAdd(args[1], args[2:]...)
	if err != nil {
		return err
	}
	c.replyInteger(int64(n))
	return nil
}

func cmdSRem(s *Server, c *clientConn, args []string) error {
	if len(args) < 3 {
		return fmt.Errorf("wrong number of arguments for 'srem' command")
	}
	n, err := s.Store.SRem(args[1], args[2:]...)
	if err != nil {
		return err
	}
	c.replyInteger(int64(n))
	return nil
}

func cmdSMembers(s *Server, c *clientConn, args []string) error {
	if len(args) != 2 {
		return fmt.Errorf("wrong number of arguments for 'smembers' command")
	}
	members, err := s.Store.SMembers(args[1])
	if err != nil {
		return err
	}
	c.replyStringArray(members)
	return nil
}

func cmdSIsMember(s *Server, c *clientConn, args []string) error {
	if len(args) != 3 {
		return fmt.Errorf("wrong number of arguments for 'sismember' command")
	}
	ok, err := s.Store.SIsMember(args[1], args[2])
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

func cmdSCard(s *Server, c *clientConn, args []string) error {
	if len(args) != 2 {
		return fmt.Errorf("wrong number of arguments for 'scard' command")
	}
	n, err := s.Store.SCard(args[1])
	if err != nil {
		return err
	}
	c.replyInteger(int64(n))
	return nil
}
