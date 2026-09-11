package server

import "fmt"

// subscribe adds c to a channel's subscriber set and sends the confirmation message Redis clients expect:
// *3\r\n$9\r\nsubscribe\r\n$<channel>\r\n:<count>\r\n
func (s *Server) subscribe(c *clientConn, channel string) {
	s.subsMu.Lock()
	if s.subs[channel] == nil {
		s.subs[channel] = make(map[*clientConn]struct{})
	}
	s.subs[channel][c] = struct{}{}
	s.subsMu.Unlock()

	c.subMu.Lock()
	c.channels[channel] = true
	count := len(c.channels)
	c.subMu.Unlock()

	c.writeMu.Lock()
	c.w.WriteArrayHeader(3)
	c.w.WriteBulkStringNoFlush("subscribe")
	c.w.WriteBulkStringNoFlush(channel)
	c.w.WriteIntegerNoFlush(int64(count))
	c.w.Flush()
	c.writeMu.Unlock()
}

func (s *Server) unsubscribe(c *clientConn, channel string) {
	s.subsMu.Lock()
	if set, ok := s.subs[channel]; ok {
		delete(set, c)
		if len(set) == 0 {
			delete(s.subs, channel)
		}
	}
	s.subsMu.Unlock()

	c.subMu.Lock()
	delete(c.channels, channel)
	count := len(c.channels)
	c.subMu.Unlock()

	c.writeMu.Lock()
	c.w.WriteArrayHeader(3)
	c.w.WriteBulkStringNoFlush("unsubscribe")
	c.w.WriteBulkStringNoFlush(channel)
	c.w.WriteIntegerNoFlush(int64(count))
	c.w.Flush()
	c.writeMu.Unlock()
}

// unsubscribeAll is called when a connection closes, so it doesn't linger in every channel's subscriber set forever.
func (s *Server) unsubscribeAll(c *clientConn) {
	c.subMu.Lock()
	channels := make([]string, 0, len(c.channels))
	for ch := range c.channels {
		channels = append(channels, ch)
	}
	c.subMu.Unlock()

	s.subsMu.Lock()
	defer s.subsMu.Unlock()
	for _, ch := range channels {
		if set, ok := s.subs[ch]; ok {
			delete(set, c)
			if len(set) == 0 {
				delete(s.subs, ch)
			}
		}
	}
}

// publish sends payload to all current subscribers and returns the number of successful deliveries.
func (s *Server) publish(channel, payload string) int {
	s.subsMu.RLock()
	subscribers := s.subs[channel]
	targets := make([]*clientConn, 0, len(subscribers))
	for c := range subscribers {
		targets = append(targets, c)
	}
	s.subsMu.RUnlock()

	for _, c := range targets {
		c.writeMu.Lock()
		c.w.WriteArrayHeader(3)
		c.w.WriteBulkStringNoFlush("message")
		c.w.WriteBulkStringNoFlush(channel)
		c.w.WriteBulkStringNoFlush(payload)
		c.w.Flush()
		c.writeMu.Unlock()
	}
	return len(targets)
}

func cmdSubscribe(s *Server, c *clientConn, args []string) error {
	if len(args) < 2 {
		return fmt.Errorf("wrong number of arguments for 'subscribe' command")
	}
	for _, ch := range args[1:] {
		s.subscribe(c, ch)
	}
	return nil
}

func cmdUnsubscribe(s *Server, c *clientConn, args []string) error {
	if len(args) < 2 {
		// No-argument UNSUBSCRIBE means "leave every channel" in real Redis.
		c.subMu.Lock()
		channels := make([]string, 0, len(c.channels))
		for ch := range c.channels {
			channels = append(channels, ch)
		}
		c.subMu.Unlock()
		for _, ch := range channels {
			s.unsubscribe(c, ch)
		}
		return nil
	}
	for _, ch := range args[1:] {
		s.unsubscribe(c, ch)
	}
	return nil
}

func cmdPublish(s *Server, c *clientConn, args []string) error {
	if len(args) != 3 {
		return fmt.Errorf("wrong number of arguments for 'publish' command")
	}
	n := s.publish(args[1], args[2])
	c.replyInteger(int64(n))
	return nil
}
