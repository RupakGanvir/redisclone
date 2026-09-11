// Package server ties the RESP protocol layer and the store together: it
// accepts TCP connections, reads commands off each one, dispatches them to
// handlers, and writes replies back.
package server

import (
	"bufio"
	"log"
	"net"
	"strings"
	"sync"
	"sync/atomic"

	"redisclone/internal/persistence"
	"redisclone/internal/resp"
	"redisclone/internal/store"
)

// Server owns the shared state every connection needs: the keyspace, the
// AOF log (nil if persistence is disabled), and pub/sub subscriber lists.
type Server struct {
	Store *store.Store
	aof   *persistence.AOF

	subsMu sync.RWMutex
	subs   map[string]map[*clientConn]struct{} // channel -> subscribers

	nextConnID atomic.Uint64
}

func New(st *store.Store, aof *persistence.AOF) *Server {
	return &Server{
		Store: st,
		aof:   aof,
		subs:  make(map[string]map[*clientConn]struct{}),
	}
}

// clientConn wraps one accepted connection. writeMu serializes writes to
// the socket: a client's own command loop writes replies, but *other*
// goroutines can also write to this same connection when delivering
// PUBLISH messages after this client has SUBSCRIBEd — writeMu is what
// keeps those two writers from interleaving mid-message.
type clientConn struct {
	id      uint64
	conn    net.Conn
	w       *resp.Writer
	writeMu sync.Mutex

	subMu    sync.Mutex
	channels map[string]bool // pub/sub channels this client is currently subscribed to
}

func (c *clientConn) subscribedCount() int {
	c.subMu.Lock()
	defer c.subMu.Unlock()
	return len(c.channels)
}

// ListenAndServe binds addr and serves connections until the listener
// errors (e.g. on shutdown).
func (s *Server) ListenAndServe(addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	defer ln.Close()
	log.Printf("redisclone listening on %s", addr)

	for {
		conn, err := ln.Accept()
		if err != nil {
			return err
		}
		go s.handleConn(conn)
	}
}

func (s *Server) handleConn(netConn net.Conn) {
	id := s.nextConnID.Add(1)
	c := &clientConn{
		id:       id,
		conn:     netConn,
		w:        resp.NewWriter(bufio.NewWriter(netConn)),
		channels: make(map[string]bool),
	}
	defer func() {
		netConn.Close()
		s.unsubscribeAll(c)
	}()

	r := resp.NewReader(bufio.NewReader(netConn))
	for {
		args, err := r.ReadCommand()
		if err != nil {
			return // client disconnected or sent garbage
		}
		if len(args) == 0 {
			continue
		}
		s.dispatch(c, args)
	}
}

// dispatch looks up the handler for args[0] and runs it. On success, if the
// command is a write command, it's appended to the AOF so it can be
// replayed on the next startup.
func (s *Server) dispatch(c *clientConn, args []string) {
	name := strings.ToUpper(args[0])
	h, ok := commandTable[name]
	if !ok {
		c.replyError("ERR unknown command '" + args[0] + "'")
		return
	}
	if err := h.fn(s, c, args); err != nil {
		c.replyError(errString(err))
		return
	}
	if h.isWrite && s.aof != nil {
		if err := s.aof.Append(args); err != nil {
			log.Printf("aof append failed: %v", err)
		}
	}
}

// --- write helpers (all take the writeMu lock, so they're safe to call
// from any goroutine, including PUBLISH delivery from a different
// connection's command loop) ---

func (c *clientConn) replySimpleString(s string) {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	c.w.WriteSimpleString(s)
}

func (c *clientConn) replyError(msg string) {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	c.w.WriteError(msg)
}

func (c *clientConn) replyInteger(n int64) {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	c.w.WriteInteger(n)
}

func (c *clientConn) replyBulkString(s string) {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	c.w.WriteBulkString(s)
}

func (c *clientConn) replyNil() {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	c.w.WriteNil()
}

func (c *clientConn) replyStringArray(items []string) {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	c.w.WriteStringArray(items)
}

// replyFlatPairs writes a map as a flat array [k1, v1, k2, v2, ...], the
// shape Redis uses for HGETALL.
func (c *clientConn) replyFlatPairs(m map[string]string) {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	c.w.WriteArrayHeader(len(m) * 2)
	for k, v := range m {
		c.w.WriteBulkStringNoFlush(k)
		c.w.WriteBulkStringNoFlush(v)
	}
	c.w.Flush()
}

func errString(err error) string {
	msg := err.Error()
	// Redis error replies conventionally start with an all-caps error
	// code word (ERR, WRONGTYPE, ...); our store errors already follow
	// that convention (see store.ErrWrongType), everything else gets a
	// generic ERR prefix.
	if strings.HasPrefix(msg, "WRONGTYPE") {
		return msg
	}
	return "ERR " + msg
}
