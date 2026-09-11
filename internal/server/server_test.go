package server

import (
	"bufio"
	"net"
	"strconv"
	"testing"
	"time"

	"redisclone/internal/resp"
	"redisclone/internal/store"
)

// testClient is a minimal RESP client used only by these tests, so we
// exercise the server exactly as a real client (redis-cli, ioredis, ...)
// would: over a real TCP socket, using the same wire protocol both ways.
type testClient struct {
	conn net.Conn
	br   *bufio.Reader
	r    *resp.Reader // for parsing array-framed replies (LRANGE, SUBSCRIBE, etc.)
	w    *resp.Writer
}

func newTestServer(t *testing.T) (addr string, srv *Server) {
	st := store.New()
	t.Cleanup(st.Close)
	srv = New(st, nil) // no AOF here — persistence has its own test in internal/persistence

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go srv.handleConn(conn)
		}
	}()
	t.Cleanup(func() { ln.Close() })
	return ln.Addr().String(), srv
}

func dialTestClient(t *testing.T, addr string) *testClient {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	br := bufio.NewReader(conn)
	return &testClient{
		conn: conn,
		br:   br,
		r:    resp.NewReader(br),
		w:    resp.NewWriter(bufio.NewWriter(conn)),
	}
}

// send encodes args the way every real client library does: an array of
// bulk strings.
func (tc *testClient) send(args ...string) {
	tc.w.WriteArrayHeader(len(args))
	for _, a := range args {
		tc.w.WriteBulkStringNoFlush(a)
	}
	tc.w.Flush()
}

// readLine reads one raw protocol line (for +simple/-error/:integer/$bulk
// replies, which aren't array-framed and so can't go through resp.Reader).
func (tc *testClient) readLine(t *testing.T) string {
	t.Helper()
	line, err := tc.br.ReadString('\n')
	if err != nil {
		t.Fatalf("readLine: %v", err)
	}
	return line
}

// readArray reads an array-framed reply where every element is a bulk
// string (true for LRANGE/SMEMBERS/HGETALL and pub/sub "message" pushes)
// by reusing resp.Reader, since that framing is identical to a client
// command's.
func (tc *testClient) readArray(t *testing.T) []string {
	t.Helper()
	args, err := tc.r.ReadCommand()
	if err != nil {
		t.Fatalf("readArray: %v", err)
	}
	return args
}

// readMixedArray reads an array-framed reply whose elements may be *either*
// bulk strings or integers — e.g. the SUBSCRIBE/UNSUBSCRIBE confirmation,
// which real Redis encodes as [bulk, bulk, integer]. resp.Reader can't be
// reused here since it only understands homogeneous bulk-string arrays
// (the shape every client *command* takes), so this is a small
// purpose-built parser for the reply side of the protocol.
func (tc *testClient) readMixedArray(t *testing.T) []string {
	t.Helper()
	header := tc.readLine(t) // e.g. "*3\r\n"
	if len(header) == 0 || header[0] != '*' {
		t.Fatalf("readMixedArray: expected array header, got %q", header)
	}
	n, err := strconv.Atoi(header[1 : len(header)-2])
	if err != nil {
		t.Fatalf("readMixedArray: bad array length %q: %v", header, err)
	}

	out := make([]string, 0, n)
	for i := 0; i < n; i++ {
		typeLine := tc.readLine(t)
		switch {
		case len(typeLine) > 0 && typeLine[0] == '$':
			size, err := strconv.Atoi(typeLine[1 : len(typeLine)-2])
			if err != nil {
				t.Fatalf("readMixedArray: bad bulk length %q: %v", typeLine, err)
			}
			valLine := tc.readLine(t) // value + trailing \r\n
			out = append(out, valLine[:size])
		case len(typeLine) > 0 && typeLine[0] == ':':
			out = append(out, typeLine[1:len(typeLine)-2])
		default:
			t.Fatalf("readMixedArray: unsupported element %q", typeLine)
		}
	}
	return out
}

func TestIntegration_StringCommands(t *testing.T) {
	addr, _ := newTestServer(t)
	c := dialTestClient(t, addr)

	c.send("SET", "foo", "bar")
	if got := c.readLine(t); got != "+OK\r\n" {
		t.Fatalf("SET reply: got %q, want +OK", got)
	}

	c.send("GET", "foo")
	if got := c.readLine(t); got != "$3\r\n" {
		t.Fatalf("GET length header: got %q", got)
	}
	if got := c.readLine(t); got != "bar\r\n" {
		t.Fatalf("GET value: got %q", got)
	}

	c.send("GET", "missing")
	if got := c.readLine(t); got != "$-1\r\n" {
		t.Fatalf("GET on missing key: got %q, want $-1", got)
	}

	c.send("INCR", "counter")
	if got := c.readLine(t); got != ":1\r\n" {
		t.Fatalf("INCR reply: got %q, want :1", got)
	}
}

func TestIntegration_ListsVisibleAcrossConnections(t *testing.T) {
	addr, _ := newTestServer(t)
	writer := dialTestClient(t, addr)
	reader := dialTestClient(t, addr)

	writer.send("RPUSH", "shared", "a", "b")
	writer.readLine(t) // :2\r\n

	// A second, independent connection should see the same server-side
	// state immediately — this is exactly what `go test -race` would catch
	// if the store's locking were broken.
	reader.send("LRANGE", "shared", "0", "-1")
	got := reader.readArray(t)
	want := []string{"a", "b"}
	if len(got) != 2 || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestIntegration_PubSub(t *testing.T) {
	addr, srv := newTestServer(t)
	sub := dialTestClient(t, addr)

	sub.send("SUBSCRIBE", "chan1")
	confirmation := sub.readMixedArray(t) // [bulk "subscribe", bulk "chan1", integer 1]
	if len(confirmation) != 3 || confirmation[0] != "subscribe" || confirmation[1] != "chan1" || confirmation[2] != "1" {
		t.Fatalf("unexpected subscribe confirmation: %v", confirmation)
	}

	n := srv.publish("chan1", "hello")
	if n != 1 {
		t.Fatalf("expected 1 subscriber to receive publish, got %d", n)
	}

	msg := sub.readArray(t) // "message" push is all bulk strings, unlike the confirmation above
	want := []string{"message", "chan1", "hello"}
	for i := range want {
		if msg[i] != want[i] {
			t.Fatalf("got %v, want %v", msg, want)
		}
	}
}

func TestIntegration_WrongTypeError(t *testing.T) {
	addr, _ := newTestServer(t)
	c := dialTestClient(t, addr)

	c.send("SET", "s", "v")
	c.readLine(t) // +OK

	c.send("LPUSH", "s", "x")
	line := c.readLine(t)
	if len(line) == 0 || line[0] != '-' {
		t.Fatalf("expected an error reply pushing to a string key, got %q", line)
	}
}

func TestIntegration_UnknownCommand(t *testing.T) {
	addr, _ := newTestServer(t)
	c := dialTestClient(t, addr)

	c.send("NOTACOMMAND", "x")
	line := c.readLine(t)
	if len(line) == 0 || line[0] != '-' {
		t.Fatalf("expected an error reply for an unknown command, got %q", line)
	}
}
