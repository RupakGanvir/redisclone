// Package resp implements the RESP (REdis Serialization Protocol).
//
// RESP is a text-ish, type-prefixed protocol. Every value on the wire starts
// with a single byte that says what kind of value follows. Using the
// character "P" as a stand-in for the prefix byte so this block isn't
// mistaken for a markdown list:
//
//	P='+'  Simple String   e.g. "+OK\r\n"
//	P='-'  Error           e.g. "-ERR unknown command\r\n"
//	P=':'  Integer         e.g. ":1000\r\n"
//	P='$'  Bulk String     e.g. "$5\r\nhello\r\n"   ($-1\r\n means nil)
//	P='*'  Array           e.g. "*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"
//
// Real clients (redis-cli, redis-py, ioredis, etc.) always send commands as
// an Array of Bulk Strings, e.g. `SET foo bar` becomes:
//
//	*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n
//
// We parse that shape strictly, but for convenience during manual testing
// (e.g. typing directly into `nc`) we also accept plain inline commands like
// "SET foo bar\r\n" with no type prefixes at all — real Redis does this too.
package resp

import (
	"bufio"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

var ErrProtocol = errors.New("resp: protocol error")

// Reader wraps a buffered connection and knows how to pull one full command
// (a slice of string arguments) off the wire at a time.
type Reader struct {
	br *bufio.Reader
}

func NewReader(br *bufio.Reader) *Reader {
	return &Reader{br: br}
}

// ReadCommand reads the next command from the stream and returns its
// arguments as plain strings, e.g. ["SET", "foo", "bar"].
func (r *Reader) ReadCommand() ([]string, error) {
	line, err := r.readLine()
	if err != nil {
		return nil, err
	}
	if len(line) == 0 {
		// Blank line — skip it and try again (some clients send \r\n keepalives).
		return r.ReadCommand()
	}

	if line[0] != '*' {
		// Not an array header: treat as an inline, space-separated command.
		// This lets you `nc localhost 6379` and type `PING` by hand.
		return splitInline(line), nil
	}

	// Array of bulk strings: *<n>\r\n ($<len>\r\n<bytes>\r\n){n}
	n, err := strconv.Atoi(line[1:])
	if err != nil {
		return nil, fmt.Errorf("%w: bad array length %q", ErrProtocol, line[1:])
	}
	if n < 0 {
		return []string{}, nil // null array, treat as empty command
	}

	args := make([]string, 0, n)
	for i := 0; i < n; i++ {
		bulkLine, err := r.readLine()
		if err != nil {
			return nil, err
		}
		if len(bulkLine) == 0 || bulkLine[0] != '$' {
			return nil, fmt.Errorf("%w: expected bulk string, got %q", ErrProtocol, bulkLine)
		}
		size, err := strconv.Atoi(bulkLine[1:])
		if err != nil {
			return nil, fmt.Errorf("%w: bad bulk length %q", ErrProtocol, bulkLine[1:])
		}
		if size < 0 {
			args = append(args, "") // null bulk string
			continue
		}
		buf := make([]byte, size+2) // +2 for trailing \r\n
		if _, err := readFull(r.br, buf); err != nil {
			return nil, err
		}
		args = append(args, string(buf[:size]))
	}
	return args, nil
}

// readLine reads up to \r\n (or \n) and returns the line without the
// terminator.
func (r *Reader) readLine() (string, error) {
	line, err := r.br.ReadString('\n')
	if err != nil {
		return "", err
	}
	line = strings.TrimRight(line, "\r\n")
	return line, nil
}

func readFull(br *bufio.Reader, buf []byte) (int, error) {
	total := 0
	for total < len(buf) {
		n, err := br.Read(buf[total:])
		total += n
		if err != nil {
			return total, err
		}
	}
	return total, nil
}

// splitInline splits a plain-text command line on whitespace. It doesn't
// try to handle quoted strings with embedded spaces — real clients never
// send inline commands, only humans testing by hand do.
func splitInline(line string) []string {
	fields := strings.Fields(line)
	return fields
}
