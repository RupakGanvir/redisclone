// Package resp implements encoding and decoding for the Redis Serialization Protocol.
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

// ReadCommand reads a RESP array of bulk strings.
func (r *Reader) ReadCommand() ([]string, error) {
	line, err := r.readLine()
	if err != nil {
		return nil, err
	}
	if len(line) == 0 {
		return r.ReadCommand()
	}

	if line[0] != '*' {
		// Support inline commands for manual TCP testing.
		return splitInline(line), nil
	}

	// Parse the array length from the RESP header.
	n, err := strconv.Atoi(line[1:])
	if err != nil {
		return nil, fmt.Errorf("%w: bad array length %q", ErrProtocol, line[1:])
	}
	if n < 0 {
		// Treat a null array as an empty command.
		return []string{}, nil
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
		buf := make([]byte, size+2)
		if _, err := readFull(r.br, buf); err != nil {
			return nil, err
		}
		args = append(args, string(buf[:size]))
	}
	return args, nil
}

// readLine reads a line and removes the line terminator.
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


func splitInline(line string) []string {
	fields := strings.Fields(line)
	return fields
}
