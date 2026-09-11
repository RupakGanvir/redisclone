package resp

import (
	"bufio"
	"strconv"
)

// Writer encodes values as RESP and writes them to a buffered connection.
// Every method flushes, so callers don't need to think about buffering —
// this keeps the server code simple at a small perf cost that doesn't
// matter for a learning project.
type Writer struct {
	bw *bufio.Writer
}

func NewWriter(bw *bufio.Writer) *Writer {
	return &Writer{bw: bw}
}

func (w *Writer) WriteSimpleString(s string) error {
	if _, err := w.bw.WriteString("+" + s + "\r\n"); err != nil {
		return err
	}
	return w.bw.Flush()
}

func (w *Writer) WriteError(msg string) error {
	if _, err := w.bw.WriteString("-" + msg + "\r\n"); err != nil {
		return err
	}
	return w.bw.Flush()
}

func (w *Writer) WriteInteger(n int64) error {
	if _, err := w.bw.WriteString(":" + strconv.FormatInt(n, 10) + "\r\n"); err != nil {
		return err
	}
	return w.bw.Flush()
}

// WriteBulkString writes a $-prefixed string. Redis distinguishes an empty
// string ("") from a missing value (nil) — use WriteNil for the latter.
func (w *Writer) WriteBulkString(s string) error {
	if _, err := w.bw.WriteString("$" + strconv.Itoa(len(s)) + "\r\n" + s + "\r\n"); err != nil {
		return err
	}
	return w.bw.Flush()
}

func (w *Writer) WriteNil() error {
	if _, err := w.bw.WriteString("$-1\r\n"); err != nil {
		return err
	}
	return w.bw.Flush()
}

func (w *Writer) WriteNilArray() error {
	if _, err := w.bw.WriteString("*-1\r\n"); err != nil {
		return err
	}
	return w.bw.Flush()
}

// WriteArray writes the array header only; callers then write each element
// themselves (without an extra Flush per element, by using the *NoFlush
// variants) and must call Flush() once at the end. This lets us stream
// arrays of arbitrary values (mixed types) without allocating a big []any.
func (w *Writer) WriteArrayHeader(n int) error {
	_, err := w.bw.WriteString("*" + strconv.Itoa(n) + "\r\n")
	return err
}

func (w *Writer) WriteBulkStringNoFlush(s string) error {
	_, err := w.bw.WriteString("$" + strconv.Itoa(len(s)) + "\r\n" + s + "\r\n")
	return err
}

func (w *Writer) WriteIntegerNoFlush(n int64) error {
	_, err := w.bw.WriteString(":" + strconv.FormatInt(n, 10) + "\r\n")
	return err
}

func (w *Writer) Flush() error {
	return w.bw.Flush()
}

// WriteStringArray is a convenience helper for the common case: an array of
// bulk strings (e.g. the reply to LRANGE, KEYS, HGETALL).
func (w *Writer) WriteStringArray(items []string) error {
	if err := w.WriteArrayHeader(len(items)); err != nil {
		return err
	}
	for _, s := range items {
		if err := w.WriteBulkStringNoFlush(s); err != nil {
			return err
		}
	}
	return w.Flush()
}
