package resp

import (
	"bufio"
	"strconv"
)

// Writer encodes values as RESP and writes them to a buffered connection.
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

// WriteBulkString writes a $-prefixed string. Redis distinguishes an empty string ("") from a missing value (nil)
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

// WriteArray writes the array header; callers write the elements and flush once.
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

// WriteStringArray is a convenience helper for the common case: an array of bulk strings.
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
