package resp

import (
	"bufio"
	"bytes"
	"reflect"
	"testing"
)

func TestReadCommand_Array(t *testing.T) {
	input := "*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"
	r := NewReader(bufio.NewReader(bytes.NewBufferString(input)))
	args, err := r.ReadCommand()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := []string{"SET", "foo", "bar"}
	if !reflect.DeepEqual(args, want) {
		t.Errorf("got %v, want %v", args, want)
	}
}

func TestReadCommand_Inline(t *testing.T) {
	input := "PING\r\n"
	r := NewReader(bufio.NewReader(bytes.NewBufferString(input)))
	args, err := r.ReadCommand()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := []string{"PING"}
	if !reflect.DeepEqual(args, want) {
		t.Errorf("got %v, want %v", args, want)
	}
}

func TestReadCommand_NullBulk(t *testing.T) {
	input := "*2\r\n$3\r\nGET\r\n$-1\r\n"
	r := NewReader(bufio.NewReader(bytes.NewBufferString(input)))
	args, err := r.ReadCommand()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := []string{"GET", ""}
	if !reflect.DeepEqual(args, want) {
		t.Errorf("got %v, want %v", args, want)
	}
}

func TestWriteRoundtrip(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(bufio.NewWriter(&buf))

	if err := w.WriteSimpleString("OK"); err != nil {
		t.Fatal(err)
	}
	if err := w.WriteError("ERR boom"); err != nil {
		t.Fatal(err)
	}
	if err := w.WriteInteger(42); err != nil {
		t.Fatal(err)
	}
	if err := w.WriteBulkString("hello"); err != nil {
		t.Fatal(err)
	}
	if err := w.WriteNil(); err != nil {
		t.Fatal(err)
	}
	if err := w.WriteStringArray([]string{"a", "b"}); err != nil {
		t.Fatal(err)
	}

	got := buf.String()
	want := "+OK\r\n" +
		"-ERR boom\r\n" +
		":42\r\n" +
		"$5\r\nhello\r\n" +
		"$-1\r\n" +
		"*2\r\n$1\r\na\r\n$1\r\nb\r\n"

	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestReadCommand_MultipleSequential(t *testing.T) {
	// Two commands back to back on the same stream, as a real connection would send them (pipelining).
	input := "*1\r\n$4\r\nPING\r\n*1\r\n$4\r\nPING\r\n"
	r := NewReader(bufio.NewReader(bytes.NewBufferString(input)))
	for i := 0; i < 2; i++ {
		args, err := r.ReadCommand()
		if err != nil {
			t.Fatalf("call %d: unexpected error: %v", i, err)
		}
		if !reflect.DeepEqual(args, []string{"PING"}) {
			t.Errorf("call %d: got %v", i, args)
		}
	}
}
