package persistence

import (
	"path/filepath"
	"reflect"
	"testing"
)

func TestAOF_AppendAndReload(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.aof")

	aof, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	commands := [][]string{
		{"SET", "foo", "bar"},
		{"RPUSH", "list", "a", "b", "c"},
		{"INCR", "counter"},
	}
	for _, cmd := range commands {
		if err := aof.Append(cmd); err != nil {
			t.Fatal(err)
		}
	}
	if err := aof.Close(); err != nil {
		t.Fatal(err)
	}

	var replayed [][]string
	err = Load(path, func(args []string) error {
		replayed = append(replayed, args)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(replayed, commands) {
		t.Fatalf("got %v, want %v", replayed, commands)
	}
}

func TestAOF_LoadMissingFileIsNotAnError(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "does-not-exist.aof")
	called := false
	err := Load(path, func(args []string) error {
		called = true
		return nil
	})
	if err != nil {
		t.Fatalf("loading a missing AOF should not error, got %v", err)
	}
	if called {
		t.Fatal("apply should never be called when the file doesn't exist")
	}
}

func TestAOF_ArgsWithSpacesAndSpecialChars(t *testing.T) {
	// RESP bulk strings are length-prefixed, so values containing spaces, newlines, or other special characters should round-trip byte-for-byte.
	dir := t.TempDir()
	path := filepath.Join(dir, "test.aof")
	aof, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	tricky := []string{"SET", "key", "value with spaces\nand a newline\r\nand CRLF"}
	if err := aof.Append(tricky); err != nil {
		t.Fatal(err)
	}
	aof.Close()

	var got []string
	Load(path, func(args []string) error {
		got = args
		return nil
	})
	if !reflect.DeepEqual(got, tricky) {
		t.Fatalf("got %v, want %v", got, tricky)
	}
}
