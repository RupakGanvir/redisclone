package store

import (
	"sync"
	"testing"
	"time"
)

func newTestStore(t *testing.T) *Store {
	s := New()
	t.Cleanup(s.Close)
	return s
}

func TestSetGet(t *testing.T) {
	s := newTestStore(t)
	s.Set("foo", "bar", SetOpts{})
	v, ok, err := s.GetString("foo")
	if err != nil || !ok || v != "bar" {
		t.Fatalf("got (%q, %v, %v), want (bar, true, nil)", v, ok, err)
	}
}

func TestGetMissingKey(t *testing.T) {
	s := newTestStore(t)
	_, ok, err := s.GetString("nope")
	if err != nil || ok {
		t.Fatalf("expected missing key to return ok=false, got ok=%v err=%v", ok, err)
	}
}

func TestSetNX(t *testing.T) {
	s := newTestStore(t)
	if ok := s.Set("k", "v1", SetOpts{OnlyIfNX: true}); !ok {
		t.Fatal("first NX set should succeed")
	}
	if ok := s.Set("k", "v2", SetOpts{OnlyIfNX: true}); ok {
		t.Fatal("second NX set should fail since key exists")
	}
	v, _, _ := s.GetString("k")
	if v != "v1" {
		t.Fatalf("value should be unchanged by failed NX set, got %q", v)
	}
}

func TestSetXX(t *testing.T) {
	s := newTestStore(t)
	if ok := s.Set("k", "v1", SetOpts{OnlyIfXX: true}); ok {
		t.Fatal("XX set on missing key should fail")
	}
	s.Set("k", "v1", SetOpts{})
	if ok := s.Set("k", "v2", SetOpts{OnlyIfXX: true}); !ok {
		t.Fatal("XX set on existing key should succeed")
	}
}

func TestExpiryPassive(t *testing.T) {
	s := newTestStore(t)
	s.Set("k", "v", SetOpts{HasTTL: true, TTL: 10 * time.Millisecond})
	if _, ok, _ := s.GetString("k"); !ok {
		t.Fatal("key should exist immediately after set")
	}
	time.Sleep(30 * time.Millisecond)
	if _, ok, _ := s.GetString("k"); ok {
		t.Fatal("key should have expired")
	}
}

func TestExpireAndTTL(t *testing.T) {
	s := newTestStore(t)
	s.Set("k", "v", SetOpts{})
	if s.TTL("k") != -1 {
		t.Fatal("key with no TTL should report -1")
	}
	if s.TTL("nope") != -2 {
		t.Fatal("missing key should report -2")
	}
	s.Expire("k", 100*time.Second)
	ttl := s.TTL("k")
	if ttl <= 0 || ttl > 100 {
		t.Fatalf("expected ttl in (0,100], got %d", ttl)
	}
	if !s.Persist("k") {
		t.Fatal("persist should succeed")
	}
	if s.TTL("k") != -1 {
		t.Fatal("ttl should be -1 after persist")
	}
}

func TestWrongType(t *testing.T) {
	s := newTestStore(t)
	s.Set("k", "v", SetOpts{})
	if _, err := s.LPush("k", "x"); err == nil {
		t.Fatal("expected WRONGTYPE error pushing to a string key")
	}
}

func TestIncrBy(t *testing.T) {
	s := newTestStore(t)
	v, err := s.IncrBy("counter", 1)
	if err != nil || v != 1 {
		t.Fatalf("got (%d, %v), want (1, nil)", v, err)
	}
	v, err = s.IncrBy("counter", 5)
	if err != nil || v != 6 {
		t.Fatalf("got (%d, %v), want (6, nil)", v, err)
	}
	s.Set("notanum", "abc", SetOpts{})
	if _, err := s.IncrBy("notanum", 1); err == nil {
		t.Fatal("expected error incrementing a non-numeric string")
	}
}

func TestListPushPopRange(t *testing.T) {
	s := newTestStore(t)
	s.RPush("mylist", "a", "b", "c")
	got, err := s.LRange("mylist", 0, -1)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"a", "b", "c"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got %v, want %v", got, want)
		}
	}

	s.LPush("mylist", "z")
	got, _ = s.LRange("mylist", 0, 0)
	if got[0] != "z" {
		t.Fatalf("LPUSH should prepend, got head %q", got[0])
	}

	popped, ok, err := s.LPop("mylist", 1)
	if err != nil || !ok || popped[0] != "z" {
		t.Fatalf("got (%v, %v, %v)", popped, ok, err)
	}
}

func TestListEmptyKeyIsDeleted(t *testing.T) {
	s := newTestStore(t)
	s.RPush("l", "only")
	s.LPop("l", 1)
	if s.Exists("l") != 0 {
		t.Fatal("list key should be deleted once it becomes empty, like real Redis")
	}
}

func TestHash(t *testing.T) {
	s := newTestStore(t)
	created, err := s.HSet("h", map[string]string{"f1": "v1"})
	if err != nil || created != 1 {
		t.Fatalf("got (%d, %v)", created, err)
	}
	v, ok, err := s.HGet("h", "f1")
	if err != nil || !ok || v != "v1" {
		t.Fatalf("got (%q, %v, %v)", v, ok, err)
	}
	if n, _ := s.HDel("h", "f1"); n != 1 {
		t.Fatalf("expected 1 field deleted, got %d", n)
	}
	if s.Exists("h") != 0 {
		t.Fatal("hash key should be deleted once empty")
	}
}

func TestSet(t *testing.T) {
	s := newTestStore(t)
	added, _ := s.SAdd("s", "a", "b", "a")
	if added != 2 {
		t.Fatalf("expected 2 distinct members added, got %d", added)
	}
	if card, _ := s.SCard("s"); card != 2 {
		t.Fatalf("expected cardinality 2, got %d", card)
	}
	if ok, _ := s.SIsMember("s", "a"); !ok {
		t.Fatal("expected a to be a member")
	}
}

func TestGlobMatch(t *testing.T) {
	cases := []struct {
		pattern, s string
		want       bool
	}{
		{"*", "anything", true},
		{"foo*", "foobar", true},
		{"foo*", "barfoo", false},
		{"f?o", "foo", true},
		{"f?o", "fooo", false},
		{"user:*:name", "user:42:name", true},
		{"user:*:name", "user:42:age", false},
	}
	for _, c := range cases {
		if got := matchGlob(c.pattern, c.s); got != c.want {
			t.Errorf("matchGlob(%q, %q) = %v, want %v", c.pattern, c.s, got, c.want)
		}
	}
}

// TestConcurrentAccess exercises the store with many goroutines hammering the same keys, run with `go test -race` to catch any data races.
func TestConcurrentAccess(t *testing.T) {
	s := newTestStore(t)
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			key := "counter"
			s.IncrBy(key, 1)
			s.RPush("list", "x")
			s.SAdd("set", "m")
			s.HSet("hash", map[string]string{"f": "v"})
		}(i)
	}
	wg.Wait()
	v, _, _ := s.GetString("counter")
	if v != "50" {
		t.Fatalf("expected counter=50 after 50 concurrent increments, got %s", v)
	}
}
