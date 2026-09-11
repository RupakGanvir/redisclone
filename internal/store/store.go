// Package store implements the in-memory data structures used by the server,
// including strings, lists, hashes, sets, and TTL-based expiry.
package store

import (
	"sync"
	"time"
)

// ValueType identifies what kind of value is stored under a key, mirroring Redis's own type system.
type ValueType int

const (
	TypeString ValueType = iota
	TypeList
	TypeHash
	TypeSet
)

func (t ValueType) String() string {
	switch t {
	case TypeString:
		return "string"
	case TypeList:
		return "list"
	case TypeHash:
		return "hash"
	case TypeSet:
		return "set"
	default:
		return "none"
	}
}

// entry is what's actually stored per key: the value itself (in exactly one of the fields below, based on typ) plus an optional absolute expiry time.
type entry struct {
	typ       ValueType
	str       string
	list      []string
	hash      map[string]string
	set       map[string]struct{}
	expiresAt time.Time // zero value = no expiry
}

func (e *entry) hasExpiry() bool {
	return !e.expiresAt.IsZero()
}

func (e *entry) isExpired(now time.Time) bool {
	return e.hasExpiry() && now.After(e.expiresAt)
}

// Store is the whole keyspace. Zero value is not usable; use New().
type Store struct {
	mu   sync.RWMutex
	data map[string]*entry

	// stopSweep terminates the background active-expiry goroutine started by New(). Tests and short-lived callers should call Close().
	stopSweep chan struct{}
}

func New() *Store {
	s := &Store{
		data:      make(map[string]*entry),
		stopSweep: make(chan struct{}),
	}
	go s.activeExpiryLoop()
	return s
}

func (s *Store) Close() {
	close(s.stopSweep)
}

// activeExpiryLoop periodically removes expired keys that are not accessed.
func (s *Store) activeExpiryLoop() {
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-s.stopSweep:
			return
		case <-ticker.C:
			s.sweepExpired()
		}
	}
}

func (s *Store) sweepExpired() {
	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	for k, e := range s.data {
		if e.isExpired(now) {
			delete(s.data, k)
		}
	}
}

// getLocked fetches an entry and applies passive expiry. Caller must hold the appropriate lock.
func (s *Store) getLocked(key string) *entry {
	e, ok := s.data[key]
	if !ok {
		return nil
	}
	if e.isExpired(time.Now()) {
		delete(s.data, key)
		return nil
	}
	return e
}

// Get performs passive-expiry-aware lookup for read commands that need the raw entry (used internally by the command handlers in internal/server).
func (s *Store) get(key string) *entry {
	s.mu.Lock() // write lock: getLocked may delete an expired key
	defer s.mu.Unlock()
	return s.getLocked(key)
}

// Del removes one or more keys and returns how many actually existed.
func (s *Store) Del(keys ...string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, k := range keys {
		if e := s.getLocked(k); e != nil {
			delete(s.data, k)
			n++
		}
	}
	return n
}

// Exists returns how many of the given keys currently exist (unexpired).
func (s *Store) Exists(keys ...string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, k := range keys {
		if s.getLocked(k) != nil {
			n++
		}
	}
	return n
}

// Type returns the type of a key, or "none" if it doesn't exist.
func (s *Store) Type(key string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	e := s.getLocked(key)
	if e == nil {
		return "none"
	}
	return e.typ.String()
}

// Keys returns all keys matching a glob-style pattern ("*" for everything).
func (s *Store) Keys(pattern string) []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	var out []string
	for k, e := range s.data {
		if e.isExpired(now) {
			continue
		}
		if matchGlob(pattern, k) {
			out = append(out, k)
		}
	}
	return out
}

// FlushAll wipes the entire keyspace.
func (s *Store) FlushAll() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data = make(map[string]*entry)
}

// DBSize returns the number of live (unexpired) keys.
func (s *Store) DBSize() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	n := 0
	for _, e := range s.data {
		if !e.isExpired(now) {
			n++
		}
	}
	return n
}

// --- Expiry commands ---

// Expire sets a TTL (relative, in seconds) on a key. Returns false if the key doesn't exist.
func (s *Store) Expire(key string, ttl time.Duration) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	e := s.getLocked(key)
	if e == nil {
		return false
	}
	e.expiresAt = time.Now().Add(ttl)
	return true
}

// Persist removes a key's TTL, making it live forever. Returns false if the key doesn't exist or had no TTL to begin with.
func (s *Store) Persist(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	e := s.getLocked(key)
	if e == nil || !e.hasExpiry() {
		return false
	}
	e.expiresAt = time.Time{}
	return true
}

// TTL returns remaining seconds, or -1 if the key exists but has no expiry, or -2 if the key doesn't exist.
func (s *Store) TTL(key string) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	e := s.getLocked(key)
	if e == nil {
		return -2
	}
	if !e.hasExpiry() {
		return -1
	}
	remaining := time.Until(e.expiresAt)
	if remaining < 0 {
		return -2
	}
	return int64(remaining.Seconds())
}
