package store

import (
	"fmt"
	"strconv"
	"time"
)

// ErrWrongType mirrors Redis's WRONGTYPE error: you tried to run a string/list/hash/set command against a key holding a different type.
type ErrWrongType struct{}

func (ErrWrongType) Error() string {
	return "WRONGTYPE Operation against a key holding the wrong kind of value"
}

// SetOpts contains the optional modifiers supported by SET.
type SetOpts struct {
	TTL      time.Duration // zero = no expiry
	HasTTL   bool
	OnlyIfNX bool // NX: only set if key does not exist
	OnlyIfXX bool // XX: only set if key already exists
}

// Set stores a string value with the given options. Returns false if an NX/XX condition prevents the write.
func (s *Store) Set(key, value string, opts SetOpts) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	existing := s.getLocked(key)
	if opts.OnlyIfNX && existing != nil {
		return false
	}
	if opts.OnlyIfXX && existing == nil {
		return false
	}

	e := &entry{typ: TypeString, str: value}
	if opts.HasTTL {
		e.expiresAt = time.Now().Add(opts.TTL)
	}
	s.data[key] = e
	return true
}

// Get returns the string value and whether the key exists (as a live string). Returns ErrWrongType if the key holds a non-string value.
func (s *Store) GetString(key string) (string, bool, error) {
	e := s.get(key)
	if e == nil {
		return "", false, nil
	}
	if e.typ != TypeString {
		return "", false, ErrWrongType{}
	}
	return e.str, true, nil
}

// Incr adds delta to the integer value at key, creating it if missing. Returns the new value or an error for invalid integers or wrong types.
func (s *Store) IncrBy(key string, delta int64) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	e := s.getLocked(key)
	if e == nil {
		e = &entry{typ: TypeString, str: "0"}
		s.data[key] = e
	}
	if e.typ != TypeString {
		return 0, ErrWrongType{}
	}
	cur, err := strconv.ParseInt(e.str, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("value is not an integer or out of range")
	}
	cur += delta
	e.str = strconv.FormatInt(cur, 10)
	return cur, nil
}

// Append adds a suffix to the string at key (creating it if missing) andreturns the new length.
func (s *Store) Append(key, suffix string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	e := s.getLocked(key)
	if e == nil {
		e = &entry{typ: TypeString}
		s.data[key] = e
	}
	if e.typ != TypeString {
		return 0, ErrWrongType{}
	}
	e.str += suffix
	return len(e.str), nil
}

// StrLen returns the length of the string at key, or 0 if it doesn't exist.
func (s *Store) StrLen(key string) (int, error) {
	e := s.get(key)
	if e == nil {
		return 0, nil
	}
	if e.typ != TypeString {
		return 0, ErrWrongType{}
	}
	return len(e.str), nil
}
