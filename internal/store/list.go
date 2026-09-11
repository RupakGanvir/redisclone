package store

// getOrCreateList fetches the list at key, creating an empty one if it doesn't exist.
func (s *Store) getOrCreateList(key string) (*entry, error) {
	e := s.getLocked(key)
	if e == nil {
		e = &entry{typ: TypeList, list: []string{}}
		s.data[key] = e
		return e, nil
	}
	if e.typ != TypeList {
		return nil, ErrWrongType{}
	}
	return e, nil
}

// LPush prepends values (in the order given) to the list at key and returns the new length. If the key doesn't exist, it creates a new list.
func (s *Store) LPush(key string, values ...string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, err := s.getOrCreateList(key)
	if err != nil {
		return 0, err
	}
	for _, v := range values {
		e.list = append([]string{v}, e.list...)
	}
	return len(e.list), nil
}

func (s *Store) RPush(key string, values ...string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, err := s.getOrCreateList(key)
	if err != nil {
		return 0, err
	}
	e.list = append(e.list, values...)
	return len(e.list), nil
}

// LPop removes and returns up to count elements from the head. ok is false if the key doesn't exist.
func (s *Store) LPop(key string, count int) ([]string, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e := s.getLocked(key)
	if e == nil {
		return nil, false, nil
	}
	if e.typ != TypeList {
		return nil, false, ErrWrongType{}
	}
	if count > len(e.list) {
		count = len(e.list)
	}
	popped := append([]string{}, e.list[:count]...)
	e.list = e.list[count:]
	if len(e.list) == 0 {
		delete(s.data, key) // Redis deletes keys that become empty
	}
	return popped, true, nil
}

func (s *Store) RPop(key string, count int) ([]string, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e := s.getLocked(key)
	if e == nil {
		return nil, false, nil
	}
	if e.typ != TypeList {
		return nil, false, ErrWrongType{}
	}
	if count > len(e.list) {
		count = len(e.list)
	}
	n := len(e.list)
	popped := append([]string{}, e.list[n-count:]...)
	// reverse so multi-pop order matches Redis (most-recently-tail first)
	for i, j := 0, len(popped)-1; i < j; i, j = i+1, j-1 {
		popped[i], popped[j] = popped[j], popped[i]
	}
	e.list = e.list[:n-count]
	if len(e.list) == 0 {
		delete(s.data, key)
	}
	return popped, true, nil
}

// LRange returns elements from start to stop inclusive, supporting Redis's negative-index convention.
func (s *Store) LRange(key string, start, stop int) ([]string, error) {
	e := s.get(key)
	if e == nil {
		return []string{}, nil
	}
	if e.typ != TypeList {
		return nil, ErrWrongType{}
	}
	n := len(e.list)
	start = normalizeIndex(start, n)
	stop = normalizeIndex(stop, n)
	if start < 0 {
		start = 0
	}
	if stop >= n {
		stop = n - 1
	}
	if start > stop || n == 0 {
		return []string{}, nil
	}
	out := make([]string, stop-start+1)
	copy(out, e.list[start:stop+1])
	return out, nil
}

func (s *Store) LLen(key string) (int, error) {
	e := s.get(key)
	if e == nil {
		return 0, nil
	}
	if e.typ != TypeList {
		return 0, ErrWrongType{}
	}
	return len(e.list), nil
}

// normalizeIndex converts a possibly-negative Redis-style index into a zero-based positive index
func normalizeIndex(i, n int) int {
	if i < 0 {
		i = n + i
	}
	return i
}
