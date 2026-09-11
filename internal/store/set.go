package store

func (s *Store) getOrCreateSet(key string) (*entry, error) {
	e := s.getLocked(key)
	if e == nil {
		e = &entry{typ: TypeSet, set: make(map[string]struct{})}
		s.data[key] = e
		return e, nil
	}
	if e.typ != TypeSet {
		return nil, ErrWrongType{}
	}
	return e, nil
}

// SAdd adds members and returns how many were newly added (Redis sets are
// deduplicated, so re-adding an existing member is a no-op).
func (s *Store) SAdd(key string, members ...string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, err := s.getOrCreateSet(key)
	if err != nil {
		return 0, err
	}
	added := 0
	for _, m := range members {
		if _, exists := e.set[m]; !exists {
			e.set[m] = struct{}{}
			added++
		}
	}
	return added, nil
}

func (s *Store) SRem(key string, members ...string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e := s.getLocked(key)
	if e == nil {
		return 0, nil
	}
	if e.typ != TypeSet {
		return 0, ErrWrongType{}
	}
	n := 0
	for _, m := range members {
		if _, ok := e.set[m]; ok {
			delete(e.set, m)
			n++
		}
	}
	if len(e.set) == 0 {
		delete(s.data, key)
	}
	return n, nil
}

func (s *Store) SMembers(key string) ([]string, error) {
	e := s.get(key)
	if e == nil {
		return []string{}, nil
	}
	if e.typ != TypeSet {
		return nil, ErrWrongType{}
	}
	out := make([]string, 0, len(e.set))
	for m := range e.set {
		out = append(out, m)
	}
	return out, nil
}

func (s *Store) SIsMember(key, member string) (bool, error) {
	e := s.get(key)
	if e == nil {
		return false, nil
	}
	if e.typ != TypeSet {
		return false, ErrWrongType{}
	}
	_, ok := e.set[member]
	return ok, nil
}

func (s *Store) SCard(key string) (int, error) {
	e := s.get(key)
	if e == nil {
		return 0, nil
	}
	if e.typ != TypeSet {
		return 0, ErrWrongType{}
	}
	return len(e.set), nil
}
