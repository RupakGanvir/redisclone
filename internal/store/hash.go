package store

func (s *Store) getOrCreateHash(key string) (*entry, error) {
	e := s.getLocked(key)
	if e == nil {
		e = &entry{typ: TypeHash, hash: make(map[string]string)}
		s.data[key] = e
		return e, nil
	}
	if e.typ != TypeHash {
		return nil, ErrWrongType{}
	}
	return e, nil
}

// HSet sets one or more field/value pairs and returns how many fields were
// newly created (as opposed to overwritten).
func (s *Store) HSet(key string, pairs map[string]string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, err := s.getOrCreateHash(key)
	if err != nil {
		return 0, err
	}
	created := 0
	for field, val := range pairs {
		if _, exists := e.hash[field]; !exists {
			created++
		}
		e.hash[field] = val
	}
	return created, nil
}

func (s *Store) HGet(key, field string) (string, bool, error) {
	e := s.get(key)
	if e == nil {
		return "", false, nil
	}
	if e.typ != TypeHash {
		return "", false, ErrWrongType{}
	}
	v, ok := e.hash[field]
	return v, ok, nil
}

func (s *Store) HDel(key string, fields ...string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e := s.getLocked(key)
	if e == nil {
		return 0, nil
	}
	if e.typ != TypeHash {
		return 0, ErrWrongType{}
	}
	n := 0
	for _, f := range fields {
		if _, ok := e.hash[f]; ok {
			delete(e.hash, f)
			n++
		}
	}
	if len(e.hash) == 0 {
		delete(s.data, key)
	}
	return n, nil
}

func (s *Store) HGetAll(key string) (map[string]string, error) {
	e := s.get(key)
	if e == nil {
		return map[string]string{}, nil
	}
	if e.typ != TypeHash {
		return nil, ErrWrongType{}
	}
	out := make(map[string]string, len(e.hash))
	for k, v := range e.hash {
		out[k] = v
	}
	return out, nil
}

func (s *Store) HExists(key, field string) (bool, error) {
	e := s.get(key)
	if e == nil {
		return false, nil
	}
	if e.typ != TypeHash {
		return false, ErrWrongType{}
	}
	_, ok := e.hash[field]
	return ok, nil
}

func (s *Store) HLen(key string) (int, error) {
	e := s.get(key)
	if e == nil {
		return 0, nil
	}
	if e.typ != TypeHash {
		return 0, ErrWrongType{}
	}
	return len(e.hash), nil
}
