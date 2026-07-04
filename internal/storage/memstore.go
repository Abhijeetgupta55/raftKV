// Package storage holds the state machine of the store: the structure that
// ultimately owns the data. Later milestones wrap it with a write-ahead log
// and replicate it via Raft, so it must stay free of any networking,
// persistence, or consensus concerns — it is a map with a lock, on purpose.
package storage

import "sync"

// MemStore is a thread-safe in-memory key-value store.
type MemStore struct {
	mu   sync.RWMutex
	data map[string][]byte
}

func NewMemStore() *MemStore {
	return &MemStore{data: make(map[string][]byte)}
}

// Put stores value under key, overwriting any previous value.
func (s *MemStore) Put(key string, value []byte) {
	// Copy so a caller mutating its slice after Put can't corrupt the store.
	v := make([]byte, len(value))
	copy(v, value)

	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = v
}

// Get returns the value stored under key. found is false if the key is
// absent, which is distinct from a present key holding an empty value.
func (s *MemStore) Get(key string) (value []byte, found bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	v, ok := s.data[key]
	if !ok {
		return nil, false
	}
	// Copy so a caller mutating the returned slice can't corrupt the store.
	out := make([]byte, len(v))
	copy(out, v)
	return out, true
}

// Delete removes key and reports whether it was present. Deleting an
// absent key is a no-op, so Delete is idempotent.
func (s *MemStore) Delete(key string) (existed bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	_, ok := s.data[key]
	delete(s.data, key)
	return ok
}

// Range calls fn for every key/value pair until fn returns false, holding
// the read lock throughout. The value slice is the store's internal data:
// fn must not mutate or retain it. Used by snapshotting, which only
// streams the bytes to disk.
func (s *MemStore) Range(fn func(key string, value []byte) bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for k, v := range s.data {
		if !fn(k, v) {
			return
		}
	}
}

// Len returns the number of stored keys.
func (s *MemStore) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.data)
}
