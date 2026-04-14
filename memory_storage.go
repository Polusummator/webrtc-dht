package main

import "sync"

type Storage interface {
	Get(key DHTKey) []byte
	Put(key DHTKey, value []byte)
	Delete(key DHTKey)
}

type MemoryStorage struct {
	mu   sync.RWMutex
	data map[string][]byte
}

func (s *MemoryStorage) Get(key DHTKey) []byte {
	s.mu.RLock()
	defer s.mu.RUnlock()

	v := s.data[string(key)]
	if v == nil {
		return nil
	}

	copyValue := make([]byte, len(v))
	copy(copyValue, v)
	return copyValue
}

func (s *MemoryStorage) Put(key DHTKey, value []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()

	copyValue := make([]byte, len(value))
	copy(copyValue, value)
	s.data[string(key)] = copyValue
}

func (s *MemoryStorage) Delete(key DHTKey) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.data, string(key))
}

func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{
		data: make(map[string][]byte),
	}
}
