package main

import "sync"

type MemoryStorage struct {
	mu   sync.RWMutex
	data map[string]*ValueMeta
}

func (s *MemoryStorage) Get(key DHTKey) *ValueMeta {
	s.mu.RLock()
	defer s.mu.RUnlock()

	v := s.data[string(key)]
	if v == nil {
		return nil
	}

	copyValue := *v
	if v.Data != nil {
		copyValue.Data = make([]byte, len(v.Data))
		copy(copyValue.Data, v.Data)
	}
	return &copyValue
}

func (s *MemoryStorage) Put(key DHTKey, value ValueMeta) {
	s.mu.Lock()
	defer s.mu.Unlock()

	copyValue := value
	if value.Data != nil {
		copyValue.Data = make([]byte, len(value.Data))
		copy(copyValue.Data, value.Data)
	}
	s.data[string(key)] = &copyValue
}

func (s *MemoryStorage) Delete(key DHTKey) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.data, string(key))
}

func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{
		data: make(map[string]*ValueMeta),
	}
}
