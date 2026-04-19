package dht

import "sync"

type MemoryStorage struct {
	mu   sync.RWMutex
	data map[DHTKey]*ValueMeta
}

func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{data: make(map[DHTKey]*ValueMeta)}
}

func (s *MemoryStorage) Get(key DHTKey) *ValueMeta {
	s.mu.RLock()
	defer s.mu.RUnlock()

	v := s.data[key]
	if v == nil {
		return nil
	}
	cp := *v
	if v.Data != nil {
		cp.Data = make([]byte, len(v.Data))
		copy(cp.Data, v.Data)
	}
	return &cp
}

func (s *MemoryStorage) Put(key DHTKey, value ValueMeta) {
	s.mu.Lock()
	defer s.mu.Unlock()

	cp := value
	if value.Data != nil {
		cp.Data = make([]byte, len(value.Data))
		copy(cp.Data, value.Data)
	}
	s.data[key] = &cp
}

func (s *MemoryStorage) Delete(key DHTKey) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.data, key)
}
