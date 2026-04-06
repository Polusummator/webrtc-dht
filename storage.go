package main

type Storage interface {
	Get(key DHTKey) []byte
	Put(key DHTKey, value []byte)
	Delete(key DHTKey)
}

// todo: mutex?
type MemoryStorage struct {
	data map[string][]byte
}

func (s *MemoryStorage) Get(key DHTKey) []byte {
	return s.data[string(key)]
}

func (s *MemoryStorage) Put(key DHTKey, value []byte) {
	s.data[string(key)] = value
}

func (s *MemoryStorage) Delete(key DHTKey) {
	delete(s.data, string(key))
}

func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{
		data: make(map[string][]byte),
	}
}
