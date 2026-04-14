package main

import (
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
)

type DiskStorage struct {
	mu      sync.RWMutex
	baseDir string
}

func NewDiskStorage(baseDir string) *DiskStorage {
	os.MkdirAll(baseDir, 0755)
	return &DiskStorage{
		baseDir: baseDir,
	}
}

func (s *DiskStorage) keyToPath(key DHTKey) string {
	encoded := hex.EncodeToString(key)
	return filepath.Join(s.baseDir, encoded)
}

func (s *DiskStorage) Get(key DHTKey) *ValueMeta {
	s.mu.RLock()
	defer s.mu.RUnlock()

	path := s.keyToPath(key)
	data, err := os.ReadFile(path)
	if err != nil {
		return nil
	}

	var meta ValueMeta
	if err := json.Unmarshal(data, &meta); err != nil {
		return nil
	}
	return &meta
}

func (s *DiskStorage) Put(key DHTKey, value ValueMeta) {
	s.mu.Lock()
	defer s.mu.Unlock()

	path := s.keyToPath(key)
	data, err := json.Marshal(value)
	if err != nil {
		return
	}
	_ = os.WriteFile(path, data, 0644)
}

func (s *DiskStorage) Delete(key DHTKey) {
	s.mu.Lock()
	defer s.mu.Unlock()

	path := s.keyToPath(key)
	_ = os.Remove(path)
}
