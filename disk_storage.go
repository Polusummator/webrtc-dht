package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

type DiskStorage struct {
	mu      sync.RWMutex
	metaDir string
	blobDir string
}

func NewDiskStorage(baseDir string) *DiskStorage {
	metaDir := filepath.Join(baseDir, "meta")
	blobDir := filepath.Join(baseDir, "blobs")
	_ = os.MkdirAll(metaDir, 0755)
	_ = os.MkdirAll(blobDir, 0755)
	return &DiskStorage{metaDir: metaDir, blobDir: blobDir}
}

func (s *DiskStorage) metaPath(key DHTKey) string {
	return filepath.Join(s.metaDir, hex.EncodeToString(key[:]))
}

func (s *DiskStorage) blobPath(ref string) string {
	return filepath.Join(s.blobDir, ref)
}

func (s *DiskStorage) Get(key DHTKey) *ValueMeta {
	s.mu.RLock()
	defer s.mu.RUnlock()

	data, err := os.ReadFile(s.metaPath(key))
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
	data, err := json.Marshal(value)
	if err != nil {
		return
	}
	_ = os.WriteFile(s.metaPath(key), data, 0644)
}

func (s *DiskStorage) Delete(key DHTKey) {
	s.mu.Lock()
	defer s.mu.Unlock()
	_ = os.Remove(s.metaPath(key))
}

func (s *DiskStorage) PutBlob(data []byte) (string, error) {
	sum := sha256.Sum256(data)
	ref := hex.EncodeToString(sum[:])

	s.mu.Lock()
	defer s.mu.Unlock()

	path := s.blobPath(ref)
	if _, err := os.Stat(path); err == nil {
		return ref, nil
	}
	if err := os.WriteFile(path, data, 0644); err != nil {
		return "", fmt.Errorf("putblob: %w", err)
	}
	return ref, nil
}

func (s *DiskStorage) GetBlob(ref string) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	data, err := os.ReadFile(s.blobPath(ref))
	if err != nil {
		return nil, ErrNotFound
	}
	return data, nil
}
