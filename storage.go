package main

import (
	"errors"
)

var ErrNotFound = errors.New("not found")

type ValueMeta struct {
	Inline   bool   `json:"inline"`
	Data     []byte `json:"data,omitempty"`
	BlobRef  string `json:"blob_ref,omitempty"`
	Size     int64  `json:"size,omitempty"`
	MimeType string `json:"mime_type,omitempty"`
}

type Storage interface {
	Get(key DHTKey) *ValueMeta
	Put(key DHTKey, value ValueMeta)
	Delete(key DHTKey)
}
