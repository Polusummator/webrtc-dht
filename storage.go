package main

import (
	"errors"
)

var ErrNotFound = errors.New("not found")

type ValueMeta struct {
	Inline   bool   `json:"inline"`
	Data     []byte `json:"data,omitempty"` // Inline=true
	BlobRef  string `json:"blob_ref,omitempty"`
	BlobNode NodeId `json:"blob_node,omitempty"`
	Size     int64  `json:"size,omitempty"`
	MimeType string `json:"mime_type,omitempty"`
}

type Storage interface {
	Get(key DHTKey) *ValueMeta
	Put(key DHTKey, value ValueMeta)
	Delete(key DHTKey)
}

type BlobStore interface {
	Storage
	PutBlob(data []byte) (ref string, err error)
	GetBlob(ref string) ([]byte, error)
}
