package main

import (
	"crypto/rand"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"
)

type DHTKey [20]byte
type NodeId = DHTKey

func (k DHTKey) MarshalJSON() ([]byte, error) {
	return json.Marshal(hex.EncodeToString(k[:]))
}

func (k *DHTKey) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}
	b, err := hex.DecodeString(s)
	if err != nil {
		return err
	}
	if len(b) != 20 {
		return fmt.Errorf("dhtkey: invalid length %d, want 20", len(b))
	}
	copy(k[:], b)
	return nil
}

func getKeyDistance(a, b DHTKey) *big.Int {
	x := new(big.Int).SetBytes(a[:])
	y := new(big.Int).SetBytes(b[:])
	return new(big.Int).Xor(x, y)
}

func generateKey() DHTKey {
	var k DHTKey
	_, _ = rand.Read(k[:])
	return k
}

func KeyFromString(s string) DHTKey {
	return sha1.Sum([]byte(s))
}

func KeyFromBytes(b []byte) DHTKey {
	return sha1.Sum(b)
}
