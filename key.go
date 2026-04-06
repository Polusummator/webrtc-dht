package main

import (
	"crypto/rand"
	"math/big"
)

type DHTKey []byte // todo: or big int

func getKeyDistance(key1 DHTKey, key2 DHTKey) *big.Int {
	buf1 := new(big.Int).SetBytes(key1)
	buf2 := new(big.Int).SetBytes(key2)
	result := new(big.Int).Xor(buf1, buf2)
	return result
}

func generateKey() DHTKey {
	k := make([]byte, 20)
	_, _ = rand.Read(k)
	return k
}
