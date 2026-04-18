package main

import (
	"math/big"
	"sort"
	"sync"
)

const K = 20

type RoutingTable struct {
	nodeId  NodeId
	buckets [160][]*NetworkNode
	mu      sync.RWMutex
}

func NewRoutingTable(id NodeId) *RoutingTable {
	return &RoutingTable{nodeId: id}
}

func (rt *RoutingTable) bucketIndex(target NodeId) int {
	dist := getKeyDistance(rt.nodeId, target)
	if dist.BitLen() == 0 {
		return 0
	}
	return dist.BitLen() - 1
}

func (rt *RoutingTable) Add(node *NetworkNode) {
	if rt.nodeId == node.Id {
		return // не добавляем самого себя
	}
	rt.mu.Lock()
	defer rt.mu.Unlock()

	idx := rt.bucketIndex(node.Id)
	bucket := rt.buckets[idx]

	for i, n := range bucket {
		if n.Id == node.Id {
			rt.buckets[idx] = append(append(bucket[:i:i], bucket[i+1:]...), node)
			return
		}
	}

	if len(bucket) < K {
		rt.buckets[idx] = append(bucket, node)
	}
	// TODO: ping, eviction
}

type nodeDistance struct {
	node     *NetworkNode
	distance *big.Int
}

func (rt *RoutingTable) FindClosest(target NodeId, count int) []*NetworkNode {
	rt.mu.RLock()
	defer rt.mu.RUnlock()

	var all []nodeDistance
	for _, bucket := range rt.buckets {
		for _, n := range bucket {
			all = append(all, nodeDistance{n, getKeyDistance(n.Id, target)})
		}
	}
	sort.Slice(all, func(i, j int) bool {
		return all[i].distance.Cmp(all[j].distance) < 0
	})

	result := make([]*NetworkNode, 0, min(count, len(all)))
	for i := 0; i < len(all) && i < count; i++ {
		result = append(result, all[i].node)
	}
	return result
}
