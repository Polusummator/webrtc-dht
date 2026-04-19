package dht

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
	ping    func(*NetworkNode) bool
}

func NewRoutingTable(id NodeId) *RoutingTable {
	return &RoutingTable{nodeId: id}
}

func (rt *RoutingTable) SetPing(fn func(*NetworkNode) bool) {
	rt.ping = fn
}

func (rt *RoutingTable) bucketIndex(target NodeId) int {
	dist := GetKeyDistance(rt.nodeId, target)
	if dist.BitLen() == 0 {
		return 0
	}
	return dist.BitLen() - 1
}

func (rt *RoutingTable) Add(node *NetworkNode) {
	if rt.nodeId == node.Id {
		return
	}
	rt.mu.Lock()

	idx := rt.bucketIndex(node.Id)
	bucket := rt.buckets[idx]

	for i, n := range bucket {
		if n.Id == node.Id {
			rt.buckets[idx] = append(append(bucket[:i:i], bucket[i+1:]...), node)
			rt.mu.Unlock()
			return
		}
	}

	if len(bucket) < K {
		rt.buckets[idx] = append(bucket, node)
		rt.mu.Unlock()
		return
	}

	lrs := bucket[0]
	rt.mu.Unlock()

	if rt.ping != nil && rt.ping(lrs) {
		rt.mu.Lock()
		b := rt.buckets[idx]
		for i, n := range b {
			if n.Id == lrs.Id {
				rt.buckets[idx] = append(append(b[:i:i], b[i+1:]...), lrs)
				break
			}
		}
		rt.mu.Unlock()
	} else {
		rt.mu.Lock()
		b := rt.buckets[idx]
		for i, n := range b {
			if n.Id == lrs.Id {
				rt.buckets[idx] = append(append(b[:i:i], b[i+1:]...), node)
				break
			}
		}
		rt.mu.Unlock()
	}
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
			all = append(all, nodeDistance{n, GetKeyDistance(n.Id, target)})
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
