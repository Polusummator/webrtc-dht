package main

import (
	"math/big"
	"sort"
	"sync"
)

const K = 20

type RoutingTable struct {
	NodeId  NodeId
	Buckets [160][]*NetworkNode
	mutex   sync.RWMutex
}

func NewRoutingTable(Id NodeId) *RoutingTable {
	return &RoutingTable{
		NodeId: Id,
	}
}

func (rt *RoutingTable) bucketIndex(target NodeId) int {
	distance := getKeyDistance(DHTKey(rt.NodeId), DHTKey(target))
	bitLen := distance.BitLen()
	if bitLen == 0 {
		return 0
	}
	return bitLen - 1
}

func (rt *RoutingTable) Add(node *NetworkNode) {
	if string(rt.NodeId) == string(node.Id) {
		return
	}
	rt.mutex.Lock()
	defer rt.mutex.Unlock()
	idx := rt.bucketIndex(node.Id)
	bucket := rt.Buckets[idx]
	for i, n := range bucket {
		if string(n.Id) == string(node.Id) {
			rt.Buckets[idx] = append(append(bucket[:i], bucket[i+1:]...), node)
			return
		}
	}
	if len(bucket) < K {
		rt.Buckets[idx] = append(bucket, node)
	} else {
		// todo: ping existing nodes and replace if unresponsive
	}
}

type NodeDistance struct {
	Node     *NetworkNode
	Distance *big.Int
}

func (rt *RoutingTable) FindClosest(target NodeId, count int) []*NetworkNode {
	rt.mutex.RLock()
	defer rt.mutex.RUnlock()
	var allNodes []NodeDistance
	for _, bucket := range rt.Buckets {
		for _, n := range bucket {
			dist := getKeyDistance(DHTKey(n.Id), DHTKey(target))
			allNodes = append(allNodes, NodeDistance{Node: n, Distance: dist})
		}
	}
	sort.Slice(allNodes, func(i, j int) bool {
		return allNodes[i].Distance.Cmp(allNodes[j].Distance) < 0
	})
	var result []*NetworkNode
	for i := 0; i < len(allNodes) && i < count; i++ {
		result = append(result, allNodes[i].Node)
	}
	return result
}
