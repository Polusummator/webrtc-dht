package main

import (
	"context"
	"errors"
	"sort"
	"sync"
)

const Alpha = 3

func (node *Node) Bootstrap(ctx context.Context, bootstrapNodes []*NetworkNode) error {
	for _, n := range bootstrapNodes {
		node.rt.Add(n)
	}
	if len(bootstrapNodes) == 0 {
		return errors.New("no bootstrap nodes")
	}
	_, err := node.LookupNode(ctx, node.node.Id)
	return err
}
func (node *Node) LookupNode(ctx context.Context, target NodeId) ([]*NetworkNode, error) {
	return node.iterativeLookup(ctx, target)
}
func (node *Node) StoreValue(ctx context.Context, key DHTKey, data ValueMeta) error {
	nodes, err := node.LookupNode(ctx, NodeId(key))
	if err != nil && len(nodes) == 0 {
		return err
	}
	for _, n := range nodes {
		_ = node.transport.Store(n, key, data)
	}
	return nil
}
func (node *Node) FindValue(ctx context.Context, key DHTKey) (*ValueMeta, error) {
	val := node.storage.Get(key)
	if val != nil {
		return val, nil
	}
	_, val, err := node.iterativeSearch(ctx, NodeId(key), true)
	return val, err
}
func (node *Node) iterativeLookup(ctx context.Context, target NodeId) ([]*NetworkNode, error) {
	nodes, _, err := node.iterativeSearch(ctx, target, false)
	return nodes, err
}
func (node *Node) iterativeSearch(ctx context.Context, target NodeId, isFindValue bool) ([]*NetworkNode, *ValueMeta, error) {
	closest := node.rt.FindClosest(target, K)
	if len(closest) == 0 {
		return nil, nil, errors.New("routing table empty")
	}
	visited := make(map[string]bool)
	for {
		var toQuery []*NetworkNode
		for _, n := range closest {
			if !visited[string(n.Id)] {
				toQuery = append(toQuery, n)
			}
		}
		if len(toQuery) == 0 {
			break
		}
		if len(toQuery) > Alpha {
			toQuery = toQuery[:Alpha]
		}
		var wg sync.WaitGroup
		var newNodes []*NetworkNode
		var foundValue *ValueMeta
		var qMu sync.Mutex
		for _, qNode := range toQuery {
			visited[string(qNode.Id)] = true
			wg.Add(1)
			go func(n *NetworkNode) {
				defer wg.Done()
				var val *ValueMeta
				var res []*NetworkNode
				var err error

				if isFindValue {
					val, res, err = node.transport.FindValue(n, DHTKey(target))
				} else {
					res, err = node.transport.FindNode(n, target)
				}

				if err == nil {
					qMu.Lock()
					defer qMu.Unlock()
					if val != nil {
						foundValue = val
					} else {
						newNodes = append(newNodes, res...)
					}
					for _, r := range res {
						node.rt.Add(r)
					}
				}
			}(qNode)
		}
		wg.Wait()
		if foundValue != nil {
			return nil, foundValue, nil
		}
		for _, n := range newNodes {
			if !visited[string(n.Id)] {
				closest = append(closest, n)
			}
		}
		sort.Slice(closest, func(i, j int) bool {
			d1 := getKeyDistance(DHTKey(closest[i].Id), DHTKey(target))
			d2 := getKeyDistance(DHTKey(closest[j].Id), DHTKey(target))
			return d1.Cmp(d2) < 0
		})
		if len(closest) > K {
			closest = closest[:K]
		}
	}

	if isFindValue {
		return closest, nil, ErrNotFound
	}
	return closest, nil, nil
}
