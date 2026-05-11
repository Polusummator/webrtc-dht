package dht

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
)

const Alpha = 3

func (node *Node) Bootstrap(ctx context.Context, bootstrapNodes []*NetworkNode) error {
	if len(bootstrapNodes) == 0 {
		return errors.New("no bootstrap nodes provided")
	}
	for _, n := range bootstrapNodes {
		node.rt.Add(n)
	}
	_, err := node.LookupNode(ctx, node.self.Id)
	return err
}

func (node *Node) LookupNode(ctx context.Context, target NodeId) ([]*NetworkNode, error) {
	nodes, _, err := node.iterativeSearch(ctx, target, false)
	return nodes, err
}

func (node *Node) StoreValue(ctx context.Context, key DHTKey, data ValueMeta) error {
	nodes, err := node.LookupNode(ctx, key)
	if err != nil && len(nodes) == 0 {
		return err
	}
	var wg sync.WaitGroup
	for _, n := range nodes {
		wg.Add(1)
		go func(target *NetworkNode) {
			defer wg.Done()
			_ = node.transport.Store(target, key, data)
		}(n)
	}
	wg.Wait()
	return nil
}

func (node *Node) StoreBlob(ctx context.Context, key DHTKey, data []byte, mimeType string) error {
	bs, ok := node.blobStore()
	if !ok {
		return errors.New("storage does not implement BlobStore; use DiskStorage")
	}

	ref, err := bs.PutBlob(data)
	if err != nil {
		return fmt.Errorf("store blob locally: %w", err)
	}

	meta := ValueMeta{
		Inline:   false,
		BlobRef:  ref,
		BlobNode: node.self.Id,
		Size:     int64(len(data)),
		MimeType: mimeType,
	}
	return node.StoreValue(ctx, key, meta)
}

func (node *Node) FindValue(ctx context.Context, key DHTKey) (*ValueMeta, error) {
	if val := node.storage.Get(key); val != nil {
		return val, nil
	}
	_, val, err := node.iterativeSearch(ctx, key, true)
	return val, err
}

func (node *Node) FetchBlob(ctx context.Context, meta *ValueMeta) ([]byte, error) {
	if meta == nil {
		return nil, errors.New("meta is nil")
	}
	if meta.Inline {
		return meta.Data, nil
	}
	if meta.BlobRef == "" {
		return nil, errors.New("blob ref is empty")
	}

	if bs, ok := node.blobStore(); ok {
		if data, err := bs.GetBlob(meta.BlobRef); err == nil {
			return data, nil
		}
	}

	peers := node.rt.FindClosest(meta.BlobNode, 1)
	if len(peers) == 0 || peers[0].Id != meta.BlobNode {
		return nil, fmt.Errorf("blob node %x not found in routing table", meta.BlobNode)
	}

	return node.transport.FetchBlob(peers[0], meta.BlobRef)
}

func (node *Node) iterativeSearch(
	ctx context.Context,
	target NodeId,
	isFindValue bool,
) ([]*NetworkNode, *ValueMeta, error) {

	closest := node.rt.FindClosest(target, K)
	if len(closest) == 0 {
		return nil, nil, errors.New("routing table is empty")
	}

	visited := make(map[NodeId]bool)

	for {
		if err := ctx.Err(); err != nil {
			return closest, nil, err
		}

		var toQuery []*NetworkNode
		for _, n := range closest {
			if !visited[n.Id] {
				toQuery = append(toQuery, n)
				if len(toQuery) == Alpha {
					break
				}
			}
		}
		if len(toQuery) == 0 {
			break
		}

		var (
			wg         sync.WaitGroup
			mu         sync.Mutex
			newNodes   []*NetworkNode
			foundValue *ValueMeta
		)

		for _, qNode := range toQuery {
			visited[qNode.Id] = true
			wg.Add(1)
			go func(n *NetworkNode) {
				defer wg.Done()

				var (
					val *ValueMeta
					res []*NetworkNode
					err error
				)
				if isFindValue {
					val, res, err = node.transport.FindValue(n, target)
				} else {
					res, err = node.transport.FindNode(n, target)
				}
				if err != nil {
					return
				}

				mu.Lock()
				defer mu.Unlock()
				if val != nil {
					foundValue = val
					return
				}
				for _, r := range res {
					node.rt.Add(r)
					if !visited[r.Id] {
						newNodes = append(newNodes, r)
					}
				}
			}(qNode)
		}
		wg.Wait()

		if foundValue != nil {
			return nil, foundValue, nil
		}

		closest = append(closest, newNodes...)
		sort.Slice(closest, func(i, j int) bool {
			return GetKeyDistance(closest[i].Id, target).
				Cmp(GetKeyDistance(closest[j].Id, target)) < 0
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
