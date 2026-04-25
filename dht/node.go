package dht

import (
	"errors"
	"fmt"
	"net"
)

type Node struct {
	self      *NetworkNode
	storage   Storage
	transport Transport
	rt        *RoutingTable
}

type NetworkNode struct {
	Id      NodeId            `json:"id"`
	Address net.IP            `json:"address"`
	Port    int               `json:"port"`
	Meta    map[string]string `json:"meta,omitempty"`
}

func NewNetworkNode(address string, port int) *NetworkNode {
	return &NetworkNode{
		Id:      GenerateKey(),
		Address: net.ParseIP(address),
		Port:    port,
	}
}

func NewNode(netNode *NetworkNode, transport Transport) *Node {
	return NewNodeWithStorage(netNode, transport, NewMemoryStorage())
}

func NewNodeWithStorage(netNode *NetworkNode, transport Transport, storage Storage) *Node {
	if storage == nil {
		storage = NewMemoryStorage()
	}
	n := &Node{
		self:      netNode,
		storage:   storage,
		transport: transport,
		rt:        NewRoutingTable(netNode.Id),
	}
	n.rt.SetPing(func(peer *NetworkNode) bool {
		return n.transport.Ping(peer) == nil
	})
	return n
}

func (node *Node) observePeer(peer *NetworkNode) {
	if peer != nil {
		node.rt.Add(peer)
	}
}

func (node *Node) OnPing(sender *NetworkNode) error {
	node.observePeer(sender)
	return nil
}

func (node *Node) OnStore(sender *NetworkNode, key DHTKey, data ValueMeta) error {
	node.observePeer(sender)
	node.storage.Put(key, data)
	return nil
}

func (node *Node) OnFindNode(sender *NetworkNode, targetID NodeId) ([]*NetworkNode, error) {
	node.observePeer(sender)
	return node.rt.FindClosest(targetID, K), nil
}

func (node *Node) OnFindValue(sender *NetworkNode, key DHTKey) (*ValueMeta, []*NetworkNode, error) {
	node.observePeer(sender)
	if value := node.storage.Get(key); value != nil {
		return value, nil, nil
	}
	nodes := node.rt.FindClosest(key, K)
	if len(nodes) == 0 {
		return nil, nil, ErrNotFound
	}
	return nil, nodes, nil
}

func (node *Node) OnFetchBlob(_ *NetworkNode, ref string) ([]byte, error) {
	bs, ok := node.storage.(BlobStore)
	if !ok {
		return nil, errors.New("node does not support blob storage")
	}
	return bs.GetBlob(ref)
}

func (node *Node) Start() error {
	if node.transport == nil {
		return errors.New("transport is not configured")
	}
	return node.transport.Listen(node)
}

func (node *Node) Close() error {
	if node.transport == nil {
		return nil
	}
	return node.transport.Close()
}

func (node *Node) blobStore() (BlobStore, bool) {
	bs, ok := node.storage.(BlobStore)
	return bs, ok
}

func (node *Node) nodeAddr() string {
	return fmt.Sprintf("%s:%d", node.self.Address, node.self.Port)
}

func (node *Node) DHTNode() *Node {
	return node
}
