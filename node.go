package main

import (
	"errors"
	"net"
	"strconv"
)

type NodeId DHTKey

type Node struct {
	node      *NetworkNode
	storage   Storage
	transport Transport
	rt        *RoutingTable
}

type NetworkNode struct {
	Id      NodeId
	Address net.IP
	Port    int
}

func NewNetworkNode(Address string, Port string) *NetworkNode {
	p, _ := strconv.Atoi(Port)
	return &NetworkNode{
		Id:      NodeId(generateKey()),
		Address: net.ParseIP(Address),
		Port:    p,
	}
}

func NewNode(netNode *NetworkNode, transport Transport) *Node {
	return NewNodeWithStorage(netNode, transport, NewMemoryStorage())
}

func NewNodeWithStorage(netNode *NetworkNode, transport Transport, storage Storage) *Node {
	if storage == nil {
		storage = NewMemoryStorage()
	}

	return &Node{
		node:      netNode,
		storage:   storage,
		transport: transport,
		rt:        NewRoutingTable(netNode.Id),
	}
}

func (node *Node) observePeer(peer *NetworkNode) {
	if peer == nil {
		return
	}
	node.rt.Add(peer)
}

func (node *Node) OnPing(sender *NetworkNode) error {
	node.observePeer(sender)
	return nil
}

func (node *Node) OnStore(sender *NetworkNode, key DHTKey, data []byte) error {
	node.observePeer(sender)
	node.storage.Put(key, data)
	return nil
}

func (node *Node) OnFindNode(sender *NetworkNode, targetID NodeId) ([]*NetworkNode, error) {
	node.observePeer(sender)
	return node.rt.FindClosest(targetID, K), nil
}

func (node *Node) OnFindValue(sender *NetworkNode, key DHTKey) ([]byte, []*NetworkNode, error) {
	node.observePeer(sender)
	if value := node.storage.Get(key); value != nil {
		return value, nil, nil
	}

	nodes := node.rt.FindClosest(NodeId(key), K)
	if len(nodes) == 0 {
		return nil, nil, ErrNotFound
	}

	return nil, nodes, nil
}

func (node *Node) Start() error {
	if node.transport == nil {
		return errors.New("transport is not configured")
	}
	return node.transport.Listen(node)
}
