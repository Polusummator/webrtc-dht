package main

import (
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
	return &Node{
		node:      netNode,
		storage:   NewMemoryStorage(),
		transport: transport,
		rt:        NewRoutingTable(netNode.Id),
	}
}
