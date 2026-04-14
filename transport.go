package main

const (
	RPCPing      = "PING"
	RPCStore     = "STORE"
	RPCFindNode  = "FIND_NODE"
	RPCFindValue = "FIND_VALUE"
)

type RPCHandler interface {
	OnPing(sender *NetworkNode) error
	OnStore(sender *NetworkNode, key DHTKey, data []byte) error
	OnFindNode(sender *NetworkNode, targetID NodeId) ([]*NetworkNode, error)
	OnFindValue(sender *NetworkNode, key DHTKey) ([]byte, []*NetworkNode, error)
}

type Transport interface {
	Ping(target *NetworkNode) error
	Store(target *NetworkNode, key DHTKey, data []byte) error
	FindNode(target *NetworkNode, targetId NodeId) ([]*NetworkNode, error)
	FindValue(target *NetworkNode, key DHTKey) ([]byte, []*NetworkNode, error)
	Listen(handler RPCHandler) error
	Close() error
}
