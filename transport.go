package main

const (
	RPCPing      = "PING"
	RPCStore     = "STORE"
	RPCFindNode  = "FIND_NODE"
	RPCFindValue = "FIND_VALUE"
	RPCFetchBlob = "FETCH_BLOB"
)

type RPCHandler interface {
	OnPing(sender *NetworkNode) error
	OnStore(sender *NetworkNode, key DHTKey, data ValueMeta) error
	OnFindNode(sender *NetworkNode, targetID NodeId) ([]*NetworkNode, error)
	OnFindValue(sender *NetworkNode, key DHTKey) (*ValueMeta, []*NetworkNode, error)
	OnFetchBlob(sender *NetworkNode, ref string) ([]byte, error)
}

type Transport interface {
	Ping(target *NetworkNode) error
	Store(target *NetworkNode, key DHTKey, data ValueMeta) error
	FindNode(target *NetworkNode, targetId NodeId) ([]*NetworkNode, error)
	FindValue(target *NetworkNode, key DHTKey) (*ValueMeta, []*NetworkNode, error)
	FetchBlob(target *NetworkNode, ref string) ([]byte, error)
	Listen(handler RPCHandler) error
	Close() error
}
