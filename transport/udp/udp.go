package udp

import (
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/Polusummator/webrtc-dht/dht"
	"github.com/pion/stun/v3"
)

const (
	defaultRequestTimeout = 2 * time.Second
	blobTCPTimeout        = 30 * time.Second
)

type message struct {
	Type     string             `json:"type"`
	IsReq    bool               `json:"is_req"`
	ReqID    string             `json:"req_id"`
	Sender   *dht.NetworkNode   `json:"sender,omitempty"`
	TargetID dht.NodeId         `json:"target_id,omitempty"`
	Key      dht.DHTKey         `json:"key,omitempty"`
	Data     *dht.ValueMeta     `json:"data,omitempty"`
	Value    *dht.ValueMeta     `json:"value,omitempty"`
	Nodes    []*dht.NetworkNode `json:"nodes,omitempty"`
	Error    string             `json:"error,omitempty"`
}

type pendingReq struct {
	ch chan *message
}

type Transport struct {
	localNode *dht.NetworkNode
	conn      *net.UDPConn
	tcpLn     net.Listener
	handler   dht.RPCHandler

	requestTimeout  time.Duration
	pendingRequests sync.Map // map[string]*pendingReq
	stunServer      string
}

func NewTransport(local *dht.NetworkNode) *Transport {
	return &Transport{
		localNode:      local,
		requestTimeout: defaultRequestTimeout,
		stunServer:     "stun.l.google.com:19302",
	}
}

func (t *Transport) SetStunServer(server string) {
	t.stunServer = server
}

func (t *Transport) Listen(handler dht.RPCHandler) error {
	if handler == nil {
		return errors.New("rpc handler is nil")
	}
	if t.localNode == nil {
		return errors.New("local node is nil")
	}
	t.handler = handler

	udpAddr := &net.UDPAddr{IP: t.localNode.Address, Port: t.localNode.Port}
	conn, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		return err
	}
	t.conn = conn

	tcpAddr := &net.TCPAddr{IP: t.localNode.Address, Port: t.localNode.Port}
	tcpLn, err := net.ListenTCP("tcp", tcpAddr)
	if err != nil {
		_ = conn.Close()
		return fmt.Errorf("blob tcp listener: %w", err)
	}
	t.tcpLn = tcpLn

	if t.stunServer != "" {
		if err := t.resolvePublicAddress(); err != nil {
			fmt.Printf("STUN warning: %v\n", err)
		}
	}

	go t.listenUDP()
	go t.listenBlobTCP()
	return nil
}

func (t *Transport) listenUDP() {
	buf := make([]byte, 65535)
	for {
		n, remoteAddr, err := t.conn.ReadFromUDP(buf)
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return
			}
			continue
		}
		if stun.IsMessage(buf[:n]) {
			continue
		}
		var msg message
		if err := json.Unmarshal(buf[:n], &msg); err == nil {
			go t.handleMessage(&msg, remoteAddr)
		}
	}
}

func (t *Transport) listenBlobTCP() {
	for {
		conn, err := t.tcpLn.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return
			}
			continue
		}
		go t.handleBlobConn(conn)
	}
}

func (t *Transport) handleBlobConn(conn net.Conn) {
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(blobTCPTimeout))

	refBuf := make([]byte, 64)
	if _, err := io.ReadFull(conn, refBuf); err != nil {
		return
	}
	ref := string(refBuf)

	var data []byte
	if t.handler != nil {
		var err error
		data, err = t.handler.OnFetchBlob(nil, ref)
		if err != nil {
			data = nil
		}
	}

	sizeBuf := make([]byte, 8)
	binary.BigEndian.PutUint64(sizeBuf, uint64(len(data)))
	_, _ = conn.Write(sizeBuf)
	if len(data) > 0 {
		_, _ = conn.Write(data)
	}
}

func (t *Transport) resolvePublicAddress() error {
	serverAddr, err := net.ResolveUDPAddr("udp", t.stunServer)
	if err != nil {
		return err
	}

	_ = t.conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	defer func() { _ = t.conn.SetReadDeadline(time.Time{}) }()

	b, err := stun.Build(stun.TransactionID, stun.BindingRequest)
	if err != nil {
		return err
	}

	if _, err := t.conn.WriteToUDP(b.Raw, serverAddr); err != nil {
		return err
	}

	buf := make([]byte, 1024)
	n, _, err := t.conn.ReadFromUDP(buf)
	if err != nil {
		return err
	}

	msg := new(stun.Message)
	msg.Raw = buf[:n]
	if err := msg.Decode(); err != nil {
		return err
	}

	var xorAddr stun.XORMappedAddress
	if err := xorAddr.GetFrom(msg); err != nil {
		return err
	}

	t.localNode.Address = xorAddr.IP
	t.localNode.Port = xorAddr.Port
	return nil
}

func (t *Transport) handleMessage(msg *message, remoteAddr *net.UDPAddr) {
	if !msg.IsReq {
		if reqItf, ok := t.pendingRequests.Load(msg.ReqID); ok {
			req := reqItf.(*pendingReq)
			select {
			case req.ch <- msg:
			default:
			}
		}
		return
	}

	resp := message{
		Type:   msg.Type,
		IsReq:  false,
		ReqID:  msg.ReqID,
		Sender: t.localNode,
	}

	if t.handler == nil {
		resp.Error = "rpc handler is not configured"
		b, _ := json.Marshal(resp)
		_, _ = t.conn.WriteToUDP(b, remoteAddr)
		return
	}

	switch msg.Type {
	case dht.RPCPing:
		if err := t.handler.OnPing(msg.Sender); err != nil {
			resp.Error = err.Error()
		}
	case dht.RPCStore:
		if msg.Data == nil {
			resp.Error = "data is missing"
			break
		}
		if err := t.handler.OnStore(msg.Sender, msg.Key, *msg.Data); err != nil {
			resp.Error = err.Error()
		}
	case dht.RPCFindNode:
		nodes, err := t.handler.OnFindNode(msg.Sender, msg.TargetID)
		if err != nil {
			resp.Error = err.Error()
			break
		}
		resp.Nodes = nodes
	case dht.RPCFindValue:
		value, nodes, err := t.handler.OnFindValue(msg.Sender, msg.Key)
		if err != nil && !errors.Is(err, dht.ErrNotFound) {
			resp.Error = err.Error()
			break
		}
		resp.Value = value
		resp.Nodes = nodes
	default:
		resp.Error = fmt.Sprintf("unsupported rpc: %s", msg.Type)
	}

	b, _ := json.Marshal(resp)
	_, _ = t.conn.WriteToUDP(b, remoteAddr)
}

func generateReqID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return fmt.Sprintf("%x", b)
}

func (t *Transport) sendReq(target *dht.NetworkNode, msg *message) (*message, error) {
	if t.conn == nil {
		return nil, errors.New("transport is not listening")
	}
	if target == nil {
		return nil, errors.New("target node is nil")
	}

	msg.Sender = t.localNode
	msg.IsReq = true
	msg.ReqID = generateReqID()

	b, err := json.Marshal(msg)
	if err != nil {
		return nil, err
	}

	addr := &net.UDPAddr{IP: target.Address, Port: target.Port}
	req := &pendingReq{ch: make(chan *message, 1)}
	t.pendingRequests.Store(msg.ReqID, req)
	defer t.pendingRequests.Delete(msg.ReqID)

	if _, err = t.conn.WriteToUDP(b, addr); err != nil {
		return nil, err
	}

	select {
	case resp := <-req.ch:
		if resp.Error != "" {
			if resp.Error == dht.ErrNotFound.Error() {
				return nil, dht.ErrNotFound
			}
			return nil, errors.New(resp.Error)
		}
		return resp, nil
	case <-time.After(t.requestTimeout):
		return nil, fmt.Errorf("request to %s timed out", addr)
	}
}

func (t *Transport) Ping(target *dht.NetworkNode) error {
	_, err := t.sendReq(target, &message{Type: dht.RPCPing})
	return err
}

func (t *Transport) Store(target *dht.NetworkNode, key dht.DHTKey, data dht.ValueMeta) error {
	_, err := t.sendReq(target, &message{Type: dht.RPCStore, Key: key, Data: &data})
	return err
}

func (t *Transport) FindNode(target *dht.NetworkNode, targetId dht.NodeId) ([]*dht.NetworkNode, error) {
	resp, err := t.sendReq(target, &message{Type: dht.RPCFindNode, TargetID: targetId})
	if err != nil {
		return nil, err
	}
	return resp.Nodes, nil
}

func (t *Transport) FindValue(target *dht.NetworkNode, key dht.DHTKey) (*dht.ValueMeta, []*dht.NetworkNode, error) {
	resp, err := t.sendReq(target, &message{Type: dht.RPCFindValue, Key: key})
	if err != nil {
		if errors.Is(err, dht.ErrNotFound) {
			return nil, nil, nil
		}
		return nil, nil, err
	}
	return resp.Value, resp.Nodes, nil
}

func (t *Transport) FetchBlob(target *dht.NetworkNode, ref string) ([]byte, error) {
	if len(ref) != 64 {
		return nil, fmt.Errorf("invalid blob ref length: %d (want 64)", len(ref))
	}

	addr := fmt.Sprintf("%s:%d", target.Address, target.Port)
	conn, err := net.DialTimeout("tcp", addr, blobTCPTimeout)
	if err != nil {
		return nil, fmt.Errorf("blob tcp dial %s: %w", addr, err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(blobTCPTimeout))

	if _, err := io.WriteString(conn, ref); err != nil {
		return nil, err
	}

	sizeBuf := make([]byte, 8)
	if _, err := io.ReadFull(conn, sizeBuf); err != nil {
		return nil, err
	}
	size := binary.BigEndian.Uint64(sizeBuf)
	if size == 0 {
		return nil, dht.ErrNotFound
	}

	data := make([]byte, size)
	if _, err := io.ReadFull(conn, data); err != nil {
		return nil, fmt.Errorf("blob read: %w", err)
	}
	return data, nil
}

func (t *Transport) Close() error {
	if t.tcpLn != nil {
		_ = t.tcpLn.Close()
	}
	if t.conn != nil {
		return t.conn.Close()
	}
	return nil
}
