package main

import (
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/pion/stun/v3"
)

const defaultRequestTimeout = 2 * time.Second

type message struct {
	Type     string         `json:"type"`
	IsReq    bool           `json:"is_req"`
	ReqID    string         `json:"req_id"`
	Sender   *NetworkNode   `json:"sender,omitempty"`
	TargetID NodeId         `json:"target_id,omitempty"`
	Key      DHTKey         `json:"key,omitempty"`
	Data     *ValueMeta     `json:"data,omitempty"`
	Value    *ValueMeta     `json:"value,omitempty"`
	Nodes    []*NetworkNode `json:"nodes,omitempty"`
	Error    string         `json:"error,omitempty"`
}

type pendingReq struct {
	ch chan *message
}

type UDPTransport struct {
	localNode *NetworkNode
	conn      *net.UDPConn
	handler   RPCHandler

	requestTimeout  time.Duration
	pendingRequests sync.Map // map[string]*pendingReq
	stunServer      string
}

func NewUDPTransport(local *NetworkNode) *UDPTransport {
	return &UDPTransport{
		localNode:      local,
		requestTimeout: defaultRequestTimeout,
		stunServer:     "stun.l.google.com:19302", // todo
	}
}

func (t *UDPTransport) SetStunServer(server string) {
	t.stunServer = server
}

func (t *UDPTransport) Listen(handler RPCHandler) error {
	if handler == nil {
		return errors.New("rpc handler is nil")
	}
	if t.localNode == nil {
		return errors.New("local node is nil")
	}
	t.handler = handler

	addr := &net.UDPAddr{IP: t.localNode.Address, Port: t.localNode.Port}
	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		return err
	}
	t.conn = conn

	if t.stunServer != "" {
		if err := t.resolvePublicAddress(); err != nil {
			fmt.Printf("STUN warning: %v\n", err)
		}
	}

	go func() {
		buf := make([]byte, 65535)
		for {
			n, remoteAddr, err := t.conn.ReadFromUDP(buf)
			if err != nil {
				if errors.Is(err, net.ErrClosed) {
					return
				}
				continue
			}

			var msg message
			if stun.IsMessage(buf[:n]) {
				continue
			}

			if err := json.Unmarshal(buf[:n], &msg); err == nil {
				go t.handleMessage(&msg, remoteAddr)
			}
		}
	}()
	return nil
}

func (t *UDPTransport) resolvePublicAddress() error {
	serverAddr, err := net.ResolveUDPAddr("udp", t.stunServer)
	if err != nil {
		return err
	}

	_ = t.conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	defer t.conn.SetReadDeadline(time.Time{})

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

func (t *UDPTransport) handleMessage(msg *message, remoteAddr *net.UDPAddr) {
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
	case RPCPing:
		if err := t.handler.OnPing(msg.Sender); err != nil {
			resp.Error = err.Error()
		}
	case RPCStore:
		if msg.Data == nil {
			resp.Error = "data is missing"
			break
		}
		if err := t.handler.OnStore(msg.Sender, msg.Key, *msg.Data); err != nil {
			resp.Error = err.Error()
		}
	case RPCFindNode:
		nodes, err := t.handler.OnFindNode(msg.Sender, msg.TargetID)
		if err != nil {
			resp.Error = err.Error()
			break
		}
		resp.Nodes = nodes
	case RPCFindValue:
		value, nodes, err := t.handler.OnFindValue(msg.Sender, msg.Key)
		if err != nil && !errors.Is(err, ErrNotFound) {
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

func (t *UDPTransport) sendReq(target *NetworkNode, msg *message) (*message, error) {
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

	_, err = t.conn.WriteToUDP(b, addr)
	if err != nil {
		return nil, err
	}

	select {
	case resp := <-req.ch:
		if resp.Error != "" {
			if resp.Error == ErrNotFound.Error() {
				return nil, ErrNotFound
			}
			return nil, errors.New(resp.Error)
		}
		return resp, nil
	case <-time.After(t.requestTimeout):
		return nil, fmt.Errorf("timeout")
	}
}

func (t *UDPTransport) Ping(target *NetworkNode) error {
	msg := message{Type: RPCPing}
	_, err := t.sendReq(target, &msg)
	return err
}

func (t *UDPTransport) Store(target *NetworkNode, key DHTKey, data ValueMeta) error {
	msg := message{Type: RPCStore, Key: key, Data: &data}
	_, err := t.sendReq(target, &msg)
	return err
}

func (t *UDPTransport) FindNode(target *NetworkNode, targetId NodeId) ([]*NetworkNode, error) {
	msg := message{Type: RPCFindNode, TargetID: targetId}
	resp, err := t.sendReq(target, &msg)
	if err != nil {
		return nil, err
	}
	return resp.Nodes, nil
}

func (t *UDPTransport) FindValue(target *NetworkNode, key DHTKey) (*ValueMeta, []*NetworkNode, error) {
	msg := message{Type: RPCFindValue, Key: key}
	resp, err := t.sendReq(target, &msg)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return nil, nil, nil
		}
		return nil, nil, err
	}
	return resp.Value, resp.Nodes, nil
}

func (t *UDPTransport) Close() error {
	if t.conn != nil {
		return t.conn.Close()
	}
	return nil
}
