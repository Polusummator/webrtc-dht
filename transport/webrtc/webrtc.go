package webrtc

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	pion "github.com/pion/webrtc/v4"

	"github.com/Polusummator/webrtc-dht/dht"
)

const (
	labelControl        = "dht-control"
	labelBlob           = "dht-blob"
	rpcTimeout          = 5 * time.Second
	blobTimeout         = 120 * time.Second
	blobChunkSize       = 256 * 1024
	blobBufferThreshold = uint64(8 * 1024 * 1024)
	sctpReceiveBufSize  = 8 * 1024 * 1024
)

type rpcMessage struct {
	Type     string             `json:"type"`
	IsReq    bool               `json:"is_req"`
	ReqID    string             `json:"req_id"`
	Sender   *dht.NetworkNode   `json:"sender,omitempty"`
	TargetID dht.NodeId         `json:"target_id,omitempty"`
	Key      dht.DHTKey         `json:"key,omitempty"`
	Ref      string             `json:"ref,omitempty"`
	Data     *dht.ValueMeta     `json:"data,omitempty"`
	Value    *dht.ValueMeta     `json:"value,omitempty"`
	Nodes    []*dht.NetworkNode `json:"nodes,omitempty"`
	Error    string             `json:"error,omitempty"`
}

type pendingRPC struct {
	ch chan *rpcMessage
}

type pendingBlob struct {
	mu  sync.Mutex
	buf []byte
	ch  chan []byte
}

type peerConn struct {
	pc        *pion.PeerConnection
	controlDC *pion.DataChannel
	blobDC    *pion.DataChannel

	controlReady  chan struct{}
	blobReady     chan struct{}
	blobLowSignal chan struct{}

	mu          sync.Mutex
	rpcPending  map[string]*pendingRPC
	blobPending map[string]*pendingBlob
}

func newPeerConn(pc *pion.PeerConnection) *peerConn {
	return &peerConn{
		pc:            pc,
		controlReady:  make(chan struct{}),
		blobReady:     make(chan struct{}),
		blobLowSignal: make(chan struct{}, 1),
		rpcPending:    make(map[string]*pendingRPC),
		blobPending:   make(map[string]*pendingBlob),
	}
}

func sendBlobChunked(dc *pion.DataChannel, lowSignal <-chan struct{}, reqID string, data []byte) error {
	for offset := 0; offset < len(data); {
		for dc.BufferedAmount() > blobBufferThreshold {
			<-lowSignal
		}
		end := offset + blobChunkSize
		isLast := byte(0)
		if end >= len(data) {
			end = len(data)
			isLast = 1
		}
		frame := make([]byte, 33+end-offset)
		copy(frame[:32], reqID)
		frame[32] = isLast
		copy(frame[33:], data[offset:end])
		if err := dc.Send(frame); err != nil {
			return err
		}
		offset = end
	}
	return nil
}

type Transport struct {
	local    *dht.NetworkNode
	signaler Signaler
	handler  dht.RPCHandler

	stunURLs []string

	peersMu sync.RWMutex
	peers   map[dht.NodeId]*peerConn
}

func NewTransport(local *dht.NetworkNode, signaler Signaler, stunURLs ...string) *Transport {
	return &Transport{
		local:    local,
		signaler: signaler,
		stunURLs: stunURLs,
		peers:    make(map[dht.NodeId]*peerConn),
	}
}

func (t *Transport) newPC() (*pion.PeerConnection, error) {
	se := pion.SettingEngine{}
	se.SetICETimeouts(2*time.Second, 2*time.Second, 500*time.Millisecond)
	se.SetSCTPMaxReceiveBufferSize(sctpReceiveBufSize)

	var servers []pion.ICEServer
	for _, u := range t.stunURLs {
		servers = append(servers, pion.ICEServer{URLs: []string{u}})
	}
	api := pion.NewAPI(pion.WithSettingEngine(se))
	return api.NewPeerConnection(pion.Configuration{ICEServers: servers})
}

func (t *Transport) Listen(handler dht.RPCHandler) error {
	t.handler = handler
	dhtNode, hasDHTNode := handler.(interface{ DHTNode() *dht.Node })
	switch sig := t.signaler.(type) {
	case *DHTSignaler:
		if hasDHTNode && sig.getNode() == nil {
			sig.Attach(dhtNode.DHTNode())
		}
	case *HybridSignaler:
		if hasDHTNode && sig.DHTSig().getNode() == nil {
			sig.DHTSig().Attach(dhtNode.DHTNode())
		}
	}
	return t.signaler.ListenOffers(t.local, t.handleIncomingOffer)
}

func (t *Transport) handleIncomingOffer(payload SignalPayload) (string, error) {
	pc, err := t.newPC()
	if err != nil {
		return "", err
	}
	peer := newPeerConn(pc)

	pc.OnDataChannel(func(dc *pion.DataChannel) {
		switch dc.Label() {
		case labelControl:
			peer.controlDC = dc
			dc.OnOpen(func() { close(peer.controlReady) })
			dc.OnMessage(func(m pion.DataChannelMessage) {
				t.onControlMsg(peer, m.Data, payload.CallerID)
			})
		case labelBlob:
			peer.blobDC = dc
			dc.OnOpen(func() {
				dc.SetBufferedAmountLowThreshold(blobBufferThreshold)
				dc.OnBufferedAmountLow(func() {
					select {
					case peer.blobLowSignal <- struct{}{}:
					default:
					}
				})
				close(peer.blobReady)
			})
			dc.OnMessage(func(m pion.DataChannelMessage) {
				onBlobMsg(peer, m.Data)
			})
		}
	})

	if err := pc.SetRemoteDescription(pion.SessionDescription{
		Type: pion.SDPTypeOffer,
		SDP:  payload.SDP,
	}); err != nil {
		_ = pc.Close()
		return "", err
	}
	answer, err := pc.CreateAnswer(nil)
	if err != nil {
		_ = pc.Close()
		return "", err
	}
	gathered := pion.GatheringCompletePromise(pc)
	if err := pc.SetLocalDescription(answer); err != nil {
		_ = pc.Close()
		return "", err
	}
	<-gathered

	t.peersMu.Lock()
	t.peers[payload.CallerID] = peer
	t.peersMu.Unlock()

	return pc.LocalDescription().SDP, nil
}

func (t *Transport) connect(target *dht.NetworkNode) (*peerConn, error) {
	pc, err := t.newPC()
	if err != nil {
		return nil, err
	}
	peer := newPeerConn(pc)

	controlDC, err := pc.CreateDataChannel(labelControl, nil)
	if err != nil {
		_ = pc.Close()
		return nil, err
	}
	blobDC, err := pc.CreateDataChannel(labelBlob, nil)
	if err != nil {
		_ = pc.Close()
		return nil, err
	}

	peer.controlDC = controlDC
	peer.blobDC = blobDC

	controlDC.OnOpen(func() { close(peer.controlReady) })
	controlDC.OnMessage(func(m pion.DataChannelMessage) {
		t.onControlMsg(peer, m.Data, target.Id)
	})
	blobDC.OnOpen(func() {
		blobDC.SetBufferedAmountLowThreshold(blobBufferThreshold)
		blobDC.OnBufferedAmountLow(func() {
			select {
			case peer.blobLowSignal <- struct{}{}:
			default:
			}
		})
		close(peer.blobReady)
	})
	blobDC.OnMessage(func(m pion.DataChannelMessage) {
		onBlobMsg(peer, m.Data)
	})

	offer, err := pc.CreateOffer(nil)
	if err != nil {
		_ = pc.Close()
		return nil, err
	}
	gathered := pion.GatheringCompletePromise(pc)
	if err := pc.SetLocalDescription(offer); err != nil {
		_ = pc.Close()
		return nil, err
	}
	<-gathered

	t.peersMu.Lock()
	t.peers[target.Id] = peer
	t.peersMu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	answerSDP, err := t.signaler.SendOffer(ctx, target, SignalPayload{
		CallerID: t.local.Id,
		SDP:      pc.LocalDescription().SDP,
	})
	if err != nil {
		t.peersMu.Lock()
		delete(t.peers, target.Id)
		t.peersMu.Unlock()
		_ = pc.Close()
		return nil, fmt.Errorf("signaling: %w", err)
	}

	if err := pc.SetRemoteDescription(pion.SessionDescription{
		Type: pion.SDPTypeAnswer,
		SDP:  answerSDP,
	}); err != nil {
		t.peersMu.Lock()
		delete(t.peers, target.Id)
		t.peersMu.Unlock()
		_ = pc.Close()
		return nil, err
	}

	select {
	case <-peer.controlReady:
		return peer, nil
	case <-time.After(15 * time.Second):
		return nil, fmt.Errorf("timeout waiting for data channel to open")
	}
}

func (t *Transport) getPeer(target *dht.NetworkNode) (*peerConn, error) {
	t.peersMu.RLock()
	p, ok := t.peers[target.Id]
	t.peersMu.RUnlock()
	if ok {
		select {
		case <-p.controlReady:
			return p, nil
		case <-time.After(15 * time.Second):
			return nil, fmt.Errorf("peer %x: control channel not ready", target.Id)
		}
	}
	return t.connect(target)
}

func (t *Transport) onControlMsg(peer *peerConn, raw []byte, senderID dht.NodeId) {
	var msg rpcMessage
	if err := json.Unmarshal(raw, &msg); err != nil {
		return
	}

	if !msg.IsReq {
		peer.mu.Lock()
		req, ok := peer.rpcPending[msg.ReqID]
		peer.mu.Unlock()
		if ok {
			select {
			case req.ch <- &msg:
			default:
			}
		}
		return
	}

	resp := rpcMessage{Type: msg.Type, IsReq: false, ReqID: msg.ReqID, Sender: t.local}

	if t.handler == nil {
		resp.Error = "handler not set"
		t.sendControl(peer, resp)
		return
	}

	switch msg.Type {
	case dht.RPCPing:
		if err := t.handler.OnPing(msg.Sender); err != nil {
			resp.Error = err.Error()
		}
		t.sendControl(peer, resp)

	case dht.RPCStore:
		if msg.Data == nil {
			resp.Error = "missing data"
		} else if err := t.handler.OnStore(msg.Sender, msg.Key, *msg.Data); err != nil {
			resp.Error = err.Error()
		}
		t.sendControl(peer, resp)

	case dht.RPCFindNode:
		nodes, err := t.handler.OnFindNode(msg.Sender, msg.TargetID)
		if err != nil {
			resp.Error = err.Error()
		} else {
			resp.Nodes = nodes
		}
		t.sendControl(peer, resp)

	case dht.RPCFindValue:
		value, nodes, err := t.handler.OnFindValue(msg.Sender, msg.Key)
		if err != nil && !errors.Is(err, dht.ErrNotFound) {
			resp.Error = err.Error()
		} else {
			resp.Value = value
			resp.Nodes = nodes
		}
		t.sendControl(peer, resp)

	case dht.RPCFetchBlob:
		go func() {
			data, err := t.handler.OnFetchBlob(msg.Sender, msg.Ref)
			if err != nil {
				resp.Error = err.Error()
				t.sendControl(peer, resp)
				return
			}
			select {
			case <-peer.blobReady:
			case <-time.After(15 * time.Second):
				resp.Error = "blob channel not ready"
				t.sendControl(peer, resp)
				return
			}
			if err := sendBlobChunked(peer.blobDC, peer.blobLowSignal, msg.ReqID, data); err != nil {
				resp.Error = err.Error()
				t.sendControl(peer, resp)
			}
		}()

	default:
		resp.Error = fmt.Sprintf("unknown rpc: %s", msg.Type)
		t.sendControl(peer, resp)
	}
}

func (t *Transport) sendControl(peer *peerConn, msg rpcMessage) {
	b, _ := json.Marshal(msg)
	_ = peer.controlDC.Send(b)
}

func onBlobMsg(peer *peerConn, data []byte) {
	if len(data) < 33 {
		return
	}
	reqID := string(data[:32])
	isLast := data[32]
	chunk := data[33:]

	peer.mu.Lock()
	bp, ok := peer.blobPending[reqID]
	peer.mu.Unlock()
	if !ok {
		return
	}

	bp.mu.Lock()
	bp.buf = append(bp.buf, chunk...)
	var full []byte
	if isLast == 1 {
		full = bp.buf
		bp.buf = nil
	}
	bp.mu.Unlock()

	if full != nil {
		select {
		case bp.ch <- full:
		default:
		}
	}
}

func (t *Transport) sendRPC(target *dht.NetworkNode, msg rpcMessage) (*rpcMessage, error) {
	msg.Sender = t.local
	msg.IsReq = true
	msg.ReqID = genReqID()

	peer, err := t.getPeer(target)
	if err != nil {
		return nil, err
	}

	req := &pendingRPC{ch: make(chan *rpcMessage, 1)}
	peer.mu.Lock()
	peer.rpcPending[msg.ReqID] = req
	peer.mu.Unlock()
	defer func() {
		peer.mu.Lock()
		delete(peer.rpcPending, msg.ReqID)
		peer.mu.Unlock()
	}()

	b, _ := json.Marshal(msg)
	if err := peer.controlDC.Send(b); err != nil {
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
	case <-time.After(rpcTimeout):
		return nil, fmt.Errorf("rpc timeout to %x", target.Id)
	}
}

func (t *Transport) Ping(target *dht.NetworkNode) error {
	_, err := t.sendRPC(target, rpcMessage{Type: dht.RPCPing})
	return err
}

func (t *Transport) Store(target *dht.NetworkNode, key dht.DHTKey, data dht.ValueMeta) error {
	_, err := t.sendRPC(target, rpcMessage{Type: dht.RPCStore, Key: key, Data: &data})
	return err
}

func (t *Transport) FindNode(target *dht.NetworkNode, targetId dht.NodeId) ([]*dht.NetworkNode, error) {
	resp, err := t.sendRPC(target, rpcMessage{Type: dht.RPCFindNode, TargetID: targetId})
	if err != nil {
		return nil, err
	}
	return resp.Nodes, nil
}

func (t *Transport) FindValue(target *dht.NetworkNode, key dht.DHTKey) (*dht.ValueMeta, []*dht.NetworkNode, error) {
	resp, err := t.sendRPC(target, rpcMessage{Type: dht.RPCFindValue, Key: key})
	if err != nil {
		if errors.Is(err, dht.ErrNotFound) {
			return nil, nil, nil
		}
		return nil, nil, err
	}
	return resp.Value, resp.Nodes, nil
}

func (t *Transport) FetchBlob(target *dht.NetworkNode, ref string) ([]byte, error) {
	peer, err := t.getPeer(target)
	if err != nil {
		return nil, err
	}

	select {
	case <-peer.blobReady:
	case <-time.After(15 * time.Second):
		return nil, fmt.Errorf("blob channel not ready")
	}

	reqID := genReqID()
	msg := rpcMessage{
		Type:   dht.RPCFetchBlob,
		ReqID:  reqID,
		Sender: t.local,
		IsReq:  true,
		Ref:    ref,
	}

	bp := &pendingBlob{ch: make(chan []byte, 1)}

	errReq := &pendingRPC{ch: make(chan *rpcMessage, 1)}
	peer.mu.Lock()
	peer.blobPending[reqID] = bp
	peer.rpcPending[reqID] = errReq
	peer.mu.Unlock()
	defer func() {
		peer.mu.Lock()
		delete(peer.blobPending, reqID)
		delete(peer.rpcPending, reqID)
		peer.mu.Unlock()
	}()

	b, _ := json.Marshal(msg)
	if err := peer.controlDC.Send(b); err != nil {
		return nil, err
	}

	select {
	case data := <-bp.ch:
		return data, nil
	case errMsg := <-errReq.ch:
		return nil, errors.New(errMsg.Error)
	case <-time.After(blobTimeout):
		return nil, fmt.Errorf("blob fetch timeout from %x", target.Id)
	}
}

func (t *Transport) Close() error {
	_ = t.signaler.Close()
	t.peersMu.Lock()
	defer t.peersMu.Unlock()
	var errs []error
	for _, p := range t.peers {
		if err := p.pc.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	t.peers = make(map[dht.NodeId]*peerConn)
	return errors.Join(errs...)
}

func genReqID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return fmt.Sprintf("%032x", b)
}
