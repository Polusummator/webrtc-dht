package webrtc

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/Polusummator/webrtc-dht/dht"
	"github.com/gorilla/websocket"
)

type SignalPayload struct {
	CallerID dht.NodeId `json:"caller_id"`
	SDP      string     `json:"sdp"`
}

type Signaler interface {
	SendOffer(ctx context.Context, target *dht.NetworkNode, payload SignalPayload) (answerSDP string, err error)
	ListenOffers(local *dht.NetworkNode, handler func(SignalPayload) (answerSDP string, err error)) error
	Close() error
}

type CentralSignaler struct {
	serverURL string

	mu      sync.Mutex
	conn    *websocket.Conn
	pending map[string]chan string
	cancel  context.CancelFunc
}

func NewCentralSignaler(serverURL string) *CentralSignaler {
	return &CentralSignaler{
		serverURL: serverURL,
		pending:   make(map[string]chan string),
	}
}

type wsSignalMsg struct {
	Type     string `json:"type"`
	ID       string `json:"id,omitempty"`
	CallerID string `json:"caller_id,omitempty"`
	CalleeID string `json:"callee_id,omitempty"`
	SDP      string `json:"sdp,omitempty"`
}

func (s *CentralSignaler) ListenOffers(local *dht.NetworkNode, handler func(SignalPayload) (string, error)) error {
	conn, _, err := websocket.DefaultDialer.Dial(s.serverURL, nil)
	if err != nil {
		return fmt.Errorf("CentralSignaler: dial %s: %w", s.serverURL, err)
	}

	reg := wsSignalMsg{Type: "register", ID: fmt.Sprintf("%x", local.Id)}
	if err := conn.WriteJSON(reg); err != nil {
		_ = conn.Close()
		return fmt.Errorf("CentralSignaler: register: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	s.mu.Lock()
	s.conn = conn
	s.cancel = cancel
	s.mu.Unlock()

	go s.readLoop(ctx, conn, local, handler)
	return nil
}

func (s *CentralSignaler) readLoop(ctx context.Context, conn *websocket.Conn, local *dht.NetworkNode, handler func(SignalPayload) (string, error)) {
	defer conn.Close()
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		var msg wsSignalMsg
		if err := conn.ReadJSON(&msg); err != nil {
			return
		}

		switch msg.Type {
		case "offer":
			go func(m wsSignalMsg) {
				answer, err := handler(SignalPayload{
					CallerID: nodeIDFromHex(m.CallerID),
					SDP:      m.SDP,
				})
				if err != nil {
					return
				}
				s.mu.Lock()
				c := s.conn
				s.mu.Unlock()
				if c == nil {
					return
				}
				_ = c.WriteJSON(wsSignalMsg{
					Type:     "answer",
					CallerID: m.CallerID,
					CalleeID: fmt.Sprintf("%x", local.Id),
					SDP:      answer,
				})
			}(msg)

		case "answer":
			key := msg.CallerID + "/" + msg.CalleeID
			s.mu.Lock()
			ch, ok := s.pending[key]
			s.mu.Unlock()
			if ok {
				select {
				case ch <- msg.SDP:
				default:
				}
			}
		}
	}
}

func (s *CentralSignaler) SendOffer(ctx context.Context, target *dht.NetworkNode, payload SignalPayload) (string, error) {
	callerHex := fmt.Sprintf("%x", payload.CallerID)
	calleeHex := fmt.Sprintf("%x", target.Id)
	key := callerHex + "/" + calleeHex

	ch := make(chan string, 1)
	s.mu.Lock()
	s.pending[key] = ch
	conn := s.conn
	s.mu.Unlock()

	defer func() {
		s.mu.Lock()
		delete(s.pending, key)
		s.mu.Unlock()
	}()

	if conn == nil {
		return "", fmt.Errorf("CentralSignaler: not connected")
	}
	if err := conn.WriteJSON(wsSignalMsg{
		Type:     "offer",
		CallerID: callerHex,
		CalleeID: calleeHex,
		SDP:      payload.SDP,
	}); err != nil {
		return "", fmt.Errorf("CentralSignaler: send offer: %w", err)
	}

	select {
	case answer := <-ch:
		return answer, nil
	case <-ctx.Done():
		return "", ctx.Err()
	case <-time.After(15 * time.Second):
		return "", fmt.Errorf("CentralSignaler: timeout waiting for answer from %x", target.Id)
	}
}

func (s *CentralSignaler) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cancel != nil {
		s.cancel()
	}
	if s.conn != nil {
		err := s.conn.Close()
		s.conn = nil
		return err
	}
	return nil
}

func nodeIDFromHex(h string) dht.NodeId {
	var id dht.NodeId
	for i := 0; i < len(id) && 2*i+1 < len(h); i++ {
		var b byte
		fmt.Sscanf(h[2*i:2*i+2], "%02x", &b)
		id[i] = b
	}
	return id
}

type DirectSignaler struct {
	port   int
	server *http.Server
	mu     sync.Mutex
	cancel context.CancelFunc
}

func NewDirectSignaler(port int) *DirectSignaler {
	return &DirectSignaler{port: port}
}

func (s *DirectSignaler) ListenOffers(local *dht.NetworkNode, handler func(SignalPayload) (string, error)) error {
	if local.Meta == nil {
		local.Meta = make(map[string]string)
	}
	local.Meta["sig_port"] = fmt.Sprintf("%d", s.port)

	mux := http.NewServeMux()
	mux.HandleFunc("/offer", func(w http.ResponseWriter, r *http.Request) {
		var p SignalPayload
		if err := json.NewDecoder(r.Body).Decode(&p); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		answer, err := handler(p)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Write([]byte(answer))
	})

	s.mu.Lock()
	s.server = &http.Server{Addr: fmt.Sprintf(":%d", s.port), Handler: mux}
	s.mu.Unlock()

	go func() { _ = s.server.ListenAndServe() }()
	return nil
}

func (s *DirectSignaler) SendOffer(ctx context.Context, target *dht.NetworkNode, payload SignalPayload) (string, error) {
	sigPort := ""
	if target.Meta != nil {
		sigPort = target.Meta["sig_port"]
	}
	if sigPort == "" {
		return "", fmt.Errorf("DirectSignaler: target %x has no sig_port in Meta", target.Id)
	}

	b, _ := json.Marshal(payload)
	url := fmt.Sprintf("http://%s:%s/offer", target.Address.String(), sigPort)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(b))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 20 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("DirectSignaler: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("DirectSignaler: status %d: %s", resp.StatusCode, body)
	}
	answer, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	return string(answer), nil
}

func (s *DirectSignaler) Close() error {
	s.mu.Lock()
	srv := s.server
	s.mu.Unlock()
	if srv != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		return srv.Shutdown(ctx)
	}
	return nil
}

type dhtOfferEntry struct {
	Payload   SignalPayload `json:"payload"`
	Timestamp int64         `json:"ts"`
}

type DHTSignaler struct {
	mu     sync.RWMutex
	node   *dht.Node
	cancel context.CancelFunc
}

func NewDHTSignaler() *DHTSignaler {
	return &DHTSignaler{}
}

func (s *DHTSignaler) Attach(node *dht.Node) {
	s.mu.Lock()
	s.node = node
	s.mu.Unlock()
}

func dhtOfferKey(callerID, calleeID dht.NodeId) dht.DHTKey {
	raw := append([]byte("rtcof:"), callerID[:]...)
	return dht.KeyFromBytes(append(raw, calleeID[:]...))
}

func dhtAnswerKey(callerID, calleeID dht.NodeId) dht.DHTKey {
	raw := append([]byte("rtcan:"), callerID[:]...)
	return dht.KeyFromBytes(append(raw, calleeID[:]...))
}

func dhtDoorbellKey(calleeID dht.NodeId) dht.DHTKey {
	return dht.KeyFromBytes(append([]byte("rtcbell:"), calleeID[:]...))
}

func (s *DHTSignaler) ListenOffers(local *dht.NetworkNode, handler func(SignalPayload) (string, error)) error {
	s.mu.Lock()
	ctx, cancel := context.WithCancel(context.Background())
	s.cancel = cancel
	s.mu.Unlock()

	go s.pollOffers(ctx, local, handler)
	return nil
}

func (s *DHTSignaler) getNode() *dht.Node {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.node
}

func (s *DHTSignaler) pollOffers(ctx context.Context, local *dht.NetworkNode, handler func(SignalPayload) (string, error)) {
	ticker := time.NewTicker(300 * time.Millisecond)
	defer ticker.Stop()
	bellKey := dhtDoorbellKey(local.Id)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			node := s.getNode()
			if node == nil {
				continue
			}
			meta, err := node.FindValue(ctx, bellKey)
			if err != nil || meta == nil || !meta.Inline || len(meta.Data) != 20 {
				continue
			}
			var callerID dht.NodeId
			copy(callerID[:], meta.Data)

			offerMeta, err := node.FindValue(ctx, dhtOfferKey(callerID, local.Id))
			if err != nil || offerMeta == nil || !offerMeta.Inline {
				continue
			}
			var entry dhtOfferEntry
			if err := json.Unmarshal(offerMeta.Data, &entry); err != nil {
				continue
			}
			if time.Since(time.Unix(entry.Timestamp, 0)) > 30*time.Second {
				continue
			}

			answer, err := handler(entry.Payload)
			if err != nil {
				continue
			}
			_ = node.StoreValue(ctx, dhtAnswerKey(callerID, local.Id), dht.ValueMeta{
				Inline: true,
				Data:   []byte(answer),
			})
			_ = node.StoreValue(ctx, bellKey, dht.ValueMeta{Inline: true, Data: nil})
		}
	}
}

func (s *DHTSignaler) SendOffer(ctx context.Context, target *dht.NetworkNode, payload SignalPayload) (string, error) {
	node := s.getNode()
	if node == nil {
		return "", fmt.Errorf("DHTSignaler: node not attached")
	}

	entry := dhtOfferEntry{Payload: payload, Timestamp: time.Now().Unix()}
	data, err := json.Marshal(entry)
	if err != nil {
		return "", err
	}

	offerKey := dhtOfferKey(payload.CallerID, target.Id)
	if err := node.StoreValue(ctx, offerKey, dht.ValueMeta{Inline: true, Data: data}); err != nil {
		return "", fmt.Errorf("store offer: %w", err)
	}
	if err := node.StoreValue(ctx, dhtDoorbellKey(target.Id), dht.ValueMeta{
		Inline: true, Data: payload.CallerID[:],
	}); err != nil {
		return "", fmt.Errorf("doorbell: %w", err)
	}

	answerKey := dhtAnswerKey(payload.CallerID, target.Id)
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(300 * time.Millisecond):
		}
		meta, err := node.FindValue(ctx, answerKey)
		if err == nil && meta != nil && meta.Inline && len(meta.Data) > 0 {
			return string(meta.Data), nil
		}
	}
	return "", fmt.Errorf("signaling timeout: no answer from %x", target.Id)
}

func (s *DHTSignaler) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cancel != nil {
		s.cancel()
	}
	return nil
}

type HybridSignaler struct {
	direct *DirectSignaler
	dhtSig *DHTSignaler
}

func NewHybridSignaler(directPort int) *HybridSignaler {
	return &HybridSignaler{
		direct: NewDirectSignaler(directPort),
		dhtSig: NewDHTSignaler(),
	}
}

func (h *HybridSignaler) DHTSig() *DHTSignaler { return h.dhtSig }

func (h *HybridSignaler) ListenOffers(local *dht.NetworkNode, handler func(SignalPayload) (string, error)) error {
	if err := h.direct.ListenOffers(local, handler); err != nil {
		return err
	}
	return h.dhtSig.ListenOffers(local, handler)
}

func (h *HybridSignaler) SendOffer(ctx context.Context, target *dht.NetworkNode, payload SignalPayload) (string, error) {
	if target.Meta != nil && target.Meta["sig_port"] != "" {
		return h.direct.SendOffer(ctx, target, payload)
	}
	return h.dhtSig.SendOffer(ctx, target, payload)
}

func (h *HybridSignaler) Close() error {
	err1 := h.direct.Close()
	err2 := h.dhtSig.Close()
	if err1 != nil {
		return err1
	}
	return err2
}
