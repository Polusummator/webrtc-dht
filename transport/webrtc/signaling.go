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

func signalPort(nodePort int) int { return nodePort + 10000 }

type HTTPSignaler struct {
	mu      sync.RWMutex
	handler func(SignalPayload) (string, error)
	srv     *http.Server
	client  *http.Client
}

func NewHTTPSignaler() *HTTPSignaler {
	return &HTTPSignaler{client: &http.Client{Timeout: 30 * time.Second}}
}

func (s *HTTPSignaler) ListenOffers(local *dht.NetworkNode, handler func(SignalPayload) (string, error)) error {
	s.mu.Lock()
	s.handler = handler
	s.mu.Unlock()

	mux := http.NewServeMux()
	mux.HandleFunc("/signal", func(w http.ResponseWriter, r *http.Request) {
		var p SignalPayload
		if err := json.NewDecoder(r.Body).Decode(&p); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		s.mu.RLock()
		h := s.handler
		s.mu.RUnlock()
		if h == nil {
			http.Error(w, "no handler", http.StatusServiceUnavailable)
			return
		}
		answer, err := h(p)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "text/plain")
		_, _ = io.WriteString(w, answer)
	})

	addr := fmt.Sprintf("%s:%d", local.Address, signalPort(local.Port))
	s.srv = &http.Server{Addr: addr, Handler: mux}
	go func() { _ = s.srv.ListenAndServe() }()
	return nil
}

func (s *HTTPSignaler) SendOffer(ctx context.Context, target *dht.NetworkNode, payload SignalPayload) (string, error) {
	b, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	url := fmt.Sprintf("http://%s:%d/signal", target.Address, signalPort(target.Port))
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(b))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := s.client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("signal server: %s", string(body))
	}
	return string(body), nil
}

func (s *HTTPSignaler) Close() error {
	if s.srv != nil {
		return s.srv.Close()
	}
	return nil
}

type dhtOfferEntry struct {
	Payload   SignalPayload `json:"payload"`
	Timestamp int64         `json:"ts"`
}

type DHTSignaler struct {
	node   *dht.Node
	mu     sync.RWMutex
	cancel context.CancelFunc
}

func NewDHTSignaler(node *dht.Node) *DHTSignaler {
	return &DHTSignaler{node: node}
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

func (s *DHTSignaler) pollOffers(ctx context.Context, local *dht.NetworkNode, handler func(SignalPayload) (string, error)) {
	ticker := time.NewTicker(300 * time.Millisecond)
	defer ticker.Stop()
	bellKey := dhtDoorbellKey(local.Id)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			meta, err := s.node.FindValue(ctx, bellKey)
			if err != nil || meta == nil || !meta.Inline || len(meta.Data) != 20 {
				continue
			}
			var callerID dht.NodeId
			copy(callerID[:], meta.Data)

			offerMeta, err := s.node.FindValue(ctx, dhtOfferKey(callerID, local.Id))
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
			_ = s.node.StoreValue(ctx, dhtAnswerKey(callerID, local.Id), dht.ValueMeta{
				Inline: true,
				Data:   []byte(answer),
			})
			_ = s.node.StoreValue(ctx, bellKey, dht.ValueMeta{Inline: true, Data: nil})
		}
	}
}

func (s *DHTSignaler) SendOffer(ctx context.Context, target *dht.NetworkNode, payload SignalPayload) (string, error) {
	entry := dhtOfferEntry{Payload: payload, Timestamp: time.Now().Unix()}
	data, err := json.Marshal(entry)
	if err != nil {
		return "", err
	}

	offerKey := dhtOfferKey(payload.CallerID, target.Id)
	if err := s.node.StoreValue(ctx, offerKey, dht.ValueMeta{Inline: true, Data: data}); err != nil {
		return "", fmt.Errorf("store offer: %w", err)
	}
	if err := s.node.StoreValue(ctx, dhtDoorbellKey(target.Id), dht.ValueMeta{
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
		meta, err := s.node.FindValue(ctx, answerKey)
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
