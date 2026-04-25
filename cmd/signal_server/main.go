package main

import (
	"encoding/json"
	"flag"
	"log"
	"net/http"
	"sync"

	"github.com/gorilla/websocket"
)

type signalMsg struct {
	Type     string `json:"type"`
	ID       string `json:"id,omitempty"`
	CallerID string `json:"caller_id,omitempty"`
	CalleeID string `json:"callee_id,omitempty"`
	SDP      string `json:"sdp,omitempty"`
}

type server struct {
	upgrader websocket.Upgrader
	mu       sync.RWMutex
	peers    map[string]*websocket.Conn
}

func newServer() *server {
	return &server{
		upgrader: websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }},
		peers:    make(map[string]*websocket.Conn),
	}
}

func (s *server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	conn, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println("upgrade:", err)
		return
	}

	var nodeID string
	defer func() {
		conn.Close()
		if nodeID != "" {
			s.mu.Lock()
			delete(s.peers, nodeID)
			s.mu.Unlock()
			log.Printf("disconnected: %s", nodeID)
		}
	}()

	for {
		var msg signalMsg
		if err := conn.ReadJSON(&msg); err != nil {
			return
		}

		switch msg.Type {
		case "register":
			nodeID = msg.ID
			s.mu.Lock()
			s.peers[nodeID] = conn
			s.mu.Unlock()
			log.Printf("registered: %s", nodeID)

		case "offer":
			s.forward(msg.CalleeID, msg)

		case "answer":
			s.forward(msg.CallerID, msg)

		default:
			log.Printf("unknown message type: %s", msg.Type)
		}
	}
}

func (s *server) forward(targetID string, msg signalMsg) {
	s.mu.RLock()
	conn, ok := s.peers[targetID]
	s.mu.RUnlock()
	if !ok {
		log.Printf("forward: peer %s not connected", targetID)
		return
	}
	b, _ := json.Marshal(msg)
	if err := conn.WriteMessage(websocket.TextMessage, b); err != nil {
		log.Printf("forward to %s: %v", targetID, err)
	}
}

func main() {
	addr := flag.String("addr", ":9000", "listen address")
	flag.Parse()

	s := newServer()
	http.Handle("/signal", s)

	log.Printf("signaling server listening on %s", *addr)
	log.Fatal(http.ListenAndServe(*addr, nil))
}
