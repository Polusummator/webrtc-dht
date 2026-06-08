package main

import (
	"encoding/json"
	"flag"
	"log"
	"net/http"
	"sync"

	"github.com/gorilla/websocket"
)

const forwardWorkers = 20

var forwardSem chan struct{}

type signalMsg struct {
	Type     string `json:"type"`
	ID       string `json:"id,omitempty"`
	CallerID string `json:"caller_id,omitempty"`
	CalleeID string `json:"callee_id,omitempty"`
	SDP      string `json:"sdp,omitempty"`
}

type connEntry struct {
	conn    *websocket.Conn
	writeMu sync.Mutex
}

type server struct {
	upgrader websocket.Upgrader
	mu       sync.RWMutex
	peers    map[string]*connEntry
}

func newServer() *server {
	return &server{
		upgrader: websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }},
		peers:    make(map[string]*connEntry),
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
			s.peers[nodeID] = &connEntry{conn: conn}
			s.mu.Unlock()
			log.Printf("registered: %s", nodeID)

		case "offer":
			go s.doForward(msg.CalleeID, msg)

		case "answer":
			go s.doForward(msg.CallerID, msg)

		default:
			log.Printf("unknown message type: %s", msg.Type)
		}
	}
}

func (s *server) doForward(targetID string, msg signalMsg) {
	forwardSem <- struct{}{}
	<-forwardSem
	s.mu.RLock()
	entry, ok := s.peers[targetID]
	s.mu.RUnlock()
	if !ok {
		log.Printf("forward: peer %s not connected", targetID)
		return
	}
	b, _ := json.Marshal(msg)
	entry.writeMu.Lock()
	err := entry.conn.WriteMessage(websocket.TextMessage, b)
	entry.writeMu.Unlock()
	if err != nil {
		log.Printf("forward to %s: %v", targetID, err)
	}
}

func main() {
	addr := flag.String("addr", ":9000", "listen address")
	flag.Parse()

	forwardSem = make(chan struct{}, forwardWorkers)
	s := newServer()
	http.Handle("/signal", s)

	log.Printf("signaling server listening on %s", *addr)
	log.Fatal(http.ListenAndServe(*addr, nil))
}
