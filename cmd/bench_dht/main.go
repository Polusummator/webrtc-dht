package main

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/Polusummator/webrtc-dht/dht"
	udptransport "github.com/Polusummator/webrtc-dht/transport/udp"
	webrtctransport "github.com/Polusummator/webrtc-dht/transport/webrtc"
)

type NodeInfo struct {
	Addr      string `json:"addr"`
	DHTPort   int    `json:"dht_port"`
	SigPort   int    `json:"sig_port,omitempty"`
	NodeIDHex string `json:"node_id_hex"`
}

type Result struct {
	Transport    string `json:"transport"`
	NodeCount    int    `json:"node_count"`
	Mode         string `json:"mode"`
	PayloadBytes int    `json:"payload_bytes"`
	Iteration    int    `json:"iteration"`
	DurationNs   int64  `json:"duration_ns"`
	Transferred  int64  `json:"transferred_bytes,omitempty"`
}

func main() {
	role := flag.String("role", "coordinator", "coordinator|bootstrap|node")
	transport := flag.String("transport", "udp", "udp|webrtc")
	localAddr := flag.String("addr", "0.0.0.0", "bind address")
	dhtPort := flag.Int("dht-port", 7200, "DHT port")
	sigPort := flag.Int("sig-port", 8200, "WebRTC signaling HTTP port")
	bootstrapAddr := flag.String("bootstrap-addr", "", "bootstrap host:port")
	sigServer := flag.String("sig-server", "http://127.0.0.1:9000", "central signal server URL")
	coordAddr := flag.String("coord-addr", "127.0.0.1", "coordinator host")
	coordPort := flag.Int("coord-port", 9200, "coordinator HTTP port")
	nodeCount := flag.Int("node-count", 5, "expected number of nodes (coordinator only)")
	iters := flag.Int("iters", 20, "iterations per operation")
	payloadSize := flag.Int("payload", 256, "value payload size in bytes")
	timeout := flag.Duration("timeout", 120*time.Second, "coordinator wait timeout")
	out := flag.String("out", "dht_results.json", "output JSON file (coordinator only)")
	flag.Parse()

	switch *role {
	case "bootstrap":
		runBootstrap(*localAddr, *dhtPort)
	case "node":
		runNode(*transport, *localAddr, *dhtPort, *sigPort, *bootstrapAddr, *sigServer, *coordAddr, *coordPort)
	case "coordinator":
		runCoordinator(*transport, *localAddr, *dhtPort, *sigPort, *bootstrapAddr, *sigServer,
			*coordAddr, *coordPort, *nodeCount, *iters, *payloadSize, *timeout, *out)
	default:
		log.Fatalf("unknown role: %s", *role)
	}
}

func outboundIP(localAddr string) string {
	if localAddr != "0.0.0.0" && localAddr != "" {
		return localAddr
	}
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		return "127.0.0.1"
	}
	defer conn.Close()
	return conn.LocalAddr().(*net.UDPAddr).IP.String()
}

func postJSON(url string, v interface{}) {
	b, _ := json.Marshal(v)
	resp, err := http.Post(url, "application/json", bytes.NewReader(b))
	if err != nil {
		log.Printf("POST %s: %v", url, err)
		return
	}
	resp.Body.Close()
}

func nodeIDFromHex(h string) dht.NodeId {
	b, _ := hex.DecodeString(h)
	var id dht.NodeId
	copy(id[:], b)
	return id
}

func toNetworkNode(tr string, info NodeInfo) *dht.NetworkNode {
	nn := &dht.NetworkNode{
		Id:      nodeIDFromHex(info.NodeIDHex),
		Address: net.ParseIP(info.Addr),
		Port:    info.DHTPort,
	}
	if tr == "webrtc" {
		nn.Meta = map[string]string{
			"sig_port": fmt.Sprintf("%d", info.SigPort),
		}
	}
	return nn
}

func bootstrapNode(n *dht.Node, bootstrapAddr string) {
	if bootstrapAddr == "" {
		return
	}
	host, portStr, err := net.SplitHostPort(bootstrapAddr)
	if err != nil {
		log.Fatalf("parse bootstrap-addr %q: %v", bootstrapAddr, err)
	}
	var port int
	fmt.Sscan(portStr, &port)
	bn := dht.NewNetworkNode(host, port)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := n.Bootstrap(ctx, []*dht.NetworkNode{bn}); err != nil {
		log.Printf("bootstrap warning: %v", err)
	}
}

func runBootstrap(localAddr string, dhtPort int) {
	nn := dht.NewNetworkNode(localAddr, dhtPort)
	tr := udptransport.NewTransport(nn)
	node := dht.NewNode(nn, tr)
	if err := node.Start(); err != nil {
		log.Fatalf("bootstrap start: %v", err)
	}
	fmt.Printf("bootstrap DHT running on %s:%d\n", localAddr, dhtPort)
	select {}
}

func runNode(transport, localAddr string, dhtPort, sigPort int, bootstrapAddr, sigServer, coordAddr string, coordPort int) {
	actualAddr := outboundIP(localAddr)

	nn := dht.NewNetworkNode(actualAddr, dhtPort)
	var tr dht.Transport
	switch transport {
	case "udp":
		tr = udptransport.NewTransport(nn)
	case "webrtc":
		sig := webrtctransport.NewCentralSignaler(sigServer)
		tr = webrtctransport.NewTransport(nn, sig)
	default:
		log.Fatalf("unknown transport: %s", transport)
	}

	node := dht.NewNode(nn, tr)
	if err := node.Start(); err != nil {
		log.Fatalf("node start: %v", err)
	}
	defer node.Close()

	if transport == "udp" {
		bootstrapNode(node, bootstrapAddr)
	} else {
		var bootPeer NodeInfo
		for {
			resp, err := http.Get(fmt.Sprintf("http://%s:%d/bootstrap-peer", coordAddr, coordPort))
			if err == nil && resp.StatusCode == http.StatusOK {
				_ = json.NewDecoder(resp.Body).Decode(&bootPeer)
				resp.Body.Close()
				if bootPeer.NodeIDHex != "" {
					break
				}
			} else if resp != nil {
				resp.Body.Close()
			}
			time.Sleep(500 * time.Millisecond)
		}
		coordNN := toNetworkNode(transport, bootPeer)
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := node.Bootstrap(ctx, []*dht.NetworkNode{coordNN}); err != nil {
			log.Printf("webrtc bootstrap warning: %v", err)
		}
	}

	info := NodeInfo{
		Addr:      actualAddr,
		DHTPort:   dhtPort,
		SigPort:   sigPort,
		NodeIDHex: fmt.Sprintf("%x", nn.Id),
	}
	postJSON(fmt.Sprintf("http://%s:%d/register-node", coordAddr, coordPort), info)
	fmt.Printf("node registered addr=%s dht-port=%d transport=%s\n", actualAddr, dhtPort, transport)

	for {
		resp, err := http.Get(fmt.Sprintf("http://%s:%d/done", coordAddr, coordPort))
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				break
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	fmt.Println("node: coordinator done, exiting")
}

func runCoordinator(
	transport, localAddr string, dhtPort, sigPort int,
	bootstrapAddr, sigServer, coordAddr string, coordPort,
	nodeCount, iters, payloadSize int,
	timeout time.Duration, out string,
) {
	var mu sync.Mutex
	var nodes []NodeInfo
	allRegistered := make(chan struct{}, 1)
	done := make(chan struct{})

	mux := http.NewServeMux()

	mux.HandleFunc("/register-node", func(w http.ResponseWriter, r *http.Request) {
		var info NodeInfo
		if err := json.NewDecoder(r.Body).Decode(&info); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		mu.Lock()
		nodes = append(nodes, info)
		n := len(nodes)
		mu.Unlock()
		fmt.Printf("node registered %s:%d (%d/%d)\n", info.Addr, info.DHTPort, n, nodeCount)
		if n >= nodeCount {
			select {
			case allRegistered <- struct{}{}:
			default:
			}
		}
		w.WriteHeader(http.StatusOK)
	})

	mux.HandleFunc("/done", func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-done:
			w.WriteHeader(http.StatusOK)
		default:
			http.Error(w, "not done yet", http.StatusNotFound)
		}
	})

	actualAddr := outboundIP(localAddr)
	nn := dht.NewNetworkNode(actualAddr, dhtPort)
	var coordTr dht.Transport
	switch transport {
	case "udp":
		coordTr = udptransport.NewTransport(nn)
	case "webrtc":
		sig := webrtctransport.NewCentralSignaler(sigServer)
		coordTr = webrtctransport.NewTransport(nn, sig)
	default:
		log.Fatalf("unknown transport: %s", transport)
	}
	coordNode := dht.NewNode(nn, coordTr)
	if err := coordNode.Start(); err != nil {
		log.Fatalf("coordinator dht start: %v", err)
	}
	defer coordNode.Close()

	if transport == "udp" {
		bootstrapNode(coordNode, bootstrapAddr)
	}

	coordInfo := NodeInfo{
		Addr:      actualAddr,
		DHTPort:   dhtPort,
		SigPort:   sigPort,
		NodeIDHex: fmt.Sprintf("%x", nn.Id),
	}
	mux.HandleFunc("/bootstrap-peer", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(coordInfo)
	})

	srv := &http.Server{Addr: fmt.Sprintf(":%d", coordPort), Handler: mux}
	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("coordinator http: %v", err)
		}
	}()
	fmt.Printf("coordinator waiting for %d nodes on :%d (transport=%s)...\n", nodeCount, coordPort, transport)

	select {
	case <-allRegistered:
		fmt.Println("all nodes registered")
	case <-time.After(timeout):
		fmt.Fprintln(os.Stderr, "WARNING: timeout waiting for nodes, proceeding with what we have")
	}

	mu.Lock()
	nodesCopy := make([]NodeInfo, len(nodes))
	copy(nodesCopy, nodes)
	mu.Unlock()

	if len(nodesCopy) == 0 {
		log.Fatal("no nodes registered, aborting")
	}

	time.Sleep(3 * time.Second)

	val := make([]byte, payloadSize)
	rand.Read(val)
	vm := dht.ValueMeta{Inline: true, Data: val}

	var results []Result

	fmt.Printf("running %d store iterations (payload=%d bytes)...\n", iters, payloadSize)
	for i := 0; i < iters; i++ {
		key := dht.GenerateKey()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)

		start := time.Now()
		err := coordNode.StoreValue(ctx, key, vm)
		dur := time.Since(start)
		cancel()

		if err != nil {
			log.Printf("store iter %d: %v", i, err)
		}
		results = append(results, Result{
			Transport:    transport,
			NodeCount:    len(nodesCopy),
			Mode:         "store",
			PayloadBytes: payloadSize,
			Iteration:    i,
			DurationNs:   dur.Nanoseconds(),
			Transferred:  int64(payloadSize),
		})
	}

	close(done)

	f, err := os.Create(out)
	if err != nil {
		log.Fatalf("create output: %v", err)
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	if err := enc.Encode(results); err != nil {
		log.Fatalf("encode results: %v", err)
	}
	fmt.Printf("wrote %d results to %s\n", len(results), out)
}
