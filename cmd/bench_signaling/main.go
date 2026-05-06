package main

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/Polusummator/webrtc-dht/dht"
	udptransport "github.com/Polusummator/webrtc-dht/transport/udp"
	webrtctransport "github.com/Polusummator/webrtc-dht/transport/webrtc"
)

type CalleeInfo struct {
	PairID    int    `json:"pair_id"`
	NodeIDHex string `json:"node_id_hex"`
	Addr      string `json:"addr"`
	Port      int    `json:"port"`
}

type PairResult struct {
	PairID    int    `json:"pair_id"`
	Signaling string `json:"signaling"`
	ConnectNs int64  `json:"connect_ns"`
	Error     string `json:"error,omitempty"`
}

type CoordOutput struct {
	Signaling string       `json:"signaling"`
	PairCount int          `json:"pair_count"`
	Results   []PairResult `json:"results"`
}

var (
	coordMu       sync.Mutex
	calleesByPair = map[int]CalleeInfo{}
	pairResults   []PairResult
	resultsReady  = make(chan struct{}, 1)
)

func main() {
	role := flag.String("role", "coordinator", "coordinator|bootstrap|callee|caller")
	signaling := flag.String("signaling", "central", "central|dht")
	pairID := flag.Int("pair-id", 0, "pair ID (callee/caller)")
	pairCount := flag.Int("pairs", 1, "total number of pairs (coordinator/callee/caller)")

	localAddr := flag.String("addr", "0.0.0.0", "bind address")
	dhtPort := flag.Int("dht-port", 7200, "local UDP DHT port base")
	bootstrapAddr := flag.String("bootstrap-addr", "", "UDP DHT bootstrap host:port")
	sigServer := flag.String("sig-server", "http://127.0.0.1:9000", "central signal server URL")
	coordAddr := flag.String("coord-addr", "127.0.0.1", "coordinator host")
	coordPort := flag.Int("coord-port", 9100, "coordinator port")
	out := flag.String("out", "signaling_results.json", "output file (coordinator)")

	flag.Parse()

	switch *role {
	case "bootstrap":
		runBootstrap(*localAddr, *dhtPort)
	case "coordinator":
		runCoordinator(*signaling, *pairCount, *coordPort, *out)
	case "callee":
		runCallee(*pairID, *localAddr, *dhtPort, *signaling, *sigServer, *bootstrapAddr, *coordAddr, *coordPort)
	case "caller":
		runCaller(*pairID, *localAddr, *dhtPort, *signaling, *sigServer, *bootstrapAddr, *coordAddr, *coordPort)
	default:
		log.Fatalf("unknown role: %s", *role)
	}
}

func runBootstrap(localAddr string, dhtPort int) {
	nn := dht.NewNetworkNode(localAddr, dhtPort)
	tr := udptransport.NewTransport(nn)
	node := dht.NewNode(nn, tr)
	if err := node.Start(); err != nil {
		log.Fatalf("bootstrap start: %v", err)
	}
	fmt.Printf("bootstrap UDP DHT running on %s:%d\n", localAddr, dhtPort)
	select {}
}

func makeUDPNode(localAddr string, port int, bootstrapAddr string) *dht.Node {
	nn := dht.NewNetworkNode(localAddr, port)
	tr := udptransport.NewTransport(nn)
	node := dht.NewNode(nn, tr)
	if err := node.Start(); err != nil {
		log.Fatalf("udp node start: %v", err)
	}
	if bootstrapAddr != "" {
		host, portStr, err := net.SplitHostPort(bootstrapAddr)
		if err != nil {
			log.Fatalf("parse bootstrap addr %q: %v", bootstrapAddr, err)
		}
		var bport int
		fmt.Sscan(portStr, &bport)
		bn := dht.NewNetworkNode(host, bport)
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		if err := node.Bootstrap(ctx, []*dht.NetworkNode{bn}); err != nil {
			log.Printf("udp bootstrap warning: %v", err)
		}
	}
	return node
}

func makeSignaler(signaling, sigServer string, udpNode *dht.Node) webrtctransport.Signaler {
	switch signaling {
	case "central":
		return webrtctransport.NewCentralSignaler(sigServer)
	case "dht":
		sig := webrtctransport.NewDHTSignaler()
		if udpNode != nil {
			sig.Attach(udpNode)
		}
		return sig
	default:
		log.Fatalf("unknown signaling: %s", signaling)
		return nil
	}
}

func webrtcPort(dhtPort int) int {
	return dhtPort + 3000
}

func runCallee(pairID int, localAddr string, dhtPort int, signaling, sigServer, bootstrapAddr, coordAddr string, coordPort int) {
	var udpNode *dht.Node
	if signaling == "dht" {
		udpNode = makeUDPNode(localAddr, dhtPort, bootstrapAddr)
	}

	webrtcNN := dht.NewNetworkNode(localAddr, webrtcPort(dhtPort))
	sig := makeSignaler(signaling, sigServer, udpNode)
	webrtcTr := webrtctransport.NewTransport(webrtcNN, sig)
	webrtcNode := dht.NewNode(webrtcNN, webrtcTr)
	if err := webrtcNode.Start(); err != nil {
		log.Fatalf("callee webrtc start: %v", err)
	}
	defer webrtcNode.Close()

	myIP := outboundIP(localAddr)
	info := CalleeInfo{
		PairID:    pairID,
		NodeIDHex: fmt.Sprintf("%x", webrtcNN.Id),
		Addr:      myIP,
		Port:      webrtcPort(dhtPort),
	}
	postJSON(fmt.Sprintf("http://%s:%d/register-callee", coordAddr, coordPort), info)
	fmt.Printf("callee pair=%d registered node=%s addr=%s\n", pairID, info.NodeIDHex, myIP)

	time.Sleep(3 * time.Minute)
}

func runCaller(pairID int, localAddr string, dhtPort int, signaling, sigServer, bootstrapAddr, coordAddr string, coordPort int) {
	var udpNode *dht.Node
	if signaling == "dht" {
		udpNode = makeUDPNode(localAddr, dhtPort, bootstrapAddr)
	}

	webrtcNN := dht.NewNetworkNode(localAddr, webrtcPort(dhtPort))
	sig := makeSignaler(signaling, sigServer, udpNode)
	webrtcTr := webrtctransport.NewTransport(webrtcNN, sig)
	webrtcNode := dht.NewNode(webrtcNN, webrtcTr)
	if err := webrtcNode.Start(); err != nil {
		log.Fatalf("caller webrtc start: %v", err)
	}
	defer webrtcNode.Close()

	calleeURL := fmt.Sprintf("http://%s:%d/callee/%d", coordAddr, coordPort, pairID)
	var calleeInfo CalleeInfo
	for i := 0; i < 60; i++ {
		resp, err := http.Get(calleeURL)
		if err == nil && resp.StatusCode == http.StatusOK {
			if err2 := json.NewDecoder(resp.Body).Decode(&calleeInfo); err2 == nil && calleeInfo.NodeIDHex != "" {
				resp.Body.Close()
				break
			}
			resp.Body.Close()
		} else if resp != nil {
			resp.Body.Close()
		}
		time.Sleep(time.Second)
	}
	if calleeInfo.NodeIDHex == "" {
		log.Fatalf("caller pair=%d: could not get callee info from coordinator", pairID)
	}

	target := &dht.NetworkNode{
		Id:      nodeIDFromHex(calleeInfo.NodeIDHex),
		Address: net.ParseIP(calleeInfo.Addr),
		Port:    calleeInfo.Port,
	}

	fmt.Printf("caller pair=%d connecting to %s (node=%s)...\n", pairID, calleeInfo.Addr, calleeInfo.NodeIDHex[:8])
	start := time.Now()
	err := webrtcTr.Ping(target)
	elapsed := time.Since(start)

	result := PairResult{
		PairID:    pairID,
		Signaling: signaling,
		ConnectNs: elapsed.Nanoseconds(),
	}
	if err != nil {
		result.Error = err.Error()
		log.Printf("caller pair=%d error: %v", pairID, err)
	} else {
		fmt.Printf("caller pair=%d connected in %v\n", pairID, elapsed)
	}
	postJSON(fmt.Sprintf("http://%s:%d/submit-result", coordAddr, coordPort), result)
}

func runCoordinator(signaling string, pairCount int, coordPort int, out string) {
	mux := http.NewServeMux()

	mux.HandleFunc("/register-callee", func(w http.ResponseWriter, r *http.Request) {
		var info CalleeInfo
		if err := json.NewDecoder(r.Body).Decode(&info); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		coordMu.Lock()
		calleesByPair[info.PairID] = info
		count := len(calleesByPair)
		coordMu.Unlock()
		fmt.Printf("callee registered pair=%d (%d/%d)\n", info.PairID, count, pairCount)
		w.WriteHeader(http.StatusOK)
	})

	mux.HandleFunc("/callee/", func(w http.ResponseWriter, r *http.Request) {
		var id int
		fmt.Sscanf(r.URL.Path[len("/callee/"):], "%d", &id)
		coordMu.Lock()
		info, ok := calleesByPair[id]
		coordMu.Unlock()
		if !ok {
			http.Error(w, "not registered yet", http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(info)
	})

	mux.HandleFunc("/submit-result", func(w http.ResponseWriter, r *http.Request) {
		var res PairResult
		if err := json.NewDecoder(r.Body).Decode(&res); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		coordMu.Lock()
		pairResults = append(pairResults, res)
		count := len(pairResults)
		coordMu.Unlock()
		fmt.Printf("result pair=%d connect=%v (%d/%d)\n", res.PairID, time.Duration(res.ConnectNs), count, pairCount)
		if count >= pairCount {
			select {
			case resultsReady <- struct{}{}:
			default:
			}
		}
		w.WriteHeader(http.StatusOK)
	})

	srv := &http.Server{Addr: fmt.Sprintf(":%d", coordPort), Handler: mux}
	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("coordinator http: %v", err)
		}
	}()
	fmt.Printf("coordinator waiting for %d pairs on :%d...\n", pairCount, coordPort)

	timer := time.NewTimer(5 * time.Minute)
	select {
	case <-resultsReady:
		fmt.Println("all results collected")
	case <-timer.C:
		fmt.Println("timeout: not all results collected")
	}

	coordMu.Lock()
	output := CoordOutput{
		Signaling: signaling,
		PairCount: pairCount,
		Results:   pairResults,
	}
	coordMu.Unlock()

	f, err := os.Create(out)
	if err != nil {
		log.Fatalf("create output: %v", err)
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	if err := enc.Encode(output); err != nil {
		log.Fatalf("encode output: %v", err)
	}
	fmt.Printf("wrote results to %s\n", out)
}

func nodeIDFromHex(h string) dht.NodeId {
	b, _ := hex.DecodeString(h)
	var id dht.NodeId
	copy(id[:], b)
	return id
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
