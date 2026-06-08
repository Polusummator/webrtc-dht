// cmd/bench_signaling2/main.go
package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	mrand "math/rand"
	"net"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/Polusummator/webrtc-dht/dht"
	udptransport "github.com/Polusummator/webrtc-dht/transport/udp"
	webrtctransport "github.com/Polusummator/webrtc-dht/transport/webrtc"
)

type RunPairRequest struct {
	PairID          int    `json:"pair_id"`
	Role            string `json:"role"`
	PairCalleeIDHex string `json:"pair_callee_id_hex"`
	PairCallerIDHex string `json:"pair_caller_id_hex"`
	CallbackURL     string `json:"callback_url,omitempty"`
	StartDelayMs    int    `json:"start_delay_ms,omitempty"`
}

type PairResult struct {
	PairID    int    `json:"pair_id"`
	Signaling string `json:"signaling"`
	LatencyNs int64  `json:"latency_ns"`
	Error     string `json:"error,omitempty"`
}

type AgentInfo struct {
	NodeIDHex string `json:"node_id_hex"`
}

type CoordOutput struct {
	Signaling string       `json:"signaling"`
	PairCount int          `json:"pair_count"`
	Results   []PairResult `json:"results"`
}

var (
	agentDHTNode *dht.Node
	agentNN      *dht.NetworkNode
	agentSig     string
	agentSigURL  string
)

func main() {
	role := flag.String("role", "coordinator", "coordinator|agent")
	signaling := flag.String("signaling", "central", "central|dht")
	pairs := flag.Int("pairs", 1, "concurrent pairs")

	localAddr := flag.String("addr", "0.0.0.0", "bind address")
	dhtPort := flag.Int("dht-port", 7200, "UDP DHT port")
	bootstrapAddr := flag.String("bootstrap-addr", "", "bootstrap host:port")
	sigServer := flag.String("sig-server", "http://127.0.0.1:9000", "central signal server URL")
	agentPort := flag.Int("agent-port", 8800, "agent HTTP port")
	agentsFlag := flag.String("agents", "", "comma-separated agent host:port (coordinator)")
	coordPort := flag.Int("coord-port", 9100, "coordinator result-collector port")
	outFile := flag.String("out", "signaling2_results.json", "output file")
	timeout := flag.Duration("timeout", 60*time.Second, "timeout per run")
	flag.Parse()

	switch *role {
	case "agent":
		runAgent(*localAddr, *dhtPort, *bootstrapAddr, *signaling, *sigServer, *agentPort)
	case "coordinator":
		runCoordinator(*signaling, *pairs, *agentsFlag, *coordPort, *outFile, *timeout)
	default:
		log.Fatalf("unknown role: %s", *role)
	}
}

func runAgent(localAddr string, dhtPort int, bootstrapAddr, signaling, sigServer string, agentPort int) {
	actualAddr := resolveIP(localAddr)
	agentSig = signaling
	agentSigURL = sigServer

	agentNN = dht.NewNetworkNode(actualAddr, dhtPort)
	agentDHTNode = dht.NewNode(agentNN, udptransport.NewTransport(agentNN))
	if err := agentDHTNode.Start(); err != nil {
		log.Fatalf("dht start: %v", err)
	}
	if bootstrapAddr != "" {
		host, portStr, _ := net.SplitHostPort(bootstrapAddr)
		var bport int
		fmt.Sscan(portStr, &bport)
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		bn := dht.NewNetworkNode(host, bport)
		if err := agentDHTNode.Bootstrap(ctx, []*dht.NetworkNode{bn}); err != nil {
			log.Printf("bootstrap warning: %v", err)
		}
	}
	fmt.Printf("agent ready: node=%x addr=%s dht=%d http=%d sig=%s\n",
		agentNN.Id, actualAddr, dhtPort, agentPort, signaling)

	mux := http.NewServeMux()
	mux.HandleFunc("/info", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(AgentInfo{NodeIDHex: fmt.Sprintf("%x", agentNN.Id)})
	})
	mux.HandleFunc("/run-pair", handleRunPair)
	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", agentPort), mux))
}

func handleRunPair(w http.ResponseWriter, r *http.Request) {
	var req RunPairRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusAccepted)
	go executePairRole(req)
}

func executePairRole(req RunPairRequest) {
	switch req.Role {
	case "callee":
		runCalleeSignaling(req)
	case "caller":
		latency, err := runCallerSignaling(req)
		res := PairResult{
			PairID:    req.PairID,
			Signaling: agentSig,
			LatencyNs: latency.Nanoseconds(),
		}
		if err != nil {
			res.Error = err.Error()
		}
		if req.CallbackURL != "" {
			b, _ := json.Marshal(res)
			resp, e := http.Post(req.CallbackURL, "application/json", bytes.NewReader(b))
			if e == nil {
				resp.Body.Close()
			}
		}
	}
}

func runCalleeSignaling(req RunPairRequest) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	pairCalleeID := nodeIDFromHex(req.PairCalleeIDHex)

	switch agentSig {
	case "central":
		sig := webrtctransport.NewCentralSignaler(agentSigURL)
		if err := sig.ListenOffers(&dht.NetworkNode{Id: pairCalleeID}, func(p webrtctransport.SignalPayload) (string, error) {
			return "answer-to-" + p.SDP, nil
		}); err != nil {
			log.Printf("callee pair=%d ListenOffers: %v", req.PairID, err)
			return
		}
		<-ctx.Done()
		sig.Close()

	case "dht":
		bellKey := dhtDoorbellKey(pairCalleeID)
		if req.StartDelayMs > 0 {
			select {
			case <-time.After(time.Duration(req.StartDelayMs) * time.Millisecond):
			case <-ctx.Done():
				return
			}
		}
		if jitter := time.Duration(mrand.Intn(50)) * time.Millisecond; jitter > 0 {
			select {
			case <-time.After(jitter):
			case <-ctx.Done():
				return
			}
		}
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				meta, err := agentDHTNode.FindValue(ctx, bellKey)
				if err != nil || meta == nil || !meta.Inline || len(meta.Data) < 20 {
					continue
				}
				var callerID dht.NodeId
				copy(callerID[:], meta.Data)
				offerKey := dhtOfferKey(callerID, pairCalleeID)
				offerMeta, err := agentDHTNode.FindValue(ctx, offerKey)
				if err != nil || offerMeta == nil || !offerMeta.Inline || len(offerMeta.Data) == 0 {
					continue
				}
				answerKey := dhtAnswerKey(callerID, pairCalleeID)
				_ = agentDHTNode.StoreValue(ctx, answerKey, dht.ValueMeta{
					Inline: true,
					Data:   []byte("answer-to-" + string(offerMeta.Data)),
				})
				_ = agentDHTNode.StoreValue(ctx, bellKey, dht.ValueMeta{Inline: true, Data: nil})
				return
			}
		}
	}
}

func runCallerSignaling(req RunPairRequest) (time.Duration, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	pairCalleeID := nodeIDFromHex(req.PairCalleeIDHex)
	pairCallerID := nodeIDFromHex(req.PairCallerIDHex)
	offerData := fmt.Sprintf("offer-%d", req.PairID)

	start := time.Now()

	switch agentSig {
	case "central":
		sig := webrtctransport.NewCentralSignaler(agentSigURL)
		if err := sig.ListenOffers(&dht.NetworkNode{Id: pairCallerID}, func(p webrtctransport.SignalPayload) (string, error) {
			return "", nil
		}); err != nil {
			return 0, err
		}
		defer sig.Close()
		_, err := sig.SendOffer(ctx, &dht.NetworkNode{Id: pairCalleeID}, webrtctransport.SignalPayload{
			CallerID: pairCallerID,
			SDP:      offerData,
		})
		if err != nil {
			return 0, err
		}
		return time.Since(start), nil

	case "dht":
		offerKey := dhtOfferKey(pairCallerID, pairCalleeID)
		if err := agentDHTNode.StoreValue(ctx, offerKey, dht.ValueMeta{
			Inline: true, Data: []byte(offerData),
		}); err != nil {
			return 0, fmt.Errorf("store offer: %w", err)
		}
		if err := agentDHTNode.StoreValue(ctx, dhtDoorbellKey(pairCalleeID), dht.ValueMeta{
			Inline: true, Data: pairCallerID[:],
		}); err != nil {
			return 0, fmt.Errorf("doorbell: %w", err)
		}
		answerKey := dhtAnswerKey(pairCallerID, pairCalleeID)
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return 0, fmt.Errorf("timeout waiting for answer")
			case <-ticker.C:
				meta, err := agentDHTNode.FindValue(ctx, answerKey)
				if err == nil && meta != nil && meta.Inline && len(meta.Data) > 0 {
					return time.Since(start), nil
				}
			}
		}
	}
	return 0, fmt.Errorf("unknown signaling")
}

func runCoordinator(signaling string, pairs int, agentsFlag string, coordPort int, outFile string, timeout time.Duration) {
	agents := parseCSV(agentsFlag)
	if len(agents) == 0 {
		log.Fatal("--agents required")
	}
	n := len(agents)

	for i, a := range agents {
		info, err := getAgentInfo(a)
		if err != nil {
			log.Fatalf("agent %s: %v", a, err)
		}
		fmt.Printf("agent[%d] %s node=%s...\n", i, a, info.NodeIDHex[:12])
	}

	var (
		mu      sync.Mutex
		results []PairResult
		done    = make(chan struct{}, 1)
	)
	myIP := resolveIP("0.0.0.0")
	callbackURL := fmt.Sprintf("http://%s:%d/result", myIP, coordPort)
	recordResult := func(res PairResult) {
		mu.Lock()
		results = append(results, res)
		cnt := len(results)
		mu.Unlock()
		fmt.Printf("  pair=%d latency=%v err=%q (%d/%d)\n",
			res.PairID, time.Duration(res.LatencyNs), res.Error, cnt, pairs)
		if cnt >= pairs {
			select {
			case done <- struct{}{}:
			default:
			}
		}
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/result", func(w http.ResponseWriter, r *http.Request) {
		var res PairResult
		if err := json.NewDecoder(r.Body).Decode(&res); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		recordResult(res)
		w.WriteHeader(http.StatusOK)
	})
	srv := &http.Server{Addr: fmt.Sprintf(":%d", coordPort), Handler: mux}
	go srv.ListenAndServe()

	fmt.Printf("\n=== signaling=%s pairs=%d agents=%d ===\n", signaling, pairs, n)

	calleeIDs := make([]string, pairs)
	callerIDs := make([]string, pairs)
	for i := 0; i < pairs; i++ {
		calleeIDs[i] = randomIDHex()
		callerIDs[i] = randomIDHex()
	}

	callerStagger := time.Duration(0)
	if signaling == "dht" && pairs > 10 {
		callerStagger = 20 * time.Millisecond
	}

	const warmupMs = 600
	const pollWindowMs = 1000

	var calleeWg sync.WaitGroup
	for i := 0; i < pairs; i++ {
		calleeWg.Add(1)
		go func(i int) {
			defer calleeWg.Done()
			agentIdx := i % n
			startDelayMs := 0
			if signaling == "dht" && callerStagger > 0 {
				callerFireDelayMs := warmupMs + i*int(callerStagger.Milliseconds())
				delay := callerFireDelayMs - pollWindowMs/2
				if delay > 0 {
					startDelayMs = delay
				}
			}
			req := RunPairRequest{
				PairID:          i,
				Role:            "callee",
				PairCalleeIDHex: calleeIDs[i],
				PairCallerIDHex: callerIDs[i],
				StartDelayMs:    startDelayMs,
			}
			dispatched := false
			for attempt := 0; attempt < n; attempt++ {
				idx := (agentIdx + attempt) % n
				if err := tryPostJSON("http://"+agents[idx]+"/run-pair", req); err == nil {
					dispatched = true
					break
				} else {
					log.Printf("pair=%d callee: agent %s failed (attempt %d/%d): %v",
						i, agents[idx], attempt+1, n, err)
					time.Sleep(20 * time.Millisecond)
				}
			}
			if !dispatched {
				log.Printf("pair=%d callee: all agents failed, pair will timeout", i)
			}
		}(i)
	}
	calleeWg.Wait()

	time.Sleep(warmupMs * time.Millisecond)

	if callerStagger > 0 {
		for i := 0; i < pairs; i++ {
			callerAgentIdx := (i + (n+1)/2) % n
			req := RunPairRequest{
				PairID:          i,
				Role:            "caller",
				PairCalleeIDHex: calleeIDs[i],
				PairCallerIDHex: callerIDs[i],
				CallbackURL:     callbackURL,
			}
			dispatched := false
			for attempt := 0; attempt < n; attempt++ {
				idx := (callerAgentIdx + attempt) % n
				if err := tryPostJSON("http://"+agents[idx]+"/run-pair", req); err == nil {
					dispatched = true
					break
				} else {
					log.Printf("pair=%d caller: agent %s failed (attempt %d/%d): %v",
						i, agents[idx], attempt+1, n, err)
				}
			}
			if !dispatched {
				log.Printf("pair=%d caller: all agents failed, recording error", i)
				recordResult(PairResult{PairID: i, Signaling: signaling, Error: "all agents unreachable for caller dispatch"})
			}
			time.Sleep(callerStagger)
		}
	} else {
		var callerWg sync.WaitGroup
		for i := 0; i < pairs; i++ {
			callerWg.Add(1)
			go func(i int) {
				defer callerWg.Done()
				callerAgentIdx := (i + (n+1)/2) % n
				req := RunPairRequest{
					PairID:          i,
					Role:            "caller",
					PairCalleeIDHex: calleeIDs[i],
					PairCallerIDHex: callerIDs[i],
					CallbackURL:     callbackURL,
				}
				dispatched := false
				for attempt := 0; attempt < n; attempt++ {
					idx := (callerAgentIdx + attempt) % n
					if err := tryPostJSON("http://"+agents[idx]+"/run-pair", req); err == nil {
						dispatched = true
						break
					} else {
						log.Printf("pair=%d caller: agent %s failed (attempt %d/%d): %v",
							i, agents[idx], attempt+1, n, err)
						time.Sleep(20 * time.Millisecond)
					}
				}
				if !dispatched {
					log.Printf("pair=%d caller: all agents failed, recording error", i)
					recordResult(PairResult{PairID: i, Signaling: signaling, Error: "all agents unreachable for caller dispatch"})
				}
			}(i)
		}
		callerWg.Wait()
	}

	fmt.Printf("waiting up to %v...\n", timeout)
	timer := time.NewTimer(timeout)
	select {
	case <-done:
		fmt.Println("all results received")
	case <-timer.C:
		fmt.Println("timeout")
	}

	shutCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	srv.Shutdown(shutCtx)

	mu.Lock()
	output := CoordOutput{Signaling: signaling, PairCount: pairs, Results: results}
	mu.Unlock()

	f, _ := os.Create(outFile)
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	enc.Encode(output)
	f.Close()
	fmt.Printf("wrote %s\n", outFile)
}

func dhtOfferKey(callerID, calleeID dht.NodeId) dht.DHTKey {
	raw := append([]byte("s2of:"), callerID[:]...)
	return dht.KeyFromBytes(append(raw, calleeID[:]...))
}
func dhtAnswerKey(callerID, calleeID dht.NodeId) dht.DHTKey {
	raw := append([]byte("s2an:"), callerID[:]...)
	return dht.KeyFromBytes(append(raw, calleeID[:]...))
}
func dhtDoorbellKey(calleeID dht.NodeId) dht.DHTKey {
	return dht.KeyFromBytes(append([]byte("s2bell:"), calleeID[:]...))
}

func randomIDHex() string {
	b := make([]byte, 20)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b)
}

func nodeIDFromHex(h string) dht.NodeId {
	b, _ := hex.DecodeString(h)
	var id dht.NodeId
	copy(id[:], b)
	return id
}

func resolveIP(addr string) string {
	if addr != "0.0.0.0" && addr != "" {
		return addr
	}
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		return "127.0.0.1"
	}
	defer conn.Close()
	return conn.LocalAddr().(*net.UDPAddr).IP.String()
}

func parseCSV(s string) []string {
	if s == "" {
		return nil
	}
	var out []string
	cur := ""
	for _, c := range s {
		if c == ',' {
			out = append(out, cur)
			cur = ""
		} else {
			cur += string(c)
		}
	}
	return append(out, cur)
}

func getAgentInfo(agentAddr string) (AgentInfo, error) {
	resp, err := http.Get("http://" + agentAddr + "/info")
	if err != nil {
		return AgentInfo{}, err
	}
	defer resp.Body.Close()
	var info AgentInfo
	return info, json.NewDecoder(resp.Body).Decode(&info)
}

func tryPostJSON(url string, v interface{}) error {
	b, _ := json.Marshal(v)
	resp, err := http.Post(url, "application/json", bytes.NewReader(b))
	if err != nil {
		return err
	}
	resp.Body.Close()
	return nil
}
