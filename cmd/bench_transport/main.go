package main

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/Polusummator/webrtc-dht/dht"
	udptransport "github.com/Polusummator/webrtc-dht/transport/udp"
	webrtctransport "github.com/Polusummator/webrtc-dht/transport/webrtc"
)

type Result struct {
	Transport    string `json:"transport"`
	Mode         string `json:"mode"`
	PayloadBytes int    `json:"payload_bytes"`
	Iteration    int    `json:"iteration"`
	DurationNs   int64  `json:"duration_ns"`
	Transferred  int64  `json:"transferred_bytes,omitempty"`
	ConnSetupNs  int64  `json:"conn_setup_ns,omitempty"`
}

func main() {
	role := flag.String("role", "server", "server or client")
	transport := flag.String("transport", "udp", "udp or webrtc")
	mode := flag.String("mode", "ping", "ping | store | findvalue | blob")
	payload := flag.Int("payload", 64, "payload size in bytes")
	iters := flag.Int("iterations", 1000, "number of iterations")
	localAddr := flag.String("addr", "0.0.0.0", "local bind address")
	localPort := flag.Int("port", 7000, "local port")
	remoteAddr := flag.String("remote-addr", "127.0.0.1", "remote address (client only)")
	remotePort := flag.Int("remote-port", 7001, "remote port (client only)")
	sigPort := flag.Int("sig-port", 8100, "local signaling HTTP port (webrtc only)")
	remoteSigPort := flag.Int("remote-sig-port", 8100, "remote signaling HTTP port (webrtc client only)")
	blobDir := flag.String("blob-dir", os.TempDir(), "directory for blob storage (server blob mode)")
	out := flag.String("out", "results.json", "output JSON file")
	flag.Parse()

	localNode := dht.NewNetworkNode(*localAddr, *localPort)
	var tr dht.Transport

	switch *transport {
	case "udp":
		tr = udptransport.NewTransport(localNode)
	case "webrtc":
		sig := webrtctransport.NewDirectSignaler(*sigPort)
		tr = webrtctransport.NewTransport(localNode, sig)
	default:
		log.Fatalf("unknown transport: %s", *transport)
	}

	var storage dht.Storage
	if *role == "server" && *mode == "blob" {
		ds := dht.NewDiskStorage(*blobDir)
		blobData := make([]byte, *payload)
		ref, err := ds.PutBlob(blobData)
		if err != nil {
			log.Fatalf("pre-store blob: %v", err)
		}
		fmt.Printf("blob ref: %s\n", ref)
		storage = ds
	}

	var node *dht.Node
	if storage != nil {
		node = dht.NewNodeWithStorage(localNode, tr, storage)
	} else {
		node = dht.NewNode(localNode, tr)
	}
	if err := node.Start(); err != nil {
		log.Fatalf("start node: %v", err)
	}
	defer node.Close()

	if *role == "server" {
		fmt.Printf("server listening on %s:%d (transport=%s mode=%s)\n", *localAddr, *localPort, *transport, *mode)
		select {}
	}

	remoteNode := dht.NewNetworkNode(*remoteAddr, *remotePort)
	if *transport == "webrtc" {
		remoteNode.Meta = map[string]string{"sig_port": fmt.Sprintf("%d", *remoteSigPort)}
	}

	fmt.Printf("connecting to %s:%d ...\n", *remoteAddr, *remotePort)
	connStart := time.Now()
	if err := tr.Ping(remoteNode); err != nil {
		log.Fatalf("initial ping failed: %v", err)
	}
	connSetupNs := time.Since(connStart).Nanoseconds()
	fmt.Printf("connected in %v\n", time.Duration(connSetupNs))

	var results []Result

	switch *mode {
	case "ping":
		for i := 0; i < *iters; i++ {
			start := time.Now()
			if err := tr.Ping(remoteNode); err != nil {
				log.Printf("ping %d failed: %v", i, err)
				continue
			}
			results = append(results, Result{
				Transport:   *transport,
				Mode:        "ping",
				Iteration:   i,
				DurationNs:  time.Since(start).Nanoseconds(),
				ConnSetupNs: connSetupNs,
			})
		}

	case "store":
		data := make([]byte, *payload)
		_, _ = rand.Read(data)
		vm := dht.ValueMeta{Inline: true, Data: data}
		key := dht.GenerateKey()
		for i := 0; i < *iters; i++ {
			start := time.Now()
			if err := tr.Store(remoteNode, key, vm); err != nil {
				log.Printf("store %d failed: %v", i, err)
				continue
			}
			results = append(results, Result{
				Transport:    *transport,
				Mode:         "store",
				PayloadBytes: *payload,
				Iteration:    i,
				DurationNs:   time.Since(start).Nanoseconds(),
				Transferred:  int64(*payload),
				ConnSetupNs:  connSetupNs,
			})
		}

	case "findvalue":
		data := make([]byte, *payload)
		_, _ = rand.Read(data)
		vm := dht.ValueMeta{Inline: true, Data: data}
		key := dht.GenerateKey()
		if err := tr.Store(remoteNode, key, vm); err != nil {
			log.Fatalf("store for findvalue failed: %v", err)
		}
		for i := 0; i < *iters; i++ {
			start := time.Now()
			_, _, err := tr.FindValue(remoteNode, key)
			if err != nil {
				log.Printf("findvalue %d failed: %v", i, err)
				continue
			}
			results = append(results, Result{
				Transport:    *transport,
				Mode:         "findvalue",
				PayloadBytes: *payload,
				Iteration:    i,
				DurationNs:   time.Since(start).Nanoseconds(),
				Transferred:  int64(*payload),
				ConnSetupNs:  connSetupNs,
			})
		}

	case "blob":
		blobData := make([]byte, *payload)
		sum := sha256.Sum256(blobData)
		ref := hex.EncodeToString(sum[:])
		fmt.Printf("fetching blob ref %s (%d bytes) ...\n", ref, *payload)
		for i := 0; i < *iters; i++ {
			start := time.Now()
			got, err := tr.FetchBlob(remoteNode, ref)
			if err != nil {
				log.Printf("fetchblob %d failed: %v", i, err)
				continue
			}
			results = append(results, Result{
				Transport:    *transport,
				Mode:         "blob",
				PayloadBytes: *payload,
				Iteration:    i,
				DurationNs:   time.Since(start).Nanoseconds(),
				Transferred:  int64(len(got)),
				ConnSetupNs:  connSetupNs,
			})
		}

	default:
		log.Fatalf("unknown mode: %s", *mode)
	}

	f, err := os.Create(*out)
	if err != nil {
		log.Fatalf("create output file: %v", err)
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	if err := enc.Encode(results); err != nil {
		log.Fatalf("encode results: %v", err)
	}
	fmt.Printf("wrote %d results to %s\n", len(results), *out)
}
