package main

import (
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/Polusummator/webrtc-dht/dht"
	webrtctransport "github.com/Polusummator/webrtc-dht/transport/webrtc"
)

type blobServer struct {
	blobs map[string][]byte
}

func (s *blobServer) OnPing(_ *dht.NetworkNode) error { return nil }
func (s *blobServer) OnStore(_ *dht.NetworkNode, _ dht.DHTKey, _ dht.ValueMeta) error {
	return nil
}
func (s *blobServer) OnFindNode(_ *dht.NetworkNode, _ dht.NodeId) ([]*dht.NetworkNode, error) {
	return nil, nil
}
func (s *blobServer) OnFindValue(_ *dht.NetworkNode, _ dht.DHTKey) (*dht.ValueMeta, []*dht.NetworkNode, error) {
	return nil, nil, dht.ErrNotFound
}
func (s *blobServer) OnFetchBlob(_ *dht.NetworkNode, ref string) ([]byte, error) {
	data, ok := s.blobs[ref]
	if !ok {
		return nil, dht.ErrNotFound
	}
	return data, nil
}

func main() {
	sizes := []int{64 * 1024, 256 * 1024, 512 * 1024, 1024 * 1024, 4 * 1024 * 1024}
	iters := 20

	serverNode := dht.NewNetworkNode("127.0.0.1", 9100)
	serverSig := webrtctransport.NewDirectSignaler(9200)
	serverTr := webrtctransport.NewTransport(serverNode, serverSig)

	blobs := make(map[string][]byte)
	for _, size := range sizes {
		data := make([]byte, size)
		rand.Read(data)
		blobs[fmt.Sprintf("blob-%d", size)] = data
	}
	srv := &blobServer{blobs: blobs}
	if err := serverTr.Listen(srv); err != nil {
		log.Fatalf("server Listen: %v", err)
	}

	time.Sleep(200 * time.Millisecond)

	clientNode := dht.NewNetworkNode("127.0.0.1", 9101)
	clientSig := webrtctransport.NewDirectSignaler(9201)
	clientTr := webrtctransport.NewTransport(clientNode, clientSig)
	clientNode.Meta = map[string]string{}
	serverNode.Meta = map[string]string{"sig_port": "9200"}

	clientDHTNode := dht.NewNode(clientNode, clientTr)
	if err := clientDHTNode.Start(); err != nil {
		log.Fatalf("client Start: %v", err)
	}

	if err := clientTr.Ping(serverNode); err != nil {
		log.Fatalf("initial ping: %v", err)
	}
	time.Sleep(200 * time.Millisecond)

	for _, size := range sizes {
		ref := fmt.Sprintf("blob-%d", size)
		var results []time.Duration
		for i := 0; i < iters; i++ {
			start := time.Now()
			got, err := clientTr.FetchBlob(serverNode, ref)
			elapsed := time.Since(start)
			if err != nil {
				log.Printf("[%dKB] iter %d error: %v", size/1024, i, err)
				continue
			}
			if len(got) != size {
				log.Printf("[%dKB] iter %d size mismatch: got %d", size/1024, i, len(got))
				continue
			}
			results = append(results, elapsed)
		}
		if len(results) == 0 {
			fmt.Printf("%-6s FAILED (all iterations errored)\n",
				fmt.Sprintf("%dKB", size/1024))
			continue
		}
		med := median(results)
		min, max := results[0], results[0]
		for _, d := range results {
			if d < min {
				min = d
			}
			if d > max {
				max = d
			}
		}
		mbps := float64(size) / med.Seconds() / 1e6
		label := fmt.Sprintf("%dKB", size/1024)
		if size >= 1024*1024 {
			label = fmt.Sprintf("%dMB", size/1024/1024)
		}
		fmt.Printf("%-6s n=%d  min=%5.1fms  median=%5.1fms  max=%6.1fms  throughput=%.1f MB/s\n",
			label, len(results),
			float64(min.Milliseconds()),
			float64(med.Milliseconds()),
			float64(max.Milliseconds()),
			mbps)
	}

	_ = clientTr.Close()
	_ = serverTr.Close()
}

func median(ds []time.Duration) time.Duration {
	if len(ds) == 0 {
		return 0
	}
	sorted := make([]time.Duration, len(ds))
	copy(sorted, ds)
	for i := 1; i < len(sorted); i++ {
		for j := i; j > 0 && sorted[j] < sorted[j-1]; j-- {
			sorted[j], sorted[j-1] = sorted[j-1], sorted[j]
		}
	}
	return sorted[len(sorted)/2]
}
