package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/Polusummator/webrtc-dht/dht"
	udptransport "github.com/Polusummator/webrtc-dht/transport/udp"
)

func main() {
	ctx := context.Background()

	dir1, err := os.MkdirTemp("", "dht-node1-*")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir1)

	dir3, err := os.MkdirTemp("", "dht-node3-*")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir3)

	net1 := dht.NewNetworkNode("127.0.0.1", 9101)
	node1 := dht.NewNodeWithStorage(net1, udptransport.NewTransport(net1), dht.NewDiskStorage(dir1))
	if err := node1.Start(); err != nil {
		log.Fatal("node1 start:", err)
	}
	defer node1.Close()

	net2 := dht.NewNetworkNode("127.0.0.1", 9102)
	node2 := dht.NewNode(net2, udptransport.NewTransport(net2))
	if err := node2.Start(); err != nil {
		log.Fatal("node2 start:", err)
	}
	defer node2.Close()

	net3 := dht.NewNetworkNode("127.0.0.1", 9103)
	node3 := dht.NewNodeWithStorage(net3, udptransport.NewTransport(net3), dht.NewDiskStorage(dir3))
	if err := node3.Start(); err != nil {
		log.Fatal("node3 start:", err)
	}
	defer node3.Close()

	time.Sleep(100 * time.Millisecond)

	if err := node2.Bootstrap(ctx, []*dht.NetworkNode{net1}); err != nil {
		log.Println("node2 bootstrap (non-fatal):", err)
	}
	if err := node3.Bootstrap(ctx, []*dht.NetworkNode{net1}); err != nil {
		log.Println("node3 bootstrap (non-fatal):", err)
	}

	const blobSize = 200 * 1024
	blobData := make([]byte, blobSize)
	if _, err := rand.Read(blobData); err != nil {
		log.Fatal(err)
	}

	fileKey := dht.KeyFromString("bigfile.bin")
	if err := node1.StoreBlob(ctx, fileKey, blobData, "application/octet-stream"); err != nil {
		log.Fatal("StoreBlob:", err)
	}

	time.Sleep(100 * time.Millisecond)

	meta, err := node3.FindValue(ctx, fileKey)
	if err != nil {
		log.Fatal("FindValue:", err)
	}
	if meta == nil {
		log.Fatal("FindValue: not found")
	}
	fmt.Printf("Metadata: BlobRef=%.16s..., Size=%d bytes, Inline=%v\n",
		meta.BlobRef, meta.Size, meta.Inline)
	if meta.Data != nil {
		log.Fatal("something wrong: data in meta")
	}

	start := time.Now()
	received, err := node3.FetchBlob(ctx, meta)
	elapsed := time.Since(start)
	if err != nil {
		log.Fatal("FetchBlob:", err)
	}

	if len(received) != len(blobData) || !bytes.Equal(received, blobData) {
		log.Fatalf("data mismatch (got %d bytes, want %d)", len(received), len(blobData))
	}

	speed := float64(blobSize) / elapsed.Seconds() / 1024 / 1024
	fmt.Printf("%d KB received in %v (%.1f MB/s)\n", blobSize/1024, elapsed.Round(time.Millisecond), speed)
}
