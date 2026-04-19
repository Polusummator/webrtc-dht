package main

import (
	"context"
	"fmt"
	"time"

	"github.com/Polusummator/webrtc-dht/dht"
	udptransport "github.com/Polusummator/webrtc-dht/transport/udp"
)

func main() {
	net1 := dht.NewNetworkNode("127.0.0.1", 8000)
	node1 := dht.NewNode(net1, udptransport.NewTransport(net1))
	if err := node1.Start(); err != nil {
		fmt.Println("Node1 start error:", err)
		return
	}
	defer node1.Close()

	net2 := dht.NewNetworkNode("127.0.0.1", 8001)
	node2 := dht.NewNode(net2, udptransport.NewTransport(net2))
	if err := node2.Start(); err != nil {
		fmt.Println("Node2 start error:", err)
		return
	}
	defer node2.Close()

	time.Sleep(100 * time.Millisecond)

	ctx := context.Background()

	if err := node2.Bootstrap(ctx, []*dht.NetworkNode{net1}); err != nil {
		fmt.Println("Bootstrap error:", err)
	}

	testKey := dht.KeyFromString("test_key")
	testData := dht.ValueMeta{Inline: true, Data: []byte("Hello world")}
	if err := node1.StoreValue(ctx, testKey, testData); err != nil {
		fmt.Println("StoreValue error:", err)
	}

	time.Sleep(100 * time.Millisecond)

	val, err := node2.FindValue(ctx, testKey)
	switch {
	case err != nil:
		fmt.Println("FindValue error:", err)
	case val != nil:
		fmt.Println("Retrieved:", string(val.Data))
	default:
		fmt.Println("Not found")
	}
}
