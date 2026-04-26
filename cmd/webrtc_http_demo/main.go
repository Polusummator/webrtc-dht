package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/Polusummator/webrtc-dht/dht"
	wrtc "github.com/Polusummator/webrtc-dht/transport/webrtc"
)

// go run ./cmd/signal_server -addr :9000

func main() {
	ctx := context.Background()
	sigServer := "http://127.0.0.1:9000"

	net1 := dht.NewNetworkNode("127.0.0.1", 7100)
	sig1 := wrtc.NewCentralSignaler(sigServer)
	node1 := dht.NewNode(net1, wrtc.NewTransport(net1, sig1))
	if err := node1.Start(); err != nil {
		log.Fatal("node1:", err)
	}
	defer node1.Close()

	net2 := dht.NewNetworkNode("127.0.0.1", 7101)
	sig2 := wrtc.NewCentralSignaler(sigServer)
	node2 := dht.NewNode(net2, wrtc.NewTransport(net2, sig2))
	if err := node2.Start(); err != nil {
		log.Fatal("node2:", err)
	}
	defer node2.Close()

	time.Sleep(200 * time.Millisecond)

	if err := node2.Bootstrap(ctx, []*dht.NetworkNode{net1}); err != nil {
		log.Println("bootstrap (non-fatal):", err)
	}

	key := dht.KeyFromString("hello")
	if err := node1.StoreValue(ctx, key, dht.ValueMeta{Inline: true, Data: []byte("world")}); err != nil {
		log.Fatal("store:", err)
	}

	time.Sleep(100 * time.Millisecond)

	val, err := node2.FindValue(ctx, key)
	if err != nil {
		log.Fatal("find:", err)
	}
	fmt.Println("Retrieved:", string(val.Data))
}
