package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/Polusummator/webrtc-dht/dht"
	wrtc "github.com/Polusummator/webrtc-dht/transport/webrtc"
)

func main() {
	ctx := context.Background()

	net1 := dht.NewNetworkNode("127.0.0.1", 7300)
	node1 := dht.NewNode(net1, wrtc.NewTransport(net1, wrtc.NewHybridSignaler(17300)))
	if err := node1.Start(); err != nil {
		log.Fatal("node1:", err)
	}
	defer node1.Close()

	net2 := dht.NewNetworkNode("127.0.0.1", 7301)
	node2 := dht.NewNode(net2, wrtc.NewTransport(net2, wrtc.NewHybridSignaler(17301)))
	if err := node2.Start(); err != nil {
		log.Fatal("node2:", err)
	}
	defer node2.Close()

	net3 := dht.NewNetworkNode("127.0.0.1", 7302)
	node3 := dht.NewNode(net3, wrtc.NewTransport(net3, wrtc.NewHybridSignaler(17302)))
	if err := node3.Start(); err != nil {
		log.Fatal("node3:", err)
	}
	defer node3.Close()

	time.Sleep(100 * time.Millisecond)

	if err := node2.Bootstrap(ctx, []*dht.NetworkNode{net1}); err != nil {
		log.Fatal("bootstrap node2:", err)
	}
	if err := node3.Bootstrap(ctx, []*dht.NetworkNode{net2}); err != nil {
		log.Fatal("bootstrap node3:", err)
	}

	key := dht.KeyFromString("hello")
	if err := node1.StoreValue(ctx, key, dht.ValueMeta{Inline: true, Data: []byte("world via DHT signaling")}); err != nil {
		log.Fatal("store:", err)
	}

	delete(net1.Meta, "sig_port")

	val, err := node3.FindValue(ctx, key)
	if err != nil {
		log.Fatal("find:", err)
	}
	fmt.Println("Retrieved:", string(val.Data))
}
