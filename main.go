package main

import (
	"context"
	"fmt"
	"time"
)

func main() {
	netNode1 := NewNetworkNode("127.0.0.1", "8000")
	transport1 := NewUDPTransport(netNode1)
	node1 := NewNode(netNode1, transport1)
	if err := node1.Start(); err != nil {
		fmt.Println("Node1 start error:", err)
	}

	netNode2 := NewNetworkNode("127.0.0.1", "8001")
	transport2 := NewUDPTransport(netNode2)
	node2 := NewNode(netNode2, transport2)
	if err := node2.Start(); err != nil {
		fmt.Println("Node2 start error:", err)
	}

	time.Sleep(100 * time.Millisecond)

	err := node2.Bootstrap(context.Background(), []*NetworkNode{netNode1})
	if err != nil {
		fmt.Println("Bootstrap error:", err)
	}

	testKey := DHTKey("test_key")
	testData := ValueMeta{Inline: true, Data: []byte("Hello world")}
	err = node1.StoreValue(context.Background(), testKey, testData)
	if err != nil {
		fmt.Println("StoreValue error:", err)
	}

	time.Sleep(100 * time.Millisecond)

	val, err := node2.FindValue(context.Background(), testKey)
	if err != nil {
		fmt.Println("FindValue error:", err)
	} else if val != nil {
		fmt.Println("Retrieved:", string(val.Data))
	} else {
		fmt.Println("Not found")
	}
}
