package main

import (
	"fmt"
	"sort"
	"sync"
	"time"

	pion "github.com/pion/webrtc/v4"
)

const (
	chunkSize  = 256 * 1024
	totalBytes = 32 * 1024 * 1024
	bufThresh  = uint64(8 << 20)
)

var blobSizes = []int{
	64 * 1024,
	256 * 1024,
	512 * 1024,
	1 * 1024 * 1024,
	4 * 1024 * 1024,
}

func main() {
	fmt.Printf("%-10s  %8s  %8s  %8s  %8s  %10s\n", "blob-size", "iters", "min-ms", "med-ms", "p95-ms", "MB/s")
	fmt.Println("----------  --------  --------  --------  --------  ----------")

	for _, sz := range blobSizes {
		label := fmt.Sprintf("%dKB", sz>>10)
		if sz >= 1<<20 {
			label = fmt.Sprintf("%dMB", sz>>20)
		}

		durs, err := bench(sz)
		if err != nil {
			fmt.Printf("%-10s  error: %v\n", label, err)
			continue
		}

		sort.Slice(durs, func(i, j int) bool { return durs[i] < durs[j] })
		minD := durs[0]
		med := durs[len(durs)/2]
		p95Idx := int(float64(len(durs)) * 0.95)
		if p95Idx >= len(durs) {
			p95Idx = len(durs) - 1
		}
		p95 := durs[p95Idx]
		mbps := float64(sz) / med.Seconds() / 1e6

		fmt.Printf("%-10s  %8d  %8.1f  %8.1f  %8.1f  %10.1f\n",
			label, len(durs),
			float64(minD.Milliseconds()),
			float64(med.Milliseconds()),
			float64(p95.Milliseconds()),
			mbps)
	}
}

func bench(blobSz int) ([]time.Duration, error) {
	iters := totalBytes / blobSz
	if iters < 8 {
		iters = 8
	}

	dc1, dc2, cleanup, err := setupPair()
	if err != nil {
		return nil, err
	}
	defer cleanup()

	chunk := make([]byte, chunkSize)
	for i := range chunk {
		chunk[i] = 0xAB
	}

	lowSig := make(chan struct{}, 1)
	dc1.SetBufferedAmountLowThreshold(bufThresh)
	dc1.OnBufferedAmountLow(func() {
		select {
		case lowSig <- struct{}{}:
		default:
		}
	})

	var durs []time.Duration

	for iter := 0; iter < iters; iter++ {
		received := 0
		var mu sync.Mutex
		done := make(chan struct{}, 1)

		dc2.OnMessage(func(m pion.DataChannelMessage) {
			mu.Lock()
			received += len(m.Data)
			if received >= blobSz {
				mu.Unlock()
				select {
				case done <- struct{}{}:
				default:
				}
				return
			}
			mu.Unlock()
		})

		start := time.Now()
		sent := 0
		for sent < blobSz {
			for dc1.BufferedAmount() > bufThresh {
				<-lowSig
			}
			end := sent + chunkSize
			if end > blobSz {
				end = blobSz
			}
			_ = dc1.Send(chunk[:end-sent])
			sent = end
		}

		select {
		case <-done:
			durs = append(durs, time.Since(start))
		case <-time.After(30 * time.Second):
			return nil, fmt.Errorf("recv timeout (iter %d)", iter)
		}
	}

	return durs, nil
}

func setupPair() (dc1, dc2 *pion.DataChannel, cleanup func(), err error) {
	api := newAPI()
	pc1, e := api.NewPeerConnection(pion.Configuration{})
	if e != nil {
		err = e
		return
	}
	pc2, e := api.NewPeerConnection(pion.Configuration{})
	if e != nil {
		_ = pc1.Close()
		err = e
		return
	}

	cleanup = func() {
		_ = pc1.Close()
		_ = pc2.Close()
	}

	pc1.OnICECandidate(func(c *pion.ICECandidate) {
		if c != nil {
			_ = pc2.AddICECandidate(c.ToJSON())
		}
	})
	pc2.OnICECandidate(func(c *pion.ICECandidate) {
		if c != nil {
			_ = pc1.AddICECandidate(c.ToJSON())
		}
	})

	dc1, err = pc1.CreateDataChannel("bench", nil)
	if err != nil {
		return
	}
	dc1Ready := make(chan struct{})
	dc1.OnOpen(func() { close(dc1Ready) })

	dc2Ready := make(chan struct{})
	pc2.OnDataChannel(func(dc *pion.DataChannel) {
		dc2 = dc
		dc.OnOpen(func() { close(dc2Ready) })
	})

	offer, e := pc1.CreateOffer(nil)
	if e != nil {
		err = e
		return
	}
	g1 := pion.GatheringCompletePromise(pc1)
	if err = pc1.SetLocalDescription(offer); err != nil {
		return
	}
	<-g1

	if err = pc2.SetRemoteDescription(*pc1.LocalDescription()); err != nil {
		return
	}
	answer, e := pc2.CreateAnswer(nil)
	if e != nil {
		err = e
		return
	}
	g2 := pion.GatheringCompletePromise(pc2)
	if err = pc2.SetLocalDescription(answer); err != nil {
		return
	}
	<-g2

	if err = pc1.SetRemoteDescription(*pc2.LocalDescription()); err != nil {
		return
	}

	select {
	case <-dc1Ready:
	case <-time.After(10 * time.Second):
		err = fmt.Errorf("dc1 open timeout")
		return
	}
	select {
	case <-dc2Ready:
	case <-time.After(10 * time.Second):
		err = fmt.Errorf("dc2 open timeout")
		return
	}
	return
}

func newAPI() *pion.API {
	se := pion.SettingEngine{}
	se.SetSCTPMaxReceiveBufferSize(8 << 20)
	return pion.NewAPI(pion.WithSettingEngine(se))
}
