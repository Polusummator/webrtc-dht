package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"sort"
	"sync"
	"time"

	pion "github.com/pion/webrtc/v4"
)

const (
	labelCtrl    = "ctrl"
	labelBlob    = "blob"
	chunkSz      = 256 * 1024
	bufThreshold = uint64(8 << 20)
	sctpRecvBuf  = 8 << 20

	frameHint  = byte(0)
	frameChunk = byte(1)
	frameLast  = byte(2)
)

type Result struct {
	SizeBytes   int   `json:"size_bytes"`
	Iteration   int   `json:"iteration"`
	DurationNs  int64 `json:"duration_ns"`
	Transferred int64 `json:"transferred_bytes"`
}

func main() {
	role := flag.String("role", "server", "server | client")
	sig := flag.String("sig", "0.0.0.0:8300", "server bind addr  OR  http://host:port  for client")
	iters := flag.Int("iters", 100, "iterations per blob size (client only)")
	outFile := flag.String("out", "", "output JSON file (client only)")
	flag.Parse()

	switch *role {
	case "server":
		runServer(*sig)
	case "client":
		url := *sig
		if len(url) > 0 && url[0] != 'h' {
			url = "http://" + url
		}
		runClient(url, *iters, *outFile)
	default:
		log.Fatalf("unknown role %q", *role)
	}
}

func newPC() (*pion.PeerConnection, error) {
	se := pion.SettingEngine{}
	se.SetICETimeouts(2*time.Second, 2*time.Second, 500*time.Millisecond)
	se.SetSCTPMaxReceiveBufferSize(uint32(sctpRecvBuf))
	api := pion.NewAPI(pion.WithSettingEngine(se))
	return api.NewPeerConnection(pion.Configuration{})
}

func sendBlob(dc *pion.DataChannel, lowSig <-chan struct{}, blob []byte) {
	hint := make([]byte, 9)
	hint[0] = frameHint
	binary.LittleEndian.PutUint64(hint[1:], uint64(len(blob)))
	if err := dc.Send(hint); err != nil {
		return
	}
	for offset := 0; offset < len(blob); {
		for dc.BufferedAmount() > bufThreshold {
			<-lowSig
		}
		end := offset + chunkSz
		ft := frameChunk
		if end >= len(blob) {
			end = len(blob)
			ft = frameLast
		}
		frame := make([]byte, 1+end-offset)
		frame[0] = ft
		copy(frame[1:], blob[offset:end])
		if err := dc.Send(frame); err != nil {
			return
		}
		offset = end
	}
}

var blobStore = map[uint64][]byte{}
var blobSizes = []int{64 << 10, 256 << 10, 512 << 10, 1 << 20, 4 << 20}

func runServer(addr string) {
	r := rand.New(rand.NewSource(42))
	for _, sz := range blobSizes {
		b := make([]byte, sz)
		r.Read(b)
		blobStore[uint64(sz)] = b
		log.Printf("pre-allocated %dKB blob", sz>>10)
	}
	http.HandleFunc("/offer", httpOfferHandler)
	log.Printf("server: HTTP signaling on %s", addr)
	log.Fatal(http.ListenAndServe(addr, nil))
}

func httpOfferHandler(w http.ResponseWriter, r *http.Request) {
	var offer pion.SessionDescription
	if err := json.NewDecoder(r.Body).Decode(&offer); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	answer, err := acceptConnection(offer)
	if err != nil {
		log.Printf("acceptConnection: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(answer)
}

func acceptConnection(offer pion.SessionDescription) (pion.SessionDescription, error) {
	pc, err := newPC()
	if err != nil {
		return pion.SessionDescription{}, err
	}

	var (
		blobDCMu sync.Mutex
		blobDC   *pion.DataChannel
		blobOnce sync.Once
		blobOpen = make(chan struct{})
		lowSig   = make(chan struct{}, 1)
	)

	pc.OnDataChannel(func(dc *pion.DataChannel) {
		switch dc.Label() {

		case labelBlob:
			blobDCMu.Lock()
			blobDC = dc
			blobDCMu.Unlock()
			dc.OnOpen(func() {
				dc.SetBufferedAmountLowThreshold(bufThreshold)
				dc.OnBufferedAmountLow(func() {
					select {
					case lowSig <- struct{}{}:
					default:
					}
				})
				blobOnce.Do(func() { close(blobOpen) })
			})

		case labelCtrl:
			dc.OnMessage(func(msg pion.DataChannelMessage) {
				if len(msg.Data) < 8 {
					return
				}
				sz := binary.LittleEndian.Uint64(msg.Data[:8])
				blob, ok := blobStore[sz]
				if !ok {
					log.Printf("server: no blob for size %d", sz)
					return
				}
				go func() {
					select {
					case <-blobOpen:
					case <-time.After(10 * time.Second):
						log.Printf("server: blob DC not ready after 10s")
						return
					}
					blobDCMu.Lock()
					dc := blobDC
					blobDCMu.Unlock()
					sendBlob(dc, lowSig, blob)
				}()
			})
		}
	})

	if err := pc.SetRemoteDescription(offer); err != nil {
		return pion.SessionDescription{}, err
	}
	answer, err := pc.CreateAnswer(nil)
	if err != nil {
		return answer, err
	}
	gathered := pion.GatheringCompletePromise(pc)
	if err := pc.SetLocalDescription(answer); err != nil {
		return answer, err
	}
	<-gathered
	return *pc.LocalDescription(), nil
}

type blobReceiver struct {
	mu    sync.Mutex
	buf   []byte
	notif chan []byte
}

func (rx *blobReceiver) onMessage(msg pion.DataChannelMessage) {
	if len(msg.Data) < 1 {
		return
	}
	ft := msg.Data[0]
	payload := msg.Data[1:]

	rx.mu.Lock()
	switch ft {
	case frameHint:
		if len(payload) < 8 {
			rx.mu.Unlock()
			return
		}
		sz := binary.LittleEndian.Uint64(payload)
		rx.buf = make([]byte, 0, sz)
		rx.mu.Unlock()
	case frameChunk:
		rx.buf = append(rx.buf, payload...)
		rx.mu.Unlock()
	case frameLast:
		rx.buf = append(rx.buf, payload...)
		full := rx.buf
		rx.buf = nil
		ch := rx.notif
		rx.notif = nil
		rx.mu.Unlock()
		if ch != nil {
			select {
			case ch <- full:
			default:
			}
		}
	default:
		rx.mu.Unlock()
	}
}

func runClient(sigURL string, iters int, outFile string) {
	pc, err := newPC()
	if err != nil {
		log.Fatalf("newPC: %v", err)
	}
	defer pc.Close()

	ctrlDC, err := pc.CreateDataChannel(labelCtrl, nil)
	if err != nil {
		log.Fatalf("create ctrl DC: %v", err)
	}
	blobDC, err := pc.CreateDataChannel(labelBlob, nil)
	if err != nil {
		log.Fatalf("create blob DC: %v", err)
	}

	blobOpen := make(chan struct{})
	lowSig := make(chan struct{}, 1)
	blobDC.OnOpen(func() {
		blobDC.SetBufferedAmountLowThreshold(bufThreshold)
		blobDC.OnBufferedAmountLow(func() {
			select {
			case lowSig <- struct{}{}:
			default:
			}
		})
		close(blobOpen)
	})
	_ = lowSig

	rx := &blobReceiver{}
	blobDC.OnMessage(rx.onMessage)

	offer, err := pc.CreateOffer(nil)
	if err != nil {
		log.Fatalf("CreateOffer: %v", err)
	}
	gathered := pion.GatheringCompletePromise(pc)
	if err := pc.SetLocalDescription(offer); err != nil {
		log.Fatalf("SetLocalDescription: %v", err)
	}
	<-gathered

	offerJSON, _ := json.Marshal(pc.LocalDescription())
	resp, err := http.Post(sigURL+"/offer", "application/json", bytes.NewReader(offerJSON))
	if err != nil {
		log.Fatalf("POST /offer: %v", err)
	}
	defer resp.Body.Close()

	var answer pion.SessionDescription
	if err := json.NewDecoder(resp.Body).Decode(&answer); err != nil {
		log.Fatalf("decode answer: %v", err)
	}
	if err := pc.SetRemoteDescription(answer); err != nil {
		log.Fatalf("SetRemoteDescription: %v", err)
	}

	select {
	case <-blobOpen:
	case <-time.After(15 * time.Second):
		log.Fatal("blob DC not ready after 15s")
	}

	ctrlReady := make(chan struct{})
	ctrlDC.OnOpen(func() { close(ctrlReady) })
	select {
	case <-ctrlReady:
	case <-time.After(15 * time.Second):
		log.Fatal("ctrl DC not ready after 15s")
	}

	var allResults []Result

	for _, sz := range blobSizes {
		var durs []time.Duration
		label := fmt.Sprintf("%dKB", sz>>10)
		if sz >= 1<<20 {
			label = fmt.Sprintf("%dMB", sz>>20)
		}

		for i := 0; i < iters; i++ {
			ch := make(chan []byte, 1)
			rx.mu.Lock()
			rx.notif = ch
			rx.mu.Unlock()

			cmd := make([]byte, 8)
			binary.LittleEndian.PutUint64(cmd, uint64(sz))
			start := time.Now()
			if err := ctrlDC.Send(cmd); err != nil {
				log.Printf("[%s] iter %d send error: %v", label, i, err)
				continue
			}

			select {
			case data := <-ch:
				elapsed := time.Since(start)
				if len(data) != sz {
					log.Printf("[%s] iter %d size mismatch: got %d want %d", label, i, len(data), sz)
					continue
				}
				durs = append(durs, elapsed)
				allResults = append(allResults, Result{
					SizeBytes:   sz,
					Iteration:   i,
					DurationNs:  elapsed.Nanoseconds(),
					Transferred: int64(len(data)),
				})
			case <-time.After(30 * time.Second):
				log.Printf("[%s] iter %d timeout", label, i)
			}
		}

		if len(durs) == 0 {
			fmt.Printf("%-6s FAILED (all iterations errored)\n", label)
			continue
		}
		med := medianDur(durs)
		minD, maxD := durs[0], durs[0]
		for _, d := range durs {
			if d < minD {
				minD = d
			}
			if d > maxD {
				maxD = d
			}
		}
		mbps := float64(sz) / med.Seconds() / 1e6
		fmt.Printf("%-6s n=%d  min=%5.1fms  median=%5.1fms  max=%6.1fms  throughput=%.1f MB/s\n",
			label, len(durs),
			float64(minD.Milliseconds()),
			float64(med.Milliseconds()),
			float64(maxD.Milliseconds()),
			mbps)
	}

	if outFile != "" {
		f, err := os.Create(outFile)
		if err != nil {
			log.Fatalf("create %s: %v", outFile, err)
		}
		defer f.Close()
		enc := json.NewEncoder(f)
		enc.SetIndent("", "  ")
		_ = enc.Encode(allResults)
		fmt.Printf("results written to %s\n", outFile)
	}
}

func medianDur(ds []time.Duration) time.Duration {
	if len(ds) == 0 {
		return 0
	}
	cp := make([]time.Duration, len(ds))
	copy(cp, ds)
	sort.Slice(cp, func(i, j int) bool { return cp[i] < cp[j] })
	return cp[len(cp)/2]
}
