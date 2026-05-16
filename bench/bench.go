package main

import (
	"bufio"
	"flag"
	"fmt"
	"math/rand"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"pulse/internal/proto"
	"pulse/internal/server"
	"pulse/internal/topic"
	"pulse/internal/wal"
)

var (
	mode      = flag.String("mode", "internal", "benchmark mode: internal, network")
	subs      = flag.Int("subs", 1, "number of subscribers")
	msgs      = flag.Int("msgs", 100000, "number of messages")
	msgSize   = flag.Int("msg-size", 64, "message size in bytes")
	addr      = flag.String("addr", "localhost:7777", "server address")
)

func main() {
	flag.Parse()

	switch *mode {
	case "internal":
		runInternalBenchmark()
	case "network":
		runNetworkBenchmark()
	default:
		fmt.Printf("unknown mode: %s\n", *mode)
	}
}

func runInternalBenchmark() {
	dir := "/tmp/pulse-bench-wal"
	os.RemoveAll(dir)

	walLog, _ := wal.Open(dir)
	tm := topic.NewManager(walLog, 131072)

	topicName := "bench"

	subChans := make([]chan []byte, *subs)
	for i := 0; i < *subs; i++ {
		ch := make(chan []byte, 1024)
		tm.Subscribe(topicName, 0, ch)
		subChans[i] = ch
	}

	time.Sleep(100 * time.Millisecond)

	msg := make([]byte, *msgSize)
	rand.Read(msg)

	start := time.Now()

	for i := 0; i < *msgs; i++ {
		tm.Publish(topicName, msg)
	}

	elapsed := time.Since(start)

	var received int64
	var wg sync.WaitGroup
	wg.Add(*subs)

	for _, ch := range subChans {
		ch := ch
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ch:
					atomic.AddInt64(&received, 1)
				case <-time.After(100 * time.Millisecond):
					return
				}
			}
		}()
	}

	wg.Wait()

	totalMsgs := int64(*msgs)
	throughput := float64(totalMsgs) / elapsed.Seconds()
	fanout := throughput * float64(*subs)

	fmt.Printf("Internal Benchmark:\n")
	fmt.Printf("  Messages: %d\n", *msgs)
	fmt.Printf("  Subscribers: %d\n", *subs)
	fmt.Printf("  Msg Size: %d bytes\n", *msgSize)
	fmt.Printf("  Duration: %v\n", elapsed)
	fmt.Printf("  Publish Rate: %.0f msgs/s\n", throughput)
	fmt.Printf("  Total Fanout: %.0f msgs/s\n", fanout)
}

func runNetworkBenchmark() {
	dir := "/tmp/pulse-bench-wal-net"
	os.RemoveAll(dir)

	walLog, _ := wal.Open(dir)
	tm := topic.NewManager(walLog, 131072)
	srv := server.New("localhost:0", tm)
	srv.Start()
	defer srv.Stop()

	subConns := make([]net.Conn, *subs)
	subReaders := make([]*bufio.Reader, *subs)

	for i := 0; i < *subs; i++ {
		conn, err := net.Dial("tcp", srv.Addr())
		if err != nil {
			fmt.Printf("failed to connect: %v\n", err)
			return
		}

		writer := bufio.NewWriter(conn)
		reader := bufio.NewReader(conn)

		sub := &proto.Subscribe{Topic: "bench", Offset: 0}
		proto.WriteFrame(writer, proto.OpSubscribe, proto.EncodeSubscribe(sub))
		writer.Flush()
		proto.ReadFrame(reader)

		subConns[i] = conn
		subReaders[i] = reader
	}

	time.Sleep(100 * time.Millisecond)

	conn, err := net.Dial("tcp", srv.Addr())
	if err != nil {
		fmt.Printf("failed to connect: %v\n", err)
		return
	}
	defer conn.Close()

	writer := bufio.NewWriter(conn)
	reader := bufio.NewReader(conn)

	sub := &proto.Subscribe{Topic: "bench", Offset: 0}
	proto.WriteFrame(writer, proto.OpSubscribe, proto.EncodeSubscribe(sub))
	writer.Flush()
	proto.ReadFrame(reader)

	msg := make([]byte, *msgSize)
	rand.Read(msg)

	start := time.Now()

	for i := 0; i < *msgs; i++ {
		publish := &proto.Publish{Topic: "bench", Msg: msg}
		proto.WriteFrame(writer, proto.OpPublish, proto.EncodePublish(publish))
		writer.Flush()
		proto.ReadFrame(reader)
	}

	elapsed := time.Since(start)

	var received int64
	var wg sync.WaitGroup
	wg.Add(*subs)

	for _, r := range subReaders {
		reader := r
		go func() {
			defer wg.Done()
			count := 0
			for count < *msgs {
				frame, err := proto.ReadFrame(reader)
				if err != nil {
					return
				}
				if frame.Op == proto.OpDeliver {
					count++
					atomic.AddInt64(&received, 1)
				}
			}
		}()
	}

	wg.Wait()

	totalMsgs := int64(*msgs)
	throughput := float64(totalMsgs) / elapsed.Seconds()
	fanout := throughput * float64(*subs)

	fmt.Printf("Network Benchmark:\n")
	fmt.Printf("  Messages: %d\n", *msgs)
	fmt.Printf("  Subscribers: %d\n", *subs)
	fmt.Printf("  Msg Size: %d bytes\n", *msgSize)
	fmt.Printf("  Duration: %v\n", elapsed)
	fmt.Printf("  Publish Rate: %.0f msgs/s\n", throughput)
	fmt.Printf("  Total Fanout: %.0f msgs/s\n", fanout)
}