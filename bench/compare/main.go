package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"math/rand"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"

	"pulse/internal/proto"
	"pulse/internal/server"
	"pulse/internal/topic"
	"pulse/internal/wal"
)

var (
	subs    = flag.Int("subs", 10, "number of subscribers")
	msgs    = flag.Int("msgs", 10000, "number of messages")
	msgSize = flag.Int("msg-size", 64, "message size in bytes")
)

func main() {
	flag.Parse()

	fmt.Println("=== Redis Pub/Sub Benchmark ===")
	runRedisBenchmark()

	fmt.Println("\n=== Pulse Pub/Sub Benchmark ===")
	runPulseBenchmark()
}

func runRedisBenchmark() {
	client := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	defer client.Close()

	ctx := context.Background()

	pubsubChans := make([]chan string, *subs)
	var wg sync.WaitGroup

	for i := 0; i < *subs; i++ {
		pubsub := client.Subscribe(ctx, "bench")
		ch := make(chan string, 256)
		pubsubChans[i] = ch

		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case msg, ok := <-pubsub.Channel():
					if !ok {
						return
					}
					ch <- msg.Payload
				case <-ctx.Done():
					return
				}
			}
		}()
	}

	time.Sleep(100 * time.Millisecond)

	msg := make([]byte, *msgSize)
	rand.Read(msg)

	start := time.Now()

	for i := 0; i < *msgs; i++ {
		client.Publish(ctx, "bench", string(msg))
	}

	elapsed := time.Since(start)

	var received int64
	wg.Add(*subs)

	for _, ch := range pubsubChans {
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

	fmt.Printf("  Messages: %d\n", *msgs)
	fmt.Printf("  Subscribers: %d\n", *subs)
	fmt.Printf("  Msg Size: %d bytes\n", *msgSize)
	fmt.Printf("  Duration: %v\n", elapsed)
	fmt.Printf("  Publish Rate: %.0f msgs/s\n", throughput)
	fmt.Printf("  Total Fanout: %.0f msgs/s\n", fanout)
}

func runPulseBenchmark() {
	dir := "/tmp/pulse-bench-redis"
	removeAll(dir)

	walLog, _ := wal.Open(dir)
	tm := topic.NewManager(walLog, 131072)
	srv := server.New("localhost:0", tm)
	srv.Start()
	defer srv.Stop()

	subConns := make([]net.Conn, *subs)
	subReaders := make([]*bufio.Reader, *subs)

	for i := 0; i < *subs; i++ {
		conn, _ := net.Dial("tcp", srv.Addr())
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

	conn, _ := net.Dial("tcp", srv.Addr())
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

	fmt.Printf("  Messages: %d\n", *msgs)
	fmt.Printf("  Subscribers: %d\n", *subs)
	fmt.Printf("  Msg Size: %d bytes\n", *msgSize)
	fmt.Printf("  Duration: %v\n", elapsed)
	fmt.Printf("  Publish Rate: %.0f msgs/s\n", throughput)
	fmt.Printf("  Total Fanout: %.0f msgs/s\n", fanout)
}

func removeAll(dir string) {
	os.RemoveAll(dir)
}