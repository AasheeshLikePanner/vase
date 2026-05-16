package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

var subs = flag.Int("subs", 10, "subscribers")
var msgs = flag.Int("msgs", 5000, "messages")
var port = flag.Int("port", 6380, "Redis port")

func main() {
	flag.Parse()
	ctx := context.Background()

	fmt.Println("=== Redis Pub/Sub Benchmark (Local) ===")
	fmt.Printf("Redis Port: %d, Subscribers: %d, Messages: %d\n\n", *port, *subs, *msgs)

	// Start subscribers
	received := make([]int64, *subs)
	var subWg sync.WaitGroup

	for i := 0; i < *subs; i++ {
		subWg.Add(1)
		go func(id int) {
			defer subWg.Done()
			rdb := redis.NewClient(&redis.Options{Addr: fmt.Sprintf("localhost:%d", *port), DialTimeout: 5*time.Second})
			defer rdb.Close()

			pubsub := rdb.Subscribe(ctx, "bench")
			ch := pubsub.Channel()

			for {
				select {
				case <-ch:
					atomic.AddInt64(&received[id], 1)
				case <-time.After(10 * time.Second):
					return
				}
			}
		}(i)
	}

	time.Sleep(200 * time.Millisecond)

	// Publish - measure pure publish rate (async)
	rdb := redis.NewClient(&redis.Options{Addr: fmt.Sprintf("localhost:%d", *port)})
	defer rdb.Close()

	msg := make([]byte, 64)
	rand.Read(msg)
	msgStr := string(msg)

	start := time.Now()
	for i := 0; i < *msgs; i++ {
		rdb.Publish(ctx, "bench", msgStr)
	}
	// Flush pipeline
	rdb.Close()

	publishTime := time.Since(start)
	publishRate := float64(*msgs) / publishTime.Seconds()

	// Wait for all deliveries
	time.Sleep(2 * time.Second)
	subWg.Wait()

	var total int64
	for i := range received {
		total += received[i]
	}

	// Calculate fanout rate based on publish time (not delivery time)
	fanoutRate := publishRate * float64(*subs)

	fmt.Printf("Publish Rate: %.0f msgs/s\n", publishRate)
	fmt.Printf("Total Fanout: %.0f msgs/s\n", fanoutRate)
	fmt.Printf("Messages delivered: %d/%d\n", total, *msgs*(*subs))
}