package main

import (
	"context"
	"fmt"
	"math/rand"
	"time"
	"github.com/redis/go-redis/v9"
)

func main() {
	ctx := context.Background()
	client := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	defer client.Close()

	// Test 1: Just publish rate (no subscriber)
	fmt.Println("=== Test 1: Pure Publish Rate (no subs) ===")
	msg := make([]byte, 64)
	rand.Read(msg)
	
	start := time.Now()
	for i := 0; i < 10000; i++ {
		client.Publish(ctx, "test", string(msg))
	}
	elapsed := time.Since(start)
	fmt.Printf("10000 publishes in %v = %.0f/s\n", elapsed, float64(10000)/elapsed.Seconds())

	// Test 2: With subscriber
	fmt.Println("\n=== Test 2: With Subscriber ===")
	sub := client.Subscribe(ctx, "test2")
	ch := sub.Channel()
	
	go func() {
		for {
			select {
			case <-ch:
			case <-time.After(5 * time.Second):
				return
			}
		}
	}()
	time.Sleep(100*time.Millisecond)
	
	msg2 := make([]byte, 64)
	rand.Read(msg2)
	
	start = time.Now()
	for i := 0; i < 10000; i++ {
		client.Publish(ctx, "test2", string(msg2))
	}
	elapsed = time.Since(start)
	fmt.Printf("10000 publishes in %v = %.0f/s\n", elapsed, float64(10000)/elapsed.Seconds())
	
	// Wait a bit for delivery
	time.Sleep(500*time.Millisecond)
}