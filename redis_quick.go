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

	fmt.Println("=== Redis Publish (no subs) ===")
	msg := make([]byte, 64)
	rand.Read(msg)
	msgStr := string(msg)

	start := time.Now()
	for i := 0; i < 10000; i++ {
		client.Publish(ctx, "bench", msgStr)
	}
	elapsed := time.Since(start)
	fmt.Printf("Publish Rate: %.0f msgs/s\n", float64(10000)/elapsed.Seconds())
}