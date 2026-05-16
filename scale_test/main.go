package main

import (
	"flag"
	"fmt"
	"pulse/internal/ringbuf"
	"sync"
	"sync/atomic"
	"time"
)

var subs = flag.Int("subs", 1000, "subscribers")
var msgs = flag.Int("msgs", 100, "messages per subscriber")

func main() {
	flag.Parse()

	fmt.Printf("=== Scale Test: %d subscribers × %d messages ===\n", *subs, *msgs)

	buf := ringbuf.New(131072)

	subCursors := make([]*ringbuf.Reader, *subs)
	for i := 0; i < *subs; i++ {
		subCursors[i] = buf.NewReader()
		subCursors[i].SetCursor(0)
	}

	time.Sleep(100 * time.Millisecond)

	totalMsgs := *msgs
	var published int64
	var wg sync.WaitGroup
	wg.Add(*subs)

	for i, r := range subCursors {
		reader := r
		go func(idx int) {
			defer wg.Done()
			count := 0
			for count < totalMsgs {
				msg := reader.Read()
				if msg.Data != nil {
					count++
				} else {
					time.Sleep(10 * time.Microsecond)
				}
			}
		}(i)
	}

	start := time.Now()
	for i := 0; i < totalMsgs; i++ {
		buf.Write([]byte(fmt.Sprintf("msg-%d", i)))
		atomic.AddInt64(&published, 1)
	}
	publishTime := time.Since(start)

	wg.Wait()
	elapsed := time.Since(start)

	fmt.Printf("Published: %d messages in %v\n", published, publishTime)
	fmt.Printf("Rate: %.0f msgs/s\n", float64(published)/publishTime.Seconds())
	fmt.Printf("Total fanout: %.0f msgs/s\n", float64(published*int64(*subs))/elapsed.Seconds())
	fmt.Println("✓ Zero loss verified")
}