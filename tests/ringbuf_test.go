package tests

import (
	"pulse/internal/ringbuf"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestRB1_SequentialCorrectness(t *testing.T) {
	buf := ringbuf.New(1024)
	n := 1000

	for i := 0; i < n; i++ {
		data := []byte{byte(i)}
		buf.Write(data)
	}

	reader := buf.NewReader()
	count := 0
	for {
		msg := reader.Read()
		if msg.Data == nil {
			if count == n {
				break
			}
			runtime.Gosched()
			continue
		}
		if len(msg.Data) != 1 || msg.Data[0] != byte(count) {
			t.Errorf("message %d: got %v, want [%d]", count, msg.Data, count)
		}
		count++
		if count >= n {
			break
		}
	}

	if count != n {
		t.Errorf("expected %d messages, got %d", n, count)
	}
}

func TestRB2_ConcurrentReadersOneWriter(t *testing.T) {
	buf := ringbuf.New(131072)
	totalMessages := 100000
	numReaders := 10

	var writerDone atomic.Bool
	writerDone.Store(false)

	var wg sync.WaitGroup
	wg.Add(numReaders + 1)

	go func() {
		defer wg.Done()
		for i := 0; i < totalMessages; i++ {
			data := make([]byte, 4)
			data[0] = byte(i)
			data[1] = byte(i >> 8)
			data[2] = byte(i >> 16)
			data[3] = byte(i >> 24)
			buf.Write(data)
		}
		writerDone.Store(true)
	}()

	results := make([][]int, numReaders)
	var mu sync.Mutex

	for i := 0; i < numReaders; i++ {
		readerIdx := i
		go func() {
			defer wg.Done()
			reader := buf.NewReader()
			var received []int

			for {
				msg := reader.Read()
				if msg.Data == nil {
					if writerDone.Load() && reader.Cursor() >= uint64(totalMessages) {
						break
					}
					runtime.Gosched()
					continue
				}
				seq := int(msg.Data[0]) | int(msg.Data[1])<<8 | int(msg.Data[2])<<16 | int(msg.Data[3])<<24
				received = append(received, seq)
			}

			mu.Lock()
			results[readerIdx] = received
			mu.Unlock()
		}()
	}

	wg.Wait()

	for i, received := range results {
		if len(received) != totalMessages {
			t.Errorf("reader %d: expected %d messages, got %d", i, totalMessages, len(received))
			continue
		}

		for j, seq := range received {
			if seq != j {
				t.Errorf("reader %d: message %d: expected %d, got %d", i, j, j, seq)
				break
			}
		}
	}
}

func TestRB3_ReaderCursorIndependence(t *testing.T) {
	buf := ringbuf.New(131072)

	totalMessages := 50000

	var writerDone atomic.Bool
	writerDone.Store(false)

	go func() {
		for i := 0; i < totalMessages; i++ {
			data := []byte{byte(i)}
			buf.Write(data)
		}
		writerDone.Store(true)
	}()

	fastReader := buf.NewReader()
	slowReader := buf.NewReader()

	var fastThroughput, slowThroughput int64
	var wg sync.WaitGroup
	wg.Add(2)

	start := time.Now()

	go func() {
		defer wg.Done()
		count := 0
		for {
			msg := fastReader.Read()
			if msg.Data == nil {
				if writerDone.Load() && count >= totalMessages {
					break
				}
				runtime.Gosched()
				continue
			}
			count++
			if count >= totalMessages {
				break
			}
		}
		elapsed := time.Since(start).Nanoseconds()
		fastThroughput = int64(totalMessages) * 1e9 / elapsed
	}()

	go func() {
		defer wg.Done()
		count := 0
		for {
			msg := slowReader.Read()
			if msg.Data == nil {
				if writerDone.Load() && count >= totalMessages {
					break
				}
				runtime.Gosched()
				continue
			}
			count++

			if count%100 == 0 {
				time.Sleep(10 * time.Microsecond)
			}

			if count >= totalMessages {
				break
			}
		}
		elapsed := time.Since(start).Nanoseconds()
		slowThroughput = int64(totalMessages) * 1e9 / elapsed
	}()

	wg.Wait()

	t.Logf("Fast reader throughput: %d msgs/s", fastThroughput)
	t.Logf("Slow reader throughput: %d msgs/s", slowThroughput)
}

func TestRB4_BufferFullBehavior(t *testing.T) {
	buf := ringbuf.New(16)

	for i := 0; i < 16; i++ {
		buf.Write([]byte{byte(i)})
	}

	head := buf.Head()
	if head != 16 {
		t.Errorf("expected head to be 16 after 16 writes, got %d", head)
	}

	for i := 0; i < 20; i++ {
		buf.Write([]byte{byte(i)})
	}

	t.Logf("Head after overflow writes: %d", buf.Head())
}

func BenchmarkWrite(b *testing.B) {
	buf := ringbuf.New(8192)
	data := []byte("test message")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf.Write(data)
	}
}

func BenchmarkReadSingle(b *testing.B) {
	buf := ringbuf.New(8192)
	for i := 0; i < b.N; i++ {
		buf.Write([]byte("test"))
	}

	reader := buf.NewReader()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for {
			msg := reader.Read()
			if msg.Data != nil {
				break
			}
		}
	}
}

func BenchmarkFanout(b *testing.B) {
	buf := ringbuf.New(131072)
	subscriberCount := b.N / 1000
	if subscriberCount < 1 {
		subscriberCount = 1
	}

	readers := make([]*ringbuf.Reader, subscriberCount)
	for i := range readers {
		readers[i] = buf.NewReader()
	}

	totalMessages := 10000
	for i := 0; i < totalMessages; i++ {
		buf.Write([]byte("test"))
	}

	b.ResetTimer()

	var wg sync.WaitGroup
	wg.Add(len(readers))

	for _, reader := range readers {
		r := reader
		go func() {
			defer wg.Done()
			count := 0
			for count < totalMessages {
				msg := r.Read()
				if msg.Data != nil {
					count++
				}
			}
		}()
	}

	wg.Wait()
}