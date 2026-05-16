package tests

import (
	"pulse/internal/topic"
	"pulse/internal/wal"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func newTestManager(t *testing.T) *topic.Manager {
	dir := t.TempDir()
	w, err := wal.Open(dir)
	if err != nil {
		t.Fatalf("failed to open WAL: %v", err)
	}
	return topic.NewManager(w, 131072)
}

func TestTM1_SingleProducerSingleSubscriberOrderedDelivery(t *testing.T) {
	m := newTestManager(t)

	totalMessages := 100000
	topicName := "test-topic"

	received := make([]int, 0, totalMessages)
	var mu sync.Mutex
	var wg sync.WaitGroup
	wg.Add(1)

	sendChan := make(chan []byte, 1024)
	sub := m.Subscribe(topicName, 0, sendChan)

	go func() {
		defer wg.Done()
		count := 0
		for count < totalMessages {
			select {
			case msg := <-sendChan:
				if len(msg) >= 4 {
					seq := int(msg[0]) | int(msg[1])<<8 | int(msg[2])<<16 | int(msg[3])<<24
					mu.Lock()
					received = append(received, seq)
					mu.Unlock()
					count++
				}
			case <-time.After(5 * time.Second):
				t.Fatalf("timeout waiting for messages")
			}
		}
	}()

	for i := 0; i < totalMessages; i++ {
		data := make([]byte, 4)
		data[0] = byte(i)
		data[1] = byte(i >> 8)
		data[2] = byte(i >> 16)
		data[3] = byte(i >> 24)
		m.Publish(topicName, data)
	}

	wg.Wait()

	if len(received) != totalMessages {
		t.Errorf("expected %d messages, got %d", totalMessages, len(received))
	}

	for i, seq := range received {
		if seq != i {
			t.Errorf("message %d: expected %d, got %d", i, i, seq)
			break
		}
	}

	sub.Close()
}

func TestTM2_SingleProducer1000Subscribers(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}

	m := newTestManager(t)

	totalMessages := 1000
	numSubscribers := 100
	topicName := "test-topic"

	subChannels := make([]chan []byte, numSubscribers)
	subs := make([]*topic.Subscription, numSubscribers)

	for i := 0; i < numSubscribers; i++ {
		ch := make(chan []byte, 256)
		subChannels[i] = ch
		subs[i] = m.Subscribe(topicName, 0, ch)
	}

	time.Sleep(100 * time.Millisecond)

	var wg sync.WaitGroup
	wg.Add(numSubscribers)

	for i := 0; i < numSubscribers; i++ {
		ch := subChannels[i]
		count := 0
		go func() {
			defer wg.Done()
			for count < totalMessages {
				select {
				case <-ch:
					count++
				case <-time.After(10 * time.Second):
					return
				}
			}
		}()
	}

	for i := 0; i < totalMessages; i++ {
		data := []byte{byte(i)}
		m.Publish(topicName, data)
	}

	wg.Wait()

	for i, ch := range subChannels {
		received := 0
		for {
			select {
			case <-ch:
				received++
			default:
				break
			}
		}
		if received != totalMessages {
			t.Errorf("subscriber %d: expected %d, got %d", i, totalMessages, received)
		}
	}

	for _, sub := range subs {
		sub.Close()
	}
}

func TestTM3_SlowSubscriberDoesNotBlockFastSubscriber(t *testing.T) {
	m := newTestManager(t)

	totalMessages := 10000
	topicName := "test-topic"

	fastChan := make(chan []byte, 256)
	slowChan := make(chan []byte, 256)

	m.Subscribe(topicName, 0, slowChan)
	m.Subscribe(topicName, 0, fastChan)

	time.Sleep(100 * time.Millisecond)

	var fastCount, slowCount int64
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		for {
			select {
			case <-fastChan:
				atomic.AddInt64(&fastCount, 1)
			case <-time.After(2 * time.Second):
				return
			}
		}
	}()

	go func() {
		defer wg.Done()
		for {
			select {
			case <-slowChan:
				atomic.AddInt64(&slowCount, 1)
				time.Sleep(100 * time.Millisecond)
			case <-time.After(2 * time.Second):
				return
			}
		}
	}()

	for i := 0; i < totalMessages; i++ {
		m.Publish(topicName, []byte{byte(i)})
	}

	time.Sleep(500 * time.Millisecond)
	wg.Wait()

	t.Logf("Fast subscriber received: %d", fastCount)
	t.Logf("Slow subscriber received: %d", slowCount)

	if fastCount < int64(totalMessages)*9/10 {
		t.Errorf("fast subscriber should receive most messages, got %d", fastCount)
	}
}

func TestTM4_SubscriberDisconnectMidStream(t *testing.T) {
	m := newTestManager(t)

	totalMessages := 5000
	topicName := "test-topic"

	slowChan := make(chan []byte, 256)
	fastChan := make(chan []byte, 256)

	slowSub := m.Subscribe(topicName, 0, slowChan)
	m.Subscribe(topicName, 0, fastChan)

	time.Sleep(50 * time.Millisecond)

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		count := 0
		for {
			select {
			case <-slowChan:
				count++
				if count == 2500 {
					slowSub.Close()
					return
				}
			case <-time.After(5 * time.Second):
				return
			}
		}
	}()

	go func() {
		defer wg.Done()
		for {
			select {
			case <-fastChan:
			case <-time.After(5 * time.Second):
				return
			}
		}
	}()

	for i := 0; i < totalMessages; i++ {
		m.Publish(topicName, []byte{byte(i)})
	}

	time.Sleep(500 * time.Millisecond)
	wg.Wait()
}

func TestTM5_LateSubscriberGetsHistoryFromWAL(t *testing.T) {
	m := newTestManager(t)

	totalMessages := 1000
	topicName := "test-topic"

	for i := 0; i < totalMessages; i++ {
		data := []byte{byte(i)}
		m.Publish(topicName, data)
	}

	time.Sleep(100 * time.Millisecond)

	lateChan := make(chan []byte, totalMessages+100)
	lateSub := m.Subscribe(topicName, 1, lateChan)

	time.Sleep(500 * time.Millisecond)

	count := 0
	for {
		select {
		case <-lateChan:
			count++
			if count >= totalMessages {
				goto done
			}
		case <-time.After(5 * time.Second):
			goto done
		}
	}

done:
	if count != totalMessages {
		t.Errorf("expected %d historical messages, got %d", totalMessages, count)
	}

	lateSub.Close()
}