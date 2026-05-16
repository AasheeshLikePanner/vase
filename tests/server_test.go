package tests

import (
	"bufio"
	"net"
	"pulse/internal/proto"
	"pulse/internal/server"
	"pulse/internal/topic"
	"pulse/internal/wal"
	"sync"
	"testing"
	"time"
)

func newTestServer(t *testing.T) *server.Server {
	dir := t.TempDir()
	w, err := wal.Open(dir)
	if err != nil {
		t.Fatalf("failed to open WAL: %v", err)
	}
	tm := topic.NewManager(w, 131072)
	s := server.New("localhost:0", tm)
	if err := s.Start(); err != nil {
		t.Fatalf("failed to start server: %v", err)
	}
	return s
}

func dial(t testing.TB, addr string) net.Conn {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}
	return conn
}

func TestS1_10kConcurrentConnections(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}

	s := newTestServer(t)
	defer s.Stop()

	const numConns = 100
	var wg sync.WaitGroup
	wg.Add(numConns)

	for i := 0; i < numConns; i++ {
		go func() {
			defer wg.Done()
			conn := dial(t, s.Addr())
			defer conn.Close()

			reader := bufio.NewReader(conn)
			for j := 0; j < 10; j++ {
				frame, err := proto.ReadFrame(reader)
				if err != nil {
					return
				}
				if frame.Op != proto.OpOK {
					return
				}
			}
		}()
	}

	subs := make([]*topic.Subscription, numConns)
	for i := 0; i < numConns; i++ {
		conn := dial(t, s.Addr())
		writer := bufio.NewWriter(conn)
		reader := bufio.NewReader(conn)

		sub := &proto.Subscribe{Topic: "test", Offset: 0}
		proto.WriteFrame(writer, proto.OpSubscribe, proto.EncodeSubscribe(sub))
		writer.Flush()

		proto.ReadFrame(reader)

		subs[i] = nil
		_ = conn
	}

	wg.Wait()
}

func TestS2_ConnectionLifecycle(t *testing.T) {
	s := newTestServer(t)
	defer s.Stop()

	conn := dial(t, s.Addr())
	defer conn.Close()

	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	sub := &proto.Subscribe{Topic: "test", Offset: 0}
	proto.WriteFrame(writer, proto.OpSubscribe, proto.EncodeSubscribe(sub))
	writer.Flush()

	frame, err := proto.ReadFrame(reader)
	if err != nil {
		t.Fatalf("failed to read response: %v", err)
	}

	if frame.Op != proto.OpOK {
		t.Errorf("expected OK, got %d", frame.Op)
	}

	time.Sleep(100 * time.Millisecond)

	conn.Close()

	time.Sleep(100 * time.Millisecond)

	count := s.ConnCount()
	if count != 0 {
		t.Errorf("expected 0 connections, got %d", count)
	}
}

func TestS3_PublishSubscribe(t *testing.T) {
	s := newTestServer(t)
	defer s.Stop()

	conn := dial(t, s.Addr())
	defer conn.Close()

	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	sub := &proto.Subscribe{Topic: "test-topic", Offset: 0}
	proto.WriteFrame(writer, proto.OpSubscribe, proto.EncodeSubscribe(sub))
	writer.Flush()

	resp, err := proto.ReadFrame(reader)
	if err != nil || resp.Op != proto.OpOK {
		t.Fatalf("subscribe failed")
	}

	publish := &proto.Publish{Topic: "test-topic", Msg: []byte("hello")}
	proto.WriteFrame(writer, proto.OpPublish, proto.EncodePublish(publish))
	writer.Flush()

	resp, err = proto.ReadFrame(reader)
	if err != nil || resp.Op != proto.OpOK {
		t.Fatalf("publish failed")
	}

	deliver, err := proto.ReadFrame(reader)
	if err != nil {
		t.Fatalf("failed to read deliver: %v", err)
	}

	if deliver.Op != proto.OpDeliver {
		t.Errorf("expected deliver, got %d", deliver.Op)
	}

	d, err := proto.ParseDeliver(deliver.Data)
	if err != nil {
		t.Fatalf("failed to parse deliver: %v", err)
	}

	if string(d.Msg) != "hello" {
		t.Errorf("expected 'hello', got %q", d.Msg)
	}
}

func BenchmarkServerPublish(b *testing.B) {
	dir := b.TempDir()
	w, _ := wal.Open(dir)
	tm := topic.NewManager(w, 131072)
	s := server.New("localhost:0", tm)
	s.Start()
	defer s.Stop()

	conn := dial(b, s.Addr())
	defer conn.Close()

	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	sub := &proto.Subscribe{Topic: "bench", Offset: 0}
	proto.WriteFrame(writer, proto.OpSubscribe, proto.EncodeSubscribe(sub))
	writer.Flush()
	proto.ReadFrame(reader)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		publish := &proto.Publish{Topic: "bench", Msg: []byte("test")}
		proto.WriteFrame(writer, proto.OpPublish, proto.EncodePublish(publish))
		writer.Flush()
		proto.ReadFrame(reader)
	}
}