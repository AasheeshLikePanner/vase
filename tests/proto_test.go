package tests

import (
	"bufio"
	"bytes"
	"math/rand"
	"pulse/internal/proto"
	"testing"
)

func TestP1_RoundTripEncodeDecode(t *testing.T) {
	for i := 0; i < 1000; i++ {
		topic := randomString(rand.Intn(64))
		msg := randomBytes(rand.Intn(1024))

		publish := &proto.Publish{Topic: topic, Msg: msg}
		encoded := proto.EncodePublish(publish)

		decoded, err := proto.ParsePublish(encoded)
		if err != nil {
			t.Fatalf("failed to decode publish: %v", err)
		}

		if decoded.Topic != topic {
			t.Errorf("topic mismatch: got %q, want %q", decoded.Topic, topic)
		}

		if !bytes.Equal(decoded.Msg, msg) {
			t.Errorf("msg mismatch")
		}
	}

	for i := 0; i < 1000; i++ {
		topic := randomString(rand.Intn(64))
		offset := uint64(rand.Intn(1000000))

		sub := &proto.Subscribe{Topic: topic, Offset: offset}
		encoded := proto.EncodeSubscribe(sub)

		decoded, err := proto.ParseSubscribe(encoded)
		if err != nil {
			t.Fatalf("failed to decode subscribe: %v", err)
		}

		if decoded.Topic != topic {
			t.Errorf("topic mismatch: got %q, want %q", decoded.Topic, topic)
		}

		if decoded.Offset != offset {
			t.Errorf("offset mismatch: got %d, want %d", decoded.Offset, offset)
		}
	}
}

func TestP2_MalformedFrameHandling(t *testing.T) {
	tests := []struct {
		name string
		data []byte
	}{
		{"truncated frame len", []byte{proto.OpPublish}},
		{"topic too long", append([]byte{proto.OpPublish, 255}, bytes.Repeat([]byte{'a'}, 300)...)},
		{"invalid op", []byte{0xFF}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			br := bytes.NewReader(tt.data)
			_, err := proto.ReadFrame(bufio.NewReader(br))
			if err == nil {
				t.Error("expected error, got nil")
			}
		})
	}
}

func TestP3_MaxSizeEnforcement(t *testing.T) {
	tooLargeMsg := make([]byte, proto.MaxMsgSize+1)
	publish := &proto.Publish{
		Topic: "test",
		Msg:   tooLargeMsg,
	}

	encoded := proto.EncodePublish(publish)
	_, err := proto.ParsePublish(encoded)

	if err == nil {
		t.Error("expected error for oversized message, got nil")
	}
}

func randomString(n int) string {
	const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

func randomBytes(n int) []byte {
	b := make([]byte, n)
	rand.Read(b)
	return b
}