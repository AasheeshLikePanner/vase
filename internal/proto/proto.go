package proto

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
)

const (
	OpPublish   = 0x01
	OpSubscribe = 0x02
	OpAck       = 0x03
	OpDeliver   = 0x04
	OpOK        = 0x05
	OpErr       = 0x06

	MaxTopicLen   = 255
	MaxMsgSize    = 64 * 1024 * 1024
)

type Frame struct {
	Op   byte
	Data []byte
}

type Publish struct {
	Topic string
	Msg   []byte
}

type Subscribe struct {
	Topic  string
	Offset uint64
}

type Ack struct {
	Offset uint64
}

type Deliver struct {
	Offset uint64
	Msg    []byte
}

type OK struct{}

type Err struct {
	Code    byte
	Message string
}

func ReadFrame(br *bufio.Reader) (*Frame, error) {
	op, err := br.ReadByte()
	if err != nil {
		return nil, fmt.Errorf("failed to read op: %w", err)
	}

	var frameLen uint32
	if err := binary.Read(br, binary.BigEndian, &frameLen); err != nil {
		return nil, fmt.Errorf("failed to read frame length: %w", err)
	}

	if frameLen > 16*1024*1024 {
		return nil, fmt.Errorf("frame too large: %d", frameLen)
	}

	data := make([]byte, frameLen)
	if _, err := io.ReadFull(br, data); err != nil {
		return nil, fmt.Errorf("failed to read frame data: %w", err)
	}

	return &Frame{Op: op, Data: data}, nil
}

func WriteFrame(bw *bufio.Writer, op byte, data []byte) error {
	if err := bw.WriteByte(op); err != nil {
		return fmt.Errorf("failed to write op: %w", err)
	}

	frameLen := uint32(len(data))
	if err := binary.Write(bw, binary.BigEndian, frameLen); err != nil {
		return fmt.Errorf("failed to write frame length: %w", err)
	}

	if len(data) > 0 {
		if _, err := bw.Write(data); err != nil {
			return fmt.Errorf("failed to write frame data: %w", err)
		}
	}

	return nil
}

func ParsePublish(data []byte) (*Publish, error) {
	if len(data) < 1 {
		return nil, fmt.Errorf("publish: no topic length")
	}

	topicLen := int(data[0])
	if topicLen > MaxTopicLen {
		return nil, fmt.Errorf("publish: topic too long: %d", topicLen)
	}

	if len(data) < 1+topicLen+4 {
		return nil, fmt.Errorf("publish: data too short")
	}

	topic := string(data[1 : 1+topicLen])
	idx := 1 + topicLen

	msgLen := binary.BigEndian.Uint32(data[idx : idx+4])
	if msgLen > MaxMsgSize {
		return nil, fmt.Errorf("publish: message too large: %d", msgLen)
	}

	idx += 4
	if len(data) < idx+int(msgLen) {
		return nil, fmt.Errorf("publish: message data truncated")
	}

	msg := data[idx : idx+int(msgLen)]

	return &Publish{Topic: topic, Msg: msg}, nil
}

func ParseSubscribe(data []byte) (*Subscribe, error) {
	if len(data) < 1 {
		return nil, fmt.Errorf("subscribe: no topic length")
	}

	topicLen := int(data[0])
	if topicLen > MaxTopicLen {
		return nil, fmt.Errorf("subscribe: topic too long: %d", topicLen)
	}

	if len(data) < 1+topicLen+8 {
		return nil, fmt.Errorf("subscribe: data too short")
	}

	topic := string(data[1 : 1+topicLen])
	idx := 1 + topicLen

	offset := binary.BigEndian.Uint64(data[idx : idx+8])

	return &Subscribe{Topic: topic, Offset: offset}, nil
}

func ParseAck(data []byte) (*Ack, error) {
	if len(data) < 8 {
		return nil, fmt.Errorf("ack: data too short")
	}

	offset := binary.BigEndian.Uint64(data[:8])
	return &Ack{Offset: offset}, nil
}

func ParseDeliver(data []byte) (*Deliver, error) {
	if len(data) < 12 {
		return nil, fmt.Errorf("deliver: data too short")
	}

	offset := binary.BigEndian.Uint64(data[:8])
	msgLen := binary.BigEndian.Uint32(data[8:12])

	if msgLen > MaxMsgSize {
		return nil, fmt.Errorf("deliver: message too large: %d", msgLen)
	}

	if len(data) < 12+int(msgLen) {
		return nil, fmt.Errorf("deliver: message data truncated")
	}

	msg := data[12 : 12+int(msgLen)]

	return &Deliver{Offset: offset, Msg: msg}, nil
}

func ParseErr(data []byte) (*Err, error) {
	if len(data) < 3 {
		return nil, fmt.Errorf("err: data too short")
	}

	code := data[0]
	msgLen := binary.BigEndian.Uint16(data[1:3])

	if len(data) < 3+int(msgLen) {
		return nil, fmt.Errorf("err: message truncated")
	}

	msg := string(data[3 : 3+msgLen])

	return &Err{Code: code, Message: msg}, nil
}

func EncodePublish(p *Publish) []byte {
	data := make([]byte, 0, 1+len(p.Topic)+4+len(p.Msg))
	data = append(data, byte(len(p.Topic)))
	data = append(data, p.Topic...)

	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], uint32(len(p.Msg)))
	data = append(data, buf[:]...)
	data = append(data, p.Msg...)

	return data
}

func EncodeSubscribe(s *Subscribe) []byte {
	data := make([]byte, 0, 1+len(s.Topic)+8)
	data = append(data, byte(len(s.Topic)))
	data = append(data, s.Topic...)

	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], s.Offset)
	data = append(data, buf[:]...)

	return data
}

func EncodeAck(a *Ack) []byte {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], a.Offset)
	return buf[:]
}

func EncodeDeliver(d *Deliver) []byte {
	data := make([]byte, 0, 8+4+len(d.Msg))

	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], d.Offset)
	data = append(data, buf[:]...)

	binary.BigEndian.PutUint32(buf[:4], uint32(len(d.Msg)))
	data = append(data, buf[:4]...)

	data = append(data, d.Msg...)

	return data
}

func EncodeOK() []byte {
	return nil
}

func EncodeErr(e *Err) []byte {
	data := make([]byte, 0, 3+len(e.Message))
	data = append(data, e.Code)

	var buf [2]byte
	binary.BigEndian.PutUint16(buf[:], uint16(len(e.Message)))
	data = append(data, buf[:]...)

	data = append(data, e.Message...)

	return data
}