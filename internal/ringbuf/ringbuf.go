package ringbuf

import (
	"runtime"
	"sync/atomic"
)

type Buffer struct {
	capacity uint64
	mask     uint64
	head     atomic.Uint64
	slots    []slot
}

type slot struct {
	seq   atomic.Uint64
	data  []byte
}

func New(capacity uint64) *Buffer {
	if capacity&(capacity-1) != 0 {
		panic("capacity must be power of 2")
	}

	slots := make([]slot, capacity)
	for i := range slots {
		slots[i].seq.Store(0)
	}

	return &Buffer{
		capacity: capacity,
		mask:     capacity - 1,
		slots:    slots,
	}
}

func (b *Buffer) Capacity() uint64 {
	return b.capacity
}

func (b *Buffer) Head() uint64 {
	return b.head.Load()
}

func (b *Buffer) Write(data []byte) uint64 {
	slot := b.head.Add(1) - 1
	idx := slot & b.mask

	b.slots[idx].data = data
	b.slots[idx].seq.Store(slot)

	return slot
}

func (b *Buffer) Read(slot uint64) []byte {
	idx := slot & b.mask
	s := &b.slots[idx]

	if s.seq.Load() != slot {
		return nil
	}

	return s.data
}

func (b *Buffer) WriteMessage(msg Message) uint64 {
	return b.Write(msg.Data)
}

func (b *Buffer) ReadMessage(slot uint64) Message {
	data := b.Read(slot)
	if data == nil {
		return Message{}
	}
	return Message{Data: data, Slot: slot}
}

type Message struct {
	Data []byte
	Slot uint64
}

type Reader struct {
	buf    *Buffer
	cursor atomic.Uint64
}

func (b *Buffer) NewReader() *Reader {
	return &Reader{
		buf:    b,
		cursor: atomic.Uint64{},
	}
}

func (r *Reader) Cursor() uint64 {
	return r.cursor.Load()
}

func (r *Reader) SetCursor(cursor uint64) {
	r.cursor.Store(cursor)
}

func (r *Reader) Read() Message {
	for {
		cursor := r.cursor.Load()
		head := r.buf.Head()

		if cursor >= head {
			return Message{}
		}

		msg := r.buf.ReadMessage(cursor)
		if msg.Data == nil {
			runtime.Gosched()
			continue
		}

		r.cursor.Store(cursor + 1)
		return msg
	}
}

func (r *Reader) ReadBatch(max int) []Message {
	var msgs []Message
	cursor := r.cursor.Load()
	head := r.buf.Head()

	for cursor < head && len(msgs) < max {
		msg := r.buf.ReadMessage(cursor)
		if msg.Data == nil {
			runtime.Gosched()
			break
		}
		msgs = append(msgs, msg)
		cursor++
	}

	if len(msgs) > 0 {
		r.cursor.Store(cursor)
	}

	return msgs
}