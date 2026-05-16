package topic

import (
	"pulse/internal/ringbuf"
	"pulse/internal/wal"
	"sync"
	"sync/atomic"
	"time"
)

type Manager struct {
	topics    map[string]*Topic
	wal       *wal.Log
	ringCap   uint64
	mu        sync.RWMutex
	nextSubID atomic.Uint64
}

func NewManager(w *wal.Log, ringCapacity uint64) *Manager {
	return &Manager{
		topics:  make(map[string]*Topic),
		wal:     w,
		ringCap: ringCapacity,
	}
}

func (m *Manager) GetOrCreateTopic(name string) *Topic {
	m.mu.Lock()
	defer m.mu.Unlock()

	if t, ok := m.topics[name]; ok {
		return t
	}

	t := &Topic{
		name:       name,
		buf:        ringbuf.New(m.ringCap),
		wal:        m.wal,
		subs:       make([]*Subscription, 0, 16),
		msgCount:   atomic.Uint64{},
		nextSubID:  atomic.Uint64{},
	}

	m.topics[name] = t
	return t
}

func (m *Manager) Publish(topicName string, data []byte) (uint64, error) {
	t := m.GetOrCreateTopic(topicName)
	return t.Publish(data)
}

func (m *Manager) Subscribe(topicName string, offset uint64, sendChan chan []byte) *Subscription {
	t := m.GetOrCreateTopic(topicName)
	return t.Subscribe(offset, sendChan)
}

func (m *Manager) Unsubscribe(sub *Subscription) {
	if sub.topic != nil {
		sub.topic.Unsubscribe(sub)
	}
}

type Topic struct {
	name       string
	buf        *ringbuf.Buffer
	wal        *wal.Log
	subs       []*Subscription
	subMu      sync.RWMutex
	msgCount   atomic.Uint64
	nextSubID  atomic.Uint64
}

func (t *Topic) Publish(data []byte) (uint64, error) {
	offset, err := t.wal.Write(data)
	if err != nil {
		return 0, err
	}

	t.buf.Write(data)
	t.msgCount.Add(1)

	return offset, nil
}

func (t *Topic) Subscribe(startOffset uint64, sendChan chan []byte) *Subscription {
	sub := &Subscription{
		id:     t.nextSubID.Add(1),
		topic:  t,
		cursor: atomic.Uint64{},
		send:   sendChan,
		closed: atomic.Bool{},
	}

	if startOffset == 0 {
		sub.cursor.Store(t.buf.Head())
	} else {
		sub.cursor.Store(startOffset)
	}

	t.subMu.Lock()
	t.subs = append(t.subs, sub)
	t.subMu.Unlock()

	go sub.run()

	return sub
}

func (t *Topic) Unsubscribe(sub *Subscription) {
	t.subMu.Lock()
	defer t.subMu.Unlock()

	for i, s := range t.subs {
		if s == sub {
			t.subs = append(t.subs[:i], t.subs[i+1:]...)
			break
		}
	}
}

func (t *Topic) SubscriberCount() int {
	t.subMu.RLock()
	defer t.subMu.RUnlock()
	return len(t.subs)
}

type Subscription struct {
	seqPad0 [64]byte
	cursor  atomic.Uint64
	seqPad1 [64]byte

	id     uint64
	topic  *Topic
	send   chan []byte
	closed atomic.Bool
}

func (s *Subscription) run() {
	for {
		if s.closed.Load() {
			break
		}

		cursor := s.cursor.Load()
		head := s.topic.buf.Head()

		if cursor >= head {
			time.Sleep(10 * time.Microsecond)
			continue
		}

		batch := 0
		for cursor < head && batch < 256 {
			if s.closed.Load() {
				return
			}

			msg := s.topic.buf.Read(cursor)
			if msg == nil {
				break
			}

			select {
			case s.send <- msg:
			case <-time.After(1 * time.Millisecond):
				goto store
			}

			cursor++
			batch++
		}

	store:
		s.cursor.Store(cursor)
		if batch == 0 {
			time.Sleep(5 * time.Microsecond)
		}
	}
}

func (s *Subscription) Close() {
	s.closed.Store(true)
}