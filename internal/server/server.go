package server

import (
	"bufio"
	"net"
	"pulse/internal/proto"
	"pulse/internal/topic"
	"sync"
	"sync/atomic"
	"time"
)

type Server struct {
	addr    string
	tm      *topic.Manager
	ln      net.Listener
	wg      sync.WaitGroup
	running atomic.Bool
	conns   atomic.Int64
}

func New(addr string, tm *topic.Manager) *Server {
	return &Server{
		addr: addr,
		tm:   tm,
	}
}

func (s *Server) Start() error {
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}

	s.ln = ln
	s.running.Store(true)

	s.wg.Add(1)
	go s.acceptLoop()

	return nil
}

func (s *Server) acceptLoop() {
	defer s.wg.Done()

	for s.running.Load() {
		conn, err := s.ln.Accept()
		if err != nil {
			if s.running.Load() {
				continue
			}
			break
		}

		s.conns.Add(1)
		s.wg.Add(1)
		go s.handleConn(conn)
	}
}

func (s *Server) handleConn(conn net.Conn) {
	defer s.wg.Done()
	defer s.conns.Add(-1)

	reader := bufio.NewReaderSize(conn, 64*1024)
	writer := bufio.NewWriterSize(conn, 64*1024)

	defer conn.Close()

	for {
		frame, err := proto.ReadFrame(reader)
		if err != nil {
			return
		}

		switch frame.Op {
		case proto.OpPublish:
			s.handlePublish(writer, frame.Data)
		case proto.OpSubscribe:
			s.handleSubscribe(conn, writer, frame.Data)
		case proto.OpAck:
		default:
			return
		}

		writer.Flush()
	}
}

func (s *Server) handlePublish(writer *bufio.Writer, data []byte) {
	p, err := proto.ParsePublish(data)
	if err != nil {
		s.writeErr(writer, 1, err.Error())
		return
	}

	offset, err := s.tm.Publish(p.Topic, p.Msg)
	if err != nil {
		s.writeErr(writer, 2, err.Error())
		return
	}

	_ = offset
	resp := proto.EncodeOK()
	if err := proto.WriteFrame(writer, proto.OpOK, resp); err != nil {
		return
	}
}

func (s *Server) handleSubscribe(conn net.Conn, writer *bufio.Writer, data []byte) {
	sub, err := proto.ParseSubscribe(data)
	if err != nil {
		s.writeErr(writer, 1, err.Error())
		return
	}

	sendChan := make(chan []byte, 256)
	topicSub := s.tm.Subscribe(sub.Topic, sub.Offset, sendChan)

	go s.deliverLoop(conn, sendChan, topicSub)

	resp := proto.EncodeOK()
	if err := proto.WriteFrame(writer, proto.OpOK, resp); err != nil {
		return
	}
}

func (s *Server) deliverLoop(conn net.Conn, sendChan chan []byte, topicSub *topic.Subscription) {
	writer := bufio.NewWriterSize(conn, 64*1024)

	defer func() {
		topicSub.Close()
		conn.Close()
	}()

	batch := make([][]byte, 0, 64)
	flush := func() {
		if len(batch) == 0 {
			return
		}
		for _, msg := range batch {
			deliver := &proto.Deliver{Offset: 0, Msg: msg}
			if err := proto.WriteFrame(writer, proto.OpDeliver, proto.EncodeDeliver(deliver)); err != nil {
				return
			}
		}
		writer.Flush()
		batch = batch[:0]
	}

	flushTick := time.NewTicker(5 * time.Millisecond)
	defer flushTick.Stop()

	for {
		select {
		case msg := <-sendChan:
			batch = append(batch, msg)
			if len(batch) >= 64 {
				flush()
			}
		case <-flushTick.C:
			flush()
		case <-time.After(1 * time.Second):
			flush()
			return
		}
	}
}

func (s *Server) writeErr(writer *bufio.Writer, code byte, msg string) {
	err := &proto.Err{Code: code, Message: msg}
	proto.WriteFrame(writer, proto.OpErr, proto.EncodeErr(err))
	writer.Flush()
}

func (s *Server) Stop() error {
	s.running.Store(false)

	if s.ln != nil {
		s.ln.Close()
	}

	s.wg.Wait()
	return nil
}

func (s *Server) ConnCount() int64 {
	return s.conns.Load()
}

func (s *Server) Addr() string {
	if s.ln != nil {
		return s.ln.Addr().String()
	}
	return s.addr
}