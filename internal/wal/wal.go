package wal

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"unsafe"
)

const (
	SegmentSize    = 128 * 1024 * 1024
	HeaderSize     = 24
	EntryHeaderSize = 12
	Magic          = 0x50554C53
)

type Log struct {
	dir         string
	currentFile *os.File
	currentMmap []byte
	segmentID   atomic.Uint64
	writePos    atomic.Uint64
	syncEnabled bool
	mu          sync.Mutex
}

type Entry struct {
	Offset uint64
	Data   []byte
}

type segmentHeader struct {
	Magic      uint32
	SegmentID  uint64
	CreatedAt  uint64
	Checksum   uint32
}

func Open(dir string) (*Log, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	log := &Log{
		dir:         dir,
		syncEnabled: false,
	}

	if err := log.recover(); err != nil {
		return nil, err
	}

	return log, nil
}

func (l *Log) recover() error {
	files, err := filepath.Glob(filepath.Join(l.dir, "segment_*.log"))
	if err != nil {
		return err
	}

	if len(files) == 0 {
		return l.createSegment(0)
	}

	var maxID uint64
	for _, f := range files {
		var id uint64
		if _, err := fmt.Sscanf(filepath.Base(f), "segment_%d.log", &id); err != nil {
			continue
		}
		if id > maxID {
			maxID = id
		}
	}

	if err := l.openSegment(maxID); err != nil {
		return err
	}

	return nil
}

func (l *Log) createSegment(id uint64) error {
	path := filepath.Join(l.dir, fmt.Sprintf("segment_%05d.log", id))

	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return err
	}

	if err := f.Truncate(SegmentSize); err != nil {
		f.Close()
		return err
	}

	mmap, err := mmap(f, SegmentSize)
	if err != nil {
		f.Close()
		return err
	}

	stat, err := f.Stat()
	if err != nil {
		f.Close()
		munmap(mmap)
		return err
	}

	header := (*segmentHeader)(unsafe.Pointer(&mmap[0]))
	header.Magic = Magic
	header.SegmentID = id
	header.CreatedAt = uint64(stat.ModTime().Unix())
	header.Checksum = 0

	if l.currentFile != nil {
		l.currentFile.Close()
	}
	if l.currentMmap != nil {
		munmap(l.currentMmap)
	}

	l.currentFile = f
	l.currentMmap = mmap
	l.segmentID.Store(id)
	l.writePos.Store(HeaderSize)

	return nil
}

func (l *Log) openSegment(id uint64) error {
	path := filepath.Join(l.dir, fmt.Sprintf("segment_%05d.log", id))

	f, err := os.OpenFile(path, os.O_RDWR, 0644)
	if err != nil {
		return err
	}

	mmap, err := mmap(f, SegmentSize)
	if err != nil {
		f.Close()
		return err
	}

	header := (*segmentHeader)(unsafe.Pointer(&mmap[0]))
	if header.Magic != Magic {
		f.Close()
		munmap(mmap)
		return l.createSegment(id + 1)
	}

	if l.currentFile != nil {
		l.currentFile.Close()
	}
	if l.currentMmap != nil {
		munmap(l.currentMmap)
	}

	l.currentFile = f
	l.currentMmap = mmap
	l.segmentID.Store(id)
	l.writePos.Store(HeaderSize)

	return nil
}

func (l *Log) Write(data []byte) (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	entrySize := EntryHeaderSize + uint64(len(data))
	pos := l.writePos.Load()

	if pos+entrySize > SegmentSize {
		newID := l.segmentID.Load() + 1
		if err := l.createSegment(newID); err != nil {
			return 0, err
		}
		pos = HeaderSize
	}

	offset := pos
	idx := pos
	l.currentMmap[idx] = 0x01
	idx++

	binary.BigEndian.PutUint32(l.currentMmap[idx:idx+4], uint32(len(data)))
	idx += 4

	binary.BigEndian.PutUint64(l.currentMmap[idx:idx+8], offset)
	idx += 8

	copy(l.currentMmap[idx:idx+uint64(len(data))], data)
	idx += uint64(len(data))

	checksum := crc32.ChecksumIEEE(l.currentMmap[offset:idx])
	binary.BigEndian.PutUint32(l.currentMmap[idx:idx+4], checksum)
	idx += 4

	l.writePos.Store(idx)

	if l.syncEnabled {
		if err := msync(l.currentMmap[:idx]); err != nil {
			return 0, err
		}
	}

	return offset, nil
}

func (l *Log) Read(offset uint64) (*Entry, error) {
	segmentID := offset / SegmentSize
	pos := offset % SegmentSize

	l.mu.Lock()
	defer l.mu.Unlock()

	currentID := l.segmentID.Load()
	if segmentID != currentID {
		if err := l.openSegment(segmentID); err != nil {
			return nil, err
		}
	}

	if pos < HeaderSize || pos >= SegmentSize {
		return nil, fmt.Errorf("invalid offset %d", offset)
	}

	idx := pos

	if l.currentMmap[idx] != 0x01 {
		return nil, fmt.Errorf("invalid entry type at offset %d", offset)
	}
	idx++

	length := binary.BigEndian.Uint32(l.currentMmap[idx : idx+4])
	idx += 4

	entryOffset := binary.BigEndian.Uint64(l.currentMmap[idx : idx+8])
	idx += 8

	if entryOffset != offset {
		return nil, fmt.Errorf("offset mismatch: expected %d, got %d", offset, entryOffset)
	}

	data := make([]byte, length)
	copy(data, l.currentMmap[idx:idx+uint64(length)])
	idx += uint64(length)

	checksum := binary.BigEndian.Uint32(l.currentMmap[idx : idx+4])
	computed := crc32.ChecksumIEEE(l.currentMmap[offset:idx])

	if checksum != computed {
		return nil, fmt.Errorf("checksum mismatch at offset %d", offset)
	}

	return &Entry{Offset: offset, Data: data}, nil
}

func (l *Log) Sync() error {
	if l.currentMmap == nil {
		return nil
	}
	return msync(l.currentMmap)
}

func (l *Log) Close() error {
	if l.currentMmap != nil {
		munmap(l.currentMmap)
		l.currentMmap = nil
	}
	if l.currentFile != nil {
		l.currentFile.Close()
		l.currentFile = nil
	}
	return nil
}

func (l *Log) SetSync(enabled bool) {
	l.syncEnabled = enabled
}

func (l *Log) FirstOffset() uint64 {
	return HeaderSize
}

func mmap(f *os.File, size int) ([]byte, error) {
	return mmapFile(f, 0, size)
}

func msync(b []byte) error {
	return msyncImpl(b)
}

func munmap(b []byte) error {
	return munmapImpl(b)
}