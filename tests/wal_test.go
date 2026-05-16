package tests

import (
	"fmt"
	"os"
	"path/filepath"
	"pulse/internal/wal"
	"sync"
	"testing"
)

func TestWAL1_RecoveryAfterCrash(t *testing.T) {
	dir := t.TempDir()

	log1, err := wal.Open(dir)
	if err != nil {
		t.Fatalf("failed to open wal: %v", err)
	}

	totalMessages := 10000
	offsets := make([]uint64, totalMessages)

	for i := 0; i < totalMessages; i++ {
		data := []byte(fmt.Sprintf("message-%d", i))
		offset, err := log1.Write(data)
		if err != nil {
			t.Fatalf("failed to write: %v", err)
		}
		offsets[i] = offset
	}

	log1.Close()

	log2, err := wal.Open(dir)
	if err != nil {
		t.Fatalf("failed to reopen wal: %v", err)
	}

	for i := 0; i < totalMessages; i++ {
		entry, err := log2.Read(offsets[i])
		if err != nil {
			t.Fatalf("failed to read entry %d at offset %d: %v", i, offsets[i], err)
		}
		expected := fmt.Sprintf("message-%d", i)
		if string(entry.Data) != expected {
			t.Errorf("entry %d: got %q, want %q", i, entry.Data, expected)
		}
	}

	log2.Close()
}

func TestWAL2_SegmentRotation(t *testing.T) {
	dir := t.TempDir()

	log, err := wal.Open(dir)
	if err != nil {
		t.Fatalf("failed to open wal: %v", err)
	}

	entriesPerSegment := wal.SegmentSize / (wal.EntryHeaderSize + 100)
	targetSegments := 2

	for i := 0; i < int(entriesPerSegment)*targetSegments; i++ {
		data := make([]byte, 100)
		for j := range data {
			data[j] = byte(i)
		}
		_, err := log.Write(data)
		if err != nil {
			t.Fatalf("failed to write: %v", err)
		}
	}

	files, err := filepath.Glob(filepath.Join(dir, "segment_*.log"))
	if err != nil {
		t.Fatalf("failed to list files: %v", err)
	}

	if len(files) < 2 {
		t.Errorf("expected at least 2 segments, got %d", len(files))
	}

	log.Close()
}

func TestWAL3_ConcurrentWriters(t *testing.T) {
	dir := t.TempDir()

	log, err := wal.Open(dir)
	if err != nil {
		t.Fatalf("failed to open wal: %v", err)
	}

	numWriters := 8
	entriesPerWriter := 10000
	totalEntries := numWriters * entriesPerWriter

	var wg sync.WaitGroup
	wg.Add(numWriters)

	offsetChan := make(chan uint64, totalEntries)

	for w := 0; w < numWriters; w++ {
		go func(writerID int) {
			defer wg.Done()
			for i := 0; i < entriesPerWriter; i++ {
				data := []byte(fmt.Sprintf("writer-%d-msg-%d", writerID, i))
				offset, err := log.Write(data)
				if err != nil {
					t.Errorf("writer %d failed to write: %v", writerID, err)
					return
				}
				offsetChan <- offset
			}
		}(w)
	}

	wg.Wait()
	close(offsetChan)

	offsets := make([]uint64, 0, totalEntries)
	for offset := range offsetChan {
		offsets = append(offsets, offset)
	}

	if len(offsets) != totalEntries {
		t.Errorf("expected %d entries, got %d", totalEntries, len(offsets))
	}

	log.Close()
}

func TestWAL4_ChecksumValidation(t *testing.T) {
	dir := t.TempDir()

	log, err := wal.Open(dir)
	if err != nil {
		t.Fatalf("failed to open wal: %v", err)
	}

	offset, err := log.Write([]byte("test message"))
	if err != nil {
		t.Fatalf("failed to write: %v", err)
	}

	log.Close()

	files, _ := filepath.Glob(filepath.Join(dir, "segment_*.log"))
	if len(files) == 0 {
		t.Fatal("no segment file found")
	}

	f, err := os.OpenFile(files[0], os.O_RDWR, 0644)
	if err != nil {
		t.Fatalf("failed to open segment: %v", err)
	}

	corruptOffset := int(offset) + 20
	if corruptOffset < int(wal.SegmentSize) {
		f.WriteAt([]byte{0xFF}, int64(corruptOffset))
	}
	f.Close()

	log2, err := wal.Open(dir)
	if err != nil {
		t.Fatalf("failed to reopen wal: %v", err)
	}

	_, err = log2.Read(offset)
	if err == nil {
		t.Error("expected error when reading corrupted entry, got nil")
	}

	log2.Close()
}

func BenchmarkAppend_NoSync(b *testing.B) {
	dir := b.TempDir()
	log, _ := wal.Open(dir)
	log.SetSync(false)

	data := make([]byte, 64)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		log.Write(data)
	}

	log.Close()
}

func BenchmarkAppend_Fsync(b *testing.B) {
	dir := b.TempDir()
	log, _ := wal.Open(dir)
	log.SetSync(true)

	data := make([]byte, 64)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		log.Write(data)
	}

	log.Close()
}

func BenchmarkRead_Sequential(b *testing.B) {
	dir := b.TempDir()
	log, _ := wal.Open(dir)

	offsets := make([]uint64, b.N)
	for i := 0; i < b.N; i++ {
		data := []byte(fmt.Sprintf("msg-%d", i))
		offset, _ := log.Write(data)
		offsets[i] = offset
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		log.Read(offsets[i])
	}

	log.Close()
}