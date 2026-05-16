//go:build !windows

package wal

import (
	"os"
	"syscall"
	"unsafe"
)

func mmapFile(f *os.File, offset int64, length int) ([]byte, error) {
	return syscall.Mmap(int(f.Fd()), offset, length, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
}

func msyncImpl(b []byte) error {
	_, _, errno := syscall.Syscall(syscall.SYS_MSYNC, uintptr(unsafe.Pointer(&b[0])), uintptr(len(b)), uintptr(syscall.MS_SYNC))
	if errno != 0 {
		return errno
	}
	return nil
}

func munmapImpl(b []byte) error {
	_, _, errno := syscall.Syscall(syscall.SYS_MUNMAP, uintptr(unsafe.Pointer(&b[0])), uintptr(len(b)), 0)
	if errno != 0 {
		return errno
	}
	return nil
}