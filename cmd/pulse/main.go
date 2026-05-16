package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"pulse/internal/server"
	"pulse/internal/topic"
	"pulse/internal/wal"
)

func main() {
	addr := flag.String("addr", ":7777", "server address")
	walDir := flag.String("wal", "./wal", "WAL directory")
	ringCap := flag.Uint64("ring", 131072, "ring buffer capacity")
	flag.Parse()

	walLog, err := wal.Open(*walDir)
	if err != nil {
		fmt.Printf("failed to open WAL: %v\n", err)
		os.Exit(1)
	}

	tm := topic.NewManager(walLog, *ringCap)
	srv := server.New(*addr, tm)

	if err := srv.Start(); err != nil {
		fmt.Printf("failed to start server: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Pulse server started on %s\n", srv.Addr())

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	fmt.Println("Shutting down...")
	srv.Stop()
	walLog.Close()
}