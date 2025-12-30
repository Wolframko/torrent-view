package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/user/torrent-view/internal/api"
	"github.com/user/torrent-view/internal/stream"
)

func main() {
	// Parse flags
	addr := flag.String("addr", ":8080", "HTTP server address")
	dataDir := flag.String("data", "./data", "Data directory for torrent and HLS files")
	maxSegments := flag.Int("max-segments", 30, "Maximum HLS segments to keep (saves disk space)")
	bufferMB := flag.Int64("buffer", 50, "Buffer ahead size in MB for torrent downloading")
	flag.Parse()

	log.Printf("Starting Torrent View server...")
	log.Printf("  Address: %s", *addr)
	log.Printf("  Data dir: %s", *dataDir)
	log.Printf("  Max segments: %d", *maxSegments)
	log.Printf("  Buffer ahead: %d MB", *bufferMB)

	// Create stream manager
	streamMgr, err := stream.NewManager(*dataDir, *maxSegments, *bufferMB)
	if err != nil {
		log.Fatalf("Failed to create stream manager: %v", err)
	}

	// Create API server
	server := api.NewServer(streamMgr)

	// Create HTTP server
	httpServer := &http.Server{
		Addr:         *addr,
		Handler:      server.Router(),
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	// Handle graceful shutdown
	done := make(chan bool)
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-quit
		log.Println("Shutting down server...")

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		if err := httpServer.Shutdown(ctx); err != nil {
			log.Fatalf("Server shutdown error: %v", err)
		}

		if err := streamMgr.Close(); err != nil {
			log.Printf("Stream manager close error: %v", err)
		}

		close(done)
	}()

	log.Printf("Server is ready at http://localhost%s", *addr)
	if err := httpServer.ListenAndServe(); err != http.ErrServerClosed {
		log.Fatalf("Server error: %v", err)
	}

	<-done
	log.Println("Server stopped")
}
