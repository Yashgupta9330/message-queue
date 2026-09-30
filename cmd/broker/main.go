package main

import (
	"context"
	"log"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/yashgupta/message-queue/internal/ackmgr"
	"github.com/yashgupta/message-queue/internal/api"
	"github.com/yashgupta/message-queue/internal/connmgr"
	"github.com/yashgupta/message-queue/internal/dispatcher"
	"github.com/yashgupta/message-queue/internal/dlqttl"
	"github.com/yashgupta/message-queue/internal/queue"
	"github.com/yashgupta/message-queue/internal/retry"
	proto "github.com/yashgupta/message-queue/proto/gen"
	"google.golang.org/grpc"
)

func main() {
	walDir := envOr("WAL_DIR", "data/wal")
	listenAddr := envOr("LISTEN_ADDR", ":50051")
	maxRetries := int32(envOrInt("MAX_RETRIES", 3))
	dispatchTimeout := envOrDuration("DISPATCH_TIMEOUT", 30*time.Second)
	scanInterval := envOrDuration("SCAN_INTERVAL", 5*time.Second)
	dlqTTL := envOrDuration("DLQ_TTL", 30*24*time.Hour)
	dlqTTLScanInterval := envOrDuration("DLQ_TTL_SCAN_INTERVAL", time.Hour)
	shutdownTimeout := envOrDuration("SHUTDOWN_TIMEOUT", 15*time.Second)

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	manager, err := queue.NewManager(walDir, logger)
	if err != nil {
		log.Fatalf("queue manager: %v", err)
	}
	defer manager.Close()

	registry := connmgr.NewRegistry()
	ackMgr := ackmgr.NewAckManager(manager, registry, maxRetries, logger)
	disp := dispatcher.NewDispatcher(manager, registry, ackMgr, logger)
	scanner := retry.NewScanner(ackMgr, scanInterval, dispatchTimeout, logger)
	dlqScanner := dlqttl.NewScanner(manager, ackMgr, dlqTTL, dlqTTLScanInterval, logger)

	go scanner.Run(ctx)
	go dlqScanner.Run(ctx)

	listener, err := net.Listen("tcp", listenAddr)
	if err != nil {
		log.Fatalf("listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	proto.RegisterBrokerServer(grpcServer, api.NewServer(ctx, manager, registry, ackMgr, disp, logger))

	// Graceful shutdown on SIGINT or SIGTERM.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigCh
		logger.Info("signal received, shutting down", "signal", sig.String())

		// Stop accepting new Publish requests immediately.
		manager.BeginShutdown()

		// Stop accepting new gRPC connections and wait for ongoing RPCs to finish.
		stopped := make(chan struct{})
		go func() {
			grpcServer.GracefulStop()
			close(stopped)
		}()
		select {
		case <-stopped:
		case <-time.After(shutdownTimeout):
			logger.Warn("graceful stop timed out, forcing")
			grpcServer.Stop()
		}

		// Wait for in-flight messages to be ACKed/NACKed before closing the WAL.
		if !ackMgr.WaitDrained(shutdownTimeout) {
			logger.Warn("in-flight messages not fully drained; they will be redelivered on restart")
		}

		cancel()
	}()

	logger.Info("messager broker started",
		"addr", listenAddr,
		"wal_dir", walDir,
		"max_retries", maxRetries,
		"dispatch_timeout", dispatchTimeout,
		"scan_interval", scanInterval,
		"dlq_ttl", dlqTTL,
		"dlq_ttl_scan_interval", dlqTTLScanInterval,
	)
	if err := grpcServer.Serve(listener); err != nil {
		log.Fatalf("serve: %v", err)
	}
}

func envOr(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func envOrInt(key string, fallback int) int {
	if value := os.Getenv(key); value != "" {
		if n, err := strconv.Atoi(value); err == nil {
			return n
		}
	}
	return fallback
}

func envOrDuration(key string, fallback time.Duration) time.Duration {
	if value := os.Getenv(key); value != "" {
		if d, err := time.ParseDuration(value); err == nil {
			return d
		}
	}
	return fallback
}
