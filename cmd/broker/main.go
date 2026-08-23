package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/example/persistent-event-stream-broker/internal/app"
	"github.com/example/persistent-event-stream-broker/internal/config"
)

func main() {
	cfg := config.Default()
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	server, err := app.NewServer(cfg, logger)
	if err != nil {
		logger.Error("server_init_failed", "error", err)
		os.Exit(1)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	go func() {
		if err := server.Start(); err != nil && err != http.ErrServerClosed {
			logger.Error("http_failed", "error", err)
		}
	}()
	<-ctx.Done()
	shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := server.Stop(shutdown); err != nil {
		logger.Error("shutdown_failed", "error", err)
	}
}
