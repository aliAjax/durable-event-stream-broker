package app

import (
	"context"
	"github.com/example/persistent-event-stream-broker/internal/config"
	"github.com/example/persistent-event-stream-broker/internal/service"
	"github.com/example/persistent-event-stream-broker/internal/storage/repository"
	transport "github.com/example/persistent-event-stream-broker/internal/transport/http"
	"log/slog"
)

type Server struct{ HTTP *transport.Server }

func NewServer(cfg config.Config, logger *slog.Logger) (*Server, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	repo := repository.New()
	broker := service.NewBroker(repo)
	return &Server{HTTP: transport.New(cfg.HTTPAddr, broker)}, nil
}
func (s *Server) Start() error                   { return s.HTTP.Start() }
func (s *Server) Stop(ctx context.Context) error { return s.HTTP.Stop(ctx) }
