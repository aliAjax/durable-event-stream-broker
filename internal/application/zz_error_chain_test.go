package application

import (
	"context"
	"errors"
	"testing"
	"github.com/example/persistent-event-stream-broker/internal/domain"
	"github.com/example/persistent-event-stream-broker/internal/service"
	"github.com/example/persistent-event-stream-broker/internal/storage/repository"
)

func TestStreamCreateErrorChain(t *testing.T) {
	s := &StreamService{Broker: service.NewBroker(repository.New())}
	_, err := s.Create(context.Background(), "tenant", "topic", "name", 1)
	var be *domain.BrokerError
	if !errors.As(err, &be) { t.Fatalf("error chain lost: %v", err) }
}
