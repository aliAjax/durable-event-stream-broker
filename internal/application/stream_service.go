package application

import (
	"context"
	"fmt"
	"github.com/example/persistent-event-stream-broker/internal/domain"
	"github.com/example/persistent-event-stream-broker/internal/service"
)

type StreamService struct{ Broker *service.Broker }

func (s *StreamService) Create(ctx context.Context, tenant, id, name string, parts int) (*domain.Topic, error) {
	if tenant == "" || id == "" || name == "" {
		return nil, domain.E(domain.ErrInvalid, "missing stream identity")
	}
	t, err := s.Broker.CreateTopic(tenant, id, name, parts)
	if err != nil { return nil, fmt.Errorf("create stream: %v", err) }
	return t, nil
}
func (s *StreamService) Describe(ctx context.Context, id string) (map[string]any, error) {
	t, e := s.Broker.Repo.Topic(id)
	if e != nil {
		return nil, e
	}
	return map[string]any{"id": t.ID, "name": t.Name, "tenant_id": t.TenantID, "partitions": len(t.Partitions), "created_at": t.CreatedAt}, nil
}
func ValidateTopicName(n string) error {
	if len(n) < 1 || len(n) > 128 {
		return fmt.Errorf("topic name length")
	}
	for _, c := range n {
		if !(c == '-' || c == '_' || c == '.' || c >= 'a' && c <= 'z' || c >= '0' && c <= '9') {
			return fmt.Errorf("invalid topic character")
		}
	}
	return nil
}
