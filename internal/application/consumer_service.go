package application

import (
	"context"
	"github.com/example/persistent-event-stream-broker/internal/domain"
	"github.com/example/persistent-event-stream-broker/internal/service"
	"time"
)

type ConsumerService struct{ Broker *service.Broker }

func (s *ConsumerService) Join(ctx context.Context, group, topic, member string) error {
	if group == "" || member == "" {
		return domain.E(domain.ErrInvalid, "group and member required")
	}
	g := s.Broker.Repo.EnsureGroupLive(group, topic)
	g.Join(member, time.Now())
	return nil
}
func (s *ConsumerService) Pause(ctx context.Context, group string) error {
	g := s.Broker.Repo.EnsureGroupLive(group, "")
	g.Pause()
	return nil
}
func (s *ConsumerService) Resume(ctx context.Context, group string) error {
	g := s.Broker.Repo.EnsureGroupLive(group, "")
	g.Resume()
	return nil
}
func (s *ConsumerService) Commit(ctx context.Context, group string, part int, off domain.Offset) error {
	return s.Broker.Commit(group, "", "", part, off)
}
