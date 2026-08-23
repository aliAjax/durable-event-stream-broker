package repository

import (
	"github.com/example/persistent-event-stream-broker/internal/domain"
	"sync"
)

type Repository struct {
	Tenants map[string]*domain.Tenant
	Topics  map[string]*domain.Topic
	Groups  map[string]*domain.ConsumerGroup
	mu      sync.RWMutex
}

func New() *Repository {
	return &Repository{Tenants: map[string]*domain.Tenant{}, Topics: map[string]*domain.Topic{}, Groups: map[string]*domain.ConsumerGroup{}}
}
func (r *Repository) CreateTenant(t *domain.Tenant) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.Tenants[t.ID]; ok {
		return domain.E(domain.ErrConflict, "tenant exists")
	}
	r.Tenants[t.ID] = t.Clone()
	return nil
}
func (r *Repository) Tenant(id string) (*domain.Tenant, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	t, ok := r.Tenants[id]
	if !ok {
		return nil, domain.E(domain.ErrNotFound, "tenant")
	}
	return t.Clone(), nil
}
func (r *Repository) CreateTopic(t *domain.Topic) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.Topics[t.ID]; ok {
		return domain.E(domain.ErrConflict, "topic exists")
	}
	if _, ok := r.Tenants[t.TenantID]; !ok {
		return domain.E(domain.ErrNotFound, "tenant")
	}
	r.Topics[t.ID] = t.Clone()
	return nil
}
func (r *Repository) Topic(id string) (*domain.Topic, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	t, ok := r.Topics[id]
	if !ok {
		return nil, domain.E(domain.ErrNotFound, "topic")
	}
	return t.Clone(), nil
}
func (r *Repository) EnsureGroup(id, topic string) *domain.ConsumerGroup {
	r.mu.Lock()
	defer r.mu.Unlock()
	if g, ok := r.Groups[id]; ok {
		return g.Clone()
	}
	g := domain.NewGroup(id, topic)
	r.Groups[id] = g
	return g.Clone()
}
func (r *Repository) EnsureGroupLive(id, topic string) *domain.ConsumerGroup {
	r.mu.Lock()
	defer r.mu.Unlock()
	if g, ok := r.Groups[id]; ok {
		return g
	}
	g := domain.NewGroup(id, topic)
	r.Groups[id] = g
	return g
}
func (r *Repository) ListTopics(tenant string) []*domain.Topic {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := []*domain.Topic{}
	for _, t := range r.Topics {
		if tenant == "" || t.TenantID == tenant {
			out = append(out, t.Clone())
		}
	}
	return out
}
func (r *Repository) TopicLive(id string) (*domain.Topic, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	t, ok := r.Topics[id]
	if !ok {
		return nil, domain.E(domain.ErrNotFound, "topic")
	}
	return t, nil
}
func (r *Repository) TenantLive(id string) (*domain.Tenant, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	t, ok := r.Tenants[id]
	if !ok {
		return nil, domain.E(domain.ErrNotFound, "tenant")
	}
	return t, nil
}
func (r *Repository) ForEachTopic(fn func(*domain.Topic)) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, t := range r.Topics {
		fn(t)
	}
}
func (r *Repository) ForEachGroup(fn func(*domain.ConsumerGroup)) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, g := range r.Groups {
		fn(g)
	}
}
