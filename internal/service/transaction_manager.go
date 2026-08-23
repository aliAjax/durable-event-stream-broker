package service

import (
	"context"
	"github.com/example/persistent-event-stream-broker/internal/domain"
	"sync"
	"time"
)

type TransactionManager struct {
	items map[string]*domain.Transaction
	mu    sync.Mutex
}

func NewTransactionManager() *TransactionManager {
	return &TransactionManager{items: map[string]*domain.Transaction{}}
}
func (m *TransactionManager) Begin(ctx context.Context, id, producer string, ttl time.Duration) (*domain.Transaction, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.items[id]; ok {
		return nil, domain.E(domain.ErrConflict, "transaction exists")
	}
	if ttl <= 0 || ttl > 24*time.Hour {
		ttl = time.Minute
	}
	t := domain.NewTransaction(id, producer, ttl)
	m.items[id] = t
	return t, nil
}
func (m *TransactionManager) Commit(ctx context.Context, id string) error {
	m.mu.Lock()
	t, ok := m.items[id]
	m.mu.Unlock()
	if !ok {
		return domain.E(domain.ErrNotFound, "transaction")
	}
	return t.Commit()
}
func (m *TransactionManager) Abort(ctx context.Context, id string) error {
	m.mu.Lock()
	t, ok := m.items[id]
	m.mu.Unlock()
	if !ok {
		return domain.E(domain.ErrNotFound, "transaction")
	}
	t.Abort()
	return nil
}
func (m *TransactionManager) Reap(now time.Time) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := 0
	for _, t := range m.items {
		if t.Expired() {
			t.Abort()
			n++
		}
	}
	return n
}
