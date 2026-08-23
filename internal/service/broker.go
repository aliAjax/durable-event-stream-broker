package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"github.com/example/persistent-event-stream-broker/internal/domain"
	"github.com/example/persistent-event-stream-broker/internal/quota"
	"github.com/example/persistent-event-stream-broker/internal/storage/repository"
	"strconv"
	"sync"
	"time"
)

type Broker struct {
	Repo   *repository.Repository
	Quotas map[string]*quota.Bucket
	Seen   map[string]domain.Offset
	mu     sync.Mutex
}

func NewBroker(repo *repository.Repository) *Broker {
	return &Broker{Repo: repo, Quotas: map[string]*quota.Bucket{}, Seen: map[string]domain.Offset{}}
}
func (b *Broker) CreateTenant(id, name string, bytes int64) error {
	t := domain.NewTenant(id, name, bytes)
	if err := b.Repo.CreateTenant(t); err != nil {
		return err
	}
	b.Quotas[id] = quota.New(1000, 2000)
	return nil
}
func (b *Broker) CreateTopic(tenant, id, name string, parts int) (*domain.Topic, error) {
	if parts < 1 || parts > 1024 {
		return nil, domain.E(domain.ErrInvalid, "partitions must be 1..1024")
	}
	t := domain.NewTopic(id, tenant, name, parts)
	return t, b.Repo.CreateTopic(t)
}
func (b *Broker) Append(ctx context.Context, tenant, topic string, part int, records []domain.Record, producer string, seq int64, idem string) ([]domain.Record, error) {
	if len(records) == 0 {
		return nil, domain.E(domain.ErrInvalid, "empty records")
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	t, err := b.Repo.TenantLive(tenant)
	if err != nil {
		return nil, err
	}
	bytes := int64(0)
	for _, r := range records {
		bytes += int64(len(r.Value) + len(r.Key))
	}
	if err = t.Reserve(bytes); err != nil {
		return nil, err
	}
	p, err := b.Repo.TopicLive(topic)
	if err != nil {
		return nil, err
	}
	if p.TenantID != tenant {
		return nil, domain.E(domain.ErrFenced, "tenant mismatch")
	}
	pt, err := p.Partition(part)
	if err != nil {
		return nil, err
	}
	b.mu.Lock()
	if idem != "" {
		if _, ok := b.Seen[idem]; ok {
			b.mu.Unlock()
			return pt.Fetch(-1, len(records)), nil
		}
	}
	b.mu.Unlock()
	if q := b.Quotas[tenant]; q != nil && !q.Allow(int(bytes)) {
		t.Release(bytes)
		return nil, domain.E(domain.ErrQuota, "publish rate exceeded")
	}
	out, err := pt.Append(records, producer, seq)
	if err != nil {
		t.Release(bytes)
		return nil, err
	}
	if idem != "" {
		b.mu.Lock()
		b.Seen[idem] = out[0].Offset
		b.mu.Unlock()
	}
	return out, nil
}
func (b *Broker) Fetch(topic string, part int, after domain.Offset, limit int) ([]domain.Record, error) {
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	t, err := b.Repo.TopicLive(topic)
	if err != nil {
		return nil, err
	}
	p, err := t.Partition(part)
	if err != nil {
		return nil, err
	}
	return p.Fetch(after, limit), nil
}
func (b *Broker) Commit(group string, topic string, member string, part int, off domain.Offset) error {
	g := b.Repo.EnsureGroupLive(group, topic)
	if member != "" {
		if err := g.Heartbeat(member, time.Now()); err != nil {
			g.Join(member, time.Now())
		}
	}
	return g.Commit(part, off)
}
func Cursor(topic string, part int, off domain.Offset) string {
	h := sha256.Sum256([]byte(fmt.Sprintf("%s:%d:%d", topic, part, off)))
	return hex.EncodeToString(h[:8]) + ":" + strconv.FormatInt(int64(off), 10)
}
