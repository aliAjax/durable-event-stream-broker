package application

import (
	"context"
	"github.com/example/persistent-event-stream-broker/internal/domain"
	"github.com/example/persistent-event-stream-broker/internal/storage/repository"
)

type QuotaService struct{ Repo *repository.Repository }

func (q *QuotaService) Usage(ctx context.Context, tenant string) (map[string]any, error) {
	t, e := q.Repo.Tenant(tenant)
	if e != nil {
		return nil, e
	}
	u, l := t.Snapshot()
	return map[string]any{"tenant_id": tenant, "used_bytes": u, "quota_bytes": l, "remaining": l - u}, nil
}
func (q *QuotaService) Set(ctx context.Context, tenant string, bytes int64) error {
	t, e := q.Repo.Tenant(tenant)
	if e != nil {
		return e
	}
	if bytes < 0 || bytes > domain.MaxQuotaBytes {
		return domain.E(domain.ErrInvalid, "quota out of range")
	}
	t.QuotaBytes = bytes
	return nil
}
