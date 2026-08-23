package workers

import (
	"github.com/example/persistent-event-stream-broker/internal/domain"
	"github.com/example/persistent-event-stream-broker/internal/storage/repository"
	"sync"
	"time"
)

type RebalanceWorker struct {
	Repo *repository.Repository
	mu   sync.Mutex
}

func (w *RebalanceWorker) Run() {
	w.mu.Lock()
	defer w.mu.Unlock()
	now := time.Now()
	for _, g := range w.Repo.Groups {
		g.Expire(now)
	}
}
func Assign(g *domain.ConsumerGroup, partitions []int) { g.Assign(partitions) }
