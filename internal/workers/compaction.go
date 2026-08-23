package workers

import (
	"context"
	"github.com/example/persistent-event-stream-broker/internal/storage/repository"
	"sync"
	"time"
)

type CompactionWorker struct {
	Repo   *repository.Repository
	Every  time.Duration
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func NewCompaction(r *repository.Repository) *CompactionWorker {
	return &CompactionWorker{Repo: r, Every: 5 * time.Minute}
}
func (w *CompactionWorker) Start(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	w.cancel = cancel
	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		ticker := time.NewTicker(w.Every)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				w.Run()
			case <-ctx.Done():
				return
			}
		}
	}()
}
func (w *CompactionWorker) Run() {
	for _, t := range w.Repo.ListTopics("") {
		for _, p := range t.Partitions {
			p.Compact()
		}
	}
}
func (w *CompactionWorker) Stop() {
	if w.cancel != nil {
		w.cancel()
	}
	w.wg.Wait()
}
