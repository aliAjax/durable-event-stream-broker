package workers

import (
	"context"
	"github.com/example/persistent-event-stream-broker/internal/storage/repository"
	"sync"
	"time"
)

type RetentionWorker struct {
	Repo     *repository.Repository
	Interval time.Duration
	stop     chan struct{}
	wg       sync.WaitGroup
}

func NewRetention(r *repository.Repository) *RetentionWorker {
	return &RetentionWorker{Repo: r, Interval: time.Minute, stop: make(chan struct{})}
}
func (w *RetentionWorker) Start() {
	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		ticker := time.NewTicker(w.Interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				w.Run()
			case <-w.stop:
				return
			}
		}
	}()
}
func (w *RetentionWorker) Run() {
	for _, t := range w.Repo.ListTopics("") {
		for _, p := range t.Partitions {
			p.Compact()
		}
	}
}
func (w *RetentionWorker) Stop(ctx context.Context) error {
	close(w.stop)
	done := make(chan struct{})
	go func() { w.wg.Wait(); close(done) }()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
