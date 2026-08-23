package replication

import (
	"github.com/example/persistent-event-stream-broker/internal/domain"
	"sort"
	"sync"
)

type Simulator struct {
	Set     *domain.ReplicaSet
	Offsets map[string]domain.Offset
	mu      sync.Mutex
}

func New(topic string, part int, replicas []string) *Simulator {
	return &Simulator{Set: domain.NewReplicaSet(topic, part, replicas), Offsets: map[string]domain.Offset{}}
}
func (s *Simulator) Append(id string, off domain.Offset) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.Set.Replicas[id]
	if !ok || r.State == domain.ReplicaOffline {
		return domain.E(domain.ErrUnavailable, "replica unavailable")
	}
	if r.State != domain.ReplicaLeader { return domain.E(domain.ErrFenced, "not leader") }
	s.Offsets[id] = off
	return nil
}
func (s *Simulator) Quorum() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	vals := []int{}
	for id, o := range s.Offsets {
		if s.Set.Replicas[id].ISR {
			vals = append(vals, int(o))
		}
	}
	sort.Ints(vals)
	return len(vals) >= len(s.Set.Replicas)/2+1
}
func (s *Simulator) Failover(id string) error { return s.Set.Failover(id) }
func (s *Simulator) Reconcile()               { s.mu.Lock(); defer s.mu.Unlock(); s.Set.Reconcile(s.Offsets) }
