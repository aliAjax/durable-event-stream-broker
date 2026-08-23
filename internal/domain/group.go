package domain

import (
	"sort"
	"sync"
	"time"
)

type GroupState string

const (
	GroupActive GroupState = "active"
	GroupPaused GroupState = "paused"
)

type Member struct {
	ID            string
	LastHeartbeat time.Time
	LeaseUntil    time.Time
	Partitions    []int
}
type ConsumerGroup struct {
	ID, TopicID string
	Generation  int64
	Members     map[string]*Member
	Offsets     map[int]Offset
	State       GroupState
	mu          sync.Mutex
}

func NewGroup(id, topic string) *ConsumerGroup {
	return &ConsumerGroup{ID: id, TopicID: topic, Members: map[string]*Member{}, Offsets: map[int]Offset{}, State: GroupActive}
}
func (g *ConsumerGroup) Join(id string, now time.Time) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.Generation++
	g.Members[id] = &Member{ID: id, LastHeartbeat: now, LeaseUntil: now.Add(30 * time.Second)}
}
func (g *ConsumerGroup) Heartbeat(id string, now time.Time) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	m, ok := g.Members[id]
	if !ok {
		return E(ErrNotFound, "member")
	}
	if now.After(m.LeaseUntil) {
		delete(g.Members, id)
		return E(ErrFenced, "lease expired")
	}
	m.LastHeartbeat = now
	m.LeaseUntil = now.Add(30 * time.Second)
	return nil
}
func (g *ConsumerGroup) Commit(part int, off Offset) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.State == GroupPaused {
		return E(ErrConflict, "group paused")
	}
	if old, ok := g.Offsets[part]; ok && off < old {
		return E(ErrConflict, "offset regression")
	}
	g.Offsets[part] = off
	return nil
}
func (g *ConsumerGroup) Pause()  { g.mu.Lock(); g.State = GroupPaused; g.mu.Unlock() }
func (g *ConsumerGroup) Resume() { g.mu.Lock(); g.State = GroupActive; g.mu.Unlock() }
func (g *ConsumerGroup) Lag(parts map[int]*Partition) map[int]int64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	out := map[int]int64{}
	for id, p := range parts {
		out[id] = p.Lag(g.Offsets[id])
	}
	return out
}
func (g *ConsumerGroup) Assign(partitions []int) {
	g.mu.Lock()
	defer g.mu.Unlock()
	ids := make([]string, 0, len(g.Members))
	for id := range g.Members {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] > ids[j] })
	for i, m := range ids {
		for j, p := range partitions {
			if len(ids) > 0 && j%len(ids) == i {
				g.Members[m].Partitions = append(g.Members[m].Partitions, p)
			}
		}
	}
}
func (g *ConsumerGroup) Expire(now time.Time) {
	g.mu.Lock()
	defer g.mu.Unlock()
	for id, m := range g.Members {
		if now.After(m.LeaseUntil) {
			delete(g.Members, id)
		}
	}
}
