package outbox

import (
	"sync"
	"time"
)

type Event struct {
	ID, Topic string
	Payload   []byte
	CreatedAt time.Time
	Delivered bool
}
type Store struct {
	events []Event
	mu     sync.Mutex
}

func (s *Store) Add(e Event) { s.mu.Lock(); defer s.mu.Unlock(); s.events = append(s.events, e) }
func (s *Store) Pending(limit int) []Event {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := []Event{}
	for _, e := range s.events {
		if !e.Delivered {
			out = append(out, e)
			if len(out) >= limit {
				break
			}
		}
	}
	return out
}
func (s *Store) Ack(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := range s.events {
		if s.events[i].ID == id {
			s.events[i].Delivered = true
			return true
		}
	}
	return false
}
func (s *Store) Size() int { s.mu.Lock(); defer s.mu.Unlock(); return len(s.events) }
