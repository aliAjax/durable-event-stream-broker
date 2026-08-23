package observability

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sync"
	"time"
)

type AuditEvent struct {
	ID, Action, Actor, Resource string
	At                          time.Time
	PrevHash, Hash              string
	Fields                      map[string]string
}
type Chain struct {
	mu     sync.Mutex
	events []AuditEvent
}

func (c *Chain) Append(action, actor, res string, fields map[string]string) AuditEvent {
	c.mu.Lock()
	defer c.mu.Unlock()
	e := AuditEvent{Action: action, Actor: actor, Resource: res, At: time.Now().UTC(), Fields: fields}
	if len(c.events) > 0 {
		e.PrevHash = c.events[len(c.events)-1].Hash
	}
	b, _ := json.Marshal(e)
	h := sha256.Sum256(append([]byte(e.PrevHash), b...))
	e.Hash = hex.EncodeToString(h[:])
	e.ID = e.Hash[:16]
	c.events = append(c.events, e)
	return e
}
func (c *Chain) Verify() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	prev := ""
	for _, e := range c.events {
		if e.PrevHash != prev {
			return false
		}
		prev = e.Hash
	}
	return true
}
