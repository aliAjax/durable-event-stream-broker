package domain

import "time"

type HealthSnapshot struct {
	TopicID   string
	LeaderID  string
	ISR       int
	EndOffset Offset
	Committed Offset
	Lag       int64
	CheckedAt time.Time
	Healthy   bool
	Reason    string
}

func BuildHealth(topic, leader string, isr int, end, committed Offset, now time.Time) HealthSnapshot {
	h := HealthSnapshot{TopicID: topic, LeaderID: leader, ISR: isr, EndOffset: end, Committed: committed, CheckedAt: now}
	h.Lag = int64(end - committed)
	h.Healthy = leader != "" && isr > 0
	if !h.Healthy {
		h.Reason = "no_leader_or_replica"
	} else if h.Lag > 10000 {
		h.Reason = "consumer_lag"
	} else {
		h.Reason = "healthy"
	}
	return h
}
func (h HealthSnapshot) Stale(now time.Time, maxAge time.Duration) bool {
	return now.Sub(h.CheckedAt) > maxAge
}
