package domain

import "time"

type RetentionDecision struct {
	TopicID string
	Offset  Offset
	Age     time.Duration
	Bytes   int64
	Delete  bool
	Reason  string
}

func EvaluateRetention(topicID string, offset Offset, created time.Time, size int64, now time.Time, maxAge time.Duration, maxBytes int64) RetentionDecision {
	age := now.Sub(created)
	decision := RetentionDecision{TopicID: topicID, Offset: offset, Age: age, Bytes: size}
	if maxAge > 0 && age >= maxAge {
		decision.Delete, decision.Reason = true, "age_limit"
		return decision
	}
	if maxBytes > 0 && size >= maxBytes {
		decision.Delete, decision.Reason = true, "byte_limit"
		return decision
	}
	decision.Reason = "retained"
	return decision
}

type OffsetWindow struct {
	First Offset
	Last  Offset
	Count int
}

func Window(offsets []Offset) OffsetWindow {
	if len(offsets) == 0 {
		return OffsetWindow{}
	}
	first, last := offsets[0], offsets[0]
	for _, offset := range offsets[1:] {
		if offset < first {
			first = offset
		}
		if offset > last {
			last = offset
		}
	}
	return OffsetWindow{First: first, Last: last, Count: len(offsets)}
}

func (w OffsetWindow) Contains(offset Offset) bool {
	return w.Count > 0 && offset >= w.First && offset <= w.Last
}
func (w OffsetWindow) Lag(committed Offset) int64 {
	if w.Count == 0 || committed >= w.Last {
		return 0
	}
	return int64(w.Last - committed)
}
