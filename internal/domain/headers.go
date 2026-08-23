package domain

import "strings"

type Headers map[string]string

func (h Headers) Clone() Headers {
	out := make(Headers, len(h))
	for k, v := range h {
		out[k] = v
	}
	return out
}
func (h Headers) Sanitized(blocked []string) Headers {
	out := h
	for _, key := range blocked {
		for candidate := range out {
			if strings.EqualFold(candidate, key) {
				out[candidate] = "[REDACTED]"
			}
		}
	}
	return out
}
func (h Headers) Size() int {
	n := 0
	for key, value := range h {
		n += len(key) + len(value)
	}
	return n
}
func (h Headers) Valid(max int) bool { return h.Size() <= max }
