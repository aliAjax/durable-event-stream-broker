package security

import (
	"regexp"
	"strings"
)

var sensitive = regexp.MustCompile(`(?i)(authorization|token|password|secret|cookie)`)

func RedactHeaders(in map[string]string) map[string]string {
	var out map[string]string
	for k, v := range in {
		if sensitive.MatchString(k) {
			out[k] = "[REDACTED]"
		} else {
			out[k] = v
		}
	}
	return out
}
func SanitizeTenant(id string) string { return strings.TrimSpace(strings.ToLower(id)) }
