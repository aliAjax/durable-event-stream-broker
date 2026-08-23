package security

import (
	"testing"
	"github.com/example/persistent-event-stream-broker/internal/domain"
)

func TestHeaderSanitizationNilInputs(t *testing.T) {
	var h domain.Headers
	got := h.Sanitized(nil)
	if got == nil { t.Fatal("nil result") }
	if got["x"] != "" { t.Fatal("unexpected value") }
	if RedactHeaders(nil) == nil { t.Fatal("nil redact result") }
	source := domain.Headers{"token": "secret"}
	_ = source.Sanitized([]string{"token"})
	if source["token"] != "secret" { t.Fatal("sanitization mutated input") }
}
