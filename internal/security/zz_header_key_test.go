package security

import "testing"

func TestRedactHeadersSkipsEmptyKey(t *testing.T) {
	got := RedactHeaders(map[string]string{"": "value", "token": "secret"})
	if _, ok := got[""]; ok { t.Fatal("empty header key retained") }
	if got["token"] != "[REDACTED]" { t.Fatal("token was not redacted") }
}
