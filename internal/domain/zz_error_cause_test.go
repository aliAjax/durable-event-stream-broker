package domain

import (
	"errors"
	"testing"
)

func TestBrokerErrorCauseChain(t *testing.T) {
	sentinel := errors.New("disk")
	if !errors.Is(Wrap(ErrUnavailable, "storage", sentinel), sentinel) { t.Fatal("cause chain lost") }
	wrapped := Wrap(ErrNotFound, "topic", sentinel)
	if CodeOf(wrapped) != ErrNotFound || MessageOf(wrapped) != "topic" { t.Fatal("broker error metadata lost") }
}
