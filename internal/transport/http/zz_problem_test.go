package httptransport

import (
	"fmt"
	"net/http/httptest"
	"testing"
	"github.com/example/persistent-event-stream-broker/internal/domain"
)

func TestProblemErrPreservesBrokerError(t *testing.T) {
	r := httptest.NewRecorder()
	problemErr(r, fmt.Errorf("wrapped: %w", domain.E(domain.ErrNotFound, "topic")))
	if r.Code != 404 { t.Fatalf("status=%d, want 404", r.Code) }
}
