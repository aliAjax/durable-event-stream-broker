package service

import (
	"context"
	"testing"
	"time"
	"github.com/example/persistent-event-stream-broker/internal/domain"
	"github.com/example/persistent-event-stream-broker/internal/storage/repository"
)

func TestAppendCancellationStopsMutation(t *testing.T) {
	r := repository.New()
	b := NewBroker(r)
	if err := b.CreateTenant("tenant", "Tenant", 1024); err != nil { t.Fatal(err) }
	if _, err := b.CreateTopic("tenant", "topic", "topic", 1); err != nil { t.Fatal(err) }
	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(time.Millisecond, cancel)
	_, err := b.Append(ctx, "tenant", "topic", 0, []domain.Record{{Value: []byte("x")}}, "p", 1, "")
	if err == nil { t.Fatal("canceled append succeeded") }
	got, _ := b.Fetch("topic", 0, -1, 10)
	if len(got) != 0 { t.Fatalf("canceled append wrote %d records", len(got)) }
}
