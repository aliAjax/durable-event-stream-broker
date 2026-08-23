package repository

import (
	"testing"
	"time"

	"github.com/example/persistent-event-stream-broker/internal/domain"
)

func seededRepository(t *testing.T) *Repository {
	t.Helper()
	repo := New()
	if err := repo.CreateTenant(domain.NewTenant("tenant-a", "original tenant", 4096)); err != nil {
		t.Fatal(err)
	}
	return repo
}

func topicFixture() *domain.Topic {
	topic := domain.NewTopic("topic-a", "tenant-a", "original topic", 1)
	topic.Partitions[0].Records = []domain.Record{{
		Offset: 1, Key: "key-a", Value: []byte("value-a"),
		Headers: map[string]string{"source": "original"}, Timestamp: time.Unix(10, 0),
	}}
	return topic
}

func TestRepositoryOwnsCreatedTenant(t *testing.T) {
	repo := New()
	input := domain.NewTenant("tenant-a", "original tenant", 4096)
	if err := repo.CreateTenant(input); err != nil {
		t.Fatal(err)
	}
	input.Name = "mutated"
	input.QuotaBytes = 1

	stored, err := repo.Tenant("tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	if stored.Name != "original tenant" || stored.QuotaBytes != 4096 {
		t.Fatalf("created tenant aliases caller input: %+v", stored)
	}
}

func TestRepositoryTenantReadIsSnapshot(t *testing.T) {
	repo := seededRepository(t)
	first, err := repo.Tenant("tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	first.Name = "mutated"
	first.UsedBytes = 3000

	second, err := repo.Tenant("tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	if second.Name != "original tenant" || second.UsedBytes != 0 {
		t.Fatalf("tenant read leaked repository state: %+v", second)
	}
}

func TestRepositoryOwnsCreatedTopic(t *testing.T) {
	repo := seededRepository(t)
	input := topicFixture()
	if err := repo.CreateTopic(input); err != nil {
		t.Fatal(err)
	}
	input.Name = "mutated"
	input.Partitions[0].Records[0].Value[0] = 'X'
	input.Partitions[0].Records[0].Headers["source"] = "mutated"

	stored, err := repo.Topic("topic-a")
	if err != nil {
		t.Fatal(err)
	}
	record := stored.Partitions[0].Records[0]
	if stored.Name != "original topic" || string(record.Value) != "value-a" || record.Headers["source"] != "original" {
		panic("created topic aliases caller input")
	}
}

func TestRepositoryTopicReadIsSnapshot(t *testing.T) {
	repo := seededRepository(t)
	if err := repo.CreateTopic(topicFixture()); err != nil {
		t.Fatal(err)
	}
	first, err := repo.Topic("topic-a")
	if err != nil {
		t.Fatal(err)
	}
	first.Partitions[0].Records[0].Value[0] = 'X'
	first.Partitions[0].Records[0].Headers["source"] = "mutated"

	second, err := repo.Topic("topic-a")
	if err != nil {
		t.Fatal(err)
	}
	record := second.Partitions[0].Records[0]
	if string(record.Value) != "value-a" || record.Headers["source"] != "original" {
		t.Fatalf("topic read leaked repository state: %+v", record)
	}
}

func TestRepositoryTopicListIsSnapshot(t *testing.T) {
	repo := seededRepository(t)
	if err := repo.CreateTopic(topicFixture()); err != nil {
		t.Fatal(err)
	}
	listed := repo.ListTopics("tenant-a")
	if len(listed) != 1 {
		t.Fatalf("listed %d topics", len(listed))
	}
	listed[0].Name = "mutated"
	listed[0].Partitions[0].Records[0].Value[0] = 'X'

	stored, err := repo.Topic("topic-a")
	if err != nil {
		t.Fatal(err)
	}
	if stored.Name != "original topic" || string(stored.Partitions[0].Records[0].Value) != "value-a" {
		t.Fatalf("topic list leaked repository state: %+v", stored)
	}
}
