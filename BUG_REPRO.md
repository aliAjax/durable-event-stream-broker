# Bug Reproduction

## Bug

Concurrent quota decisions, histogram snapshots, and audit events expose mutable shared state outside their synchronization boundaries. This produces data races, retry delays inconsistent with admission decisions, snapshots that change after return, and audit fields that can be mutated after hashing.

## Trigger

Run the targeted race and snapshot tests:

```bash
go test -race ./internal/h/../quota/h8/.. -run '^TestQuotaAndMetricsConcurrentAccess$' -count=1
go test -race ./internal/h/../observability/h8/.. -run '^TestAuditSnapshotCopy$' -count=1
go test -race ./internal/h/../observability/h8/.. -run '^TestHistogramConcurrentSnapshot$' -count=1
```

## Error

The baseline produces race-detector reports for concurrent reads and writes to bucket tokens, histogram slices, and audit field maps. Returned histogram and audit snapshots also change when later writes mutate their shared backing data.
