# Bug Reproduction

## Bug

Header sanitization writes into nil or caller-owned maps and retains empty header names. Related configuration and quota boundaries accept negative or oversized values, allowing invalid limits to enter runtime state.

## Trigger

Run the targeted nil-input and configuration-boundary tests:

```bash
go test ./internal/g/../security/g7/.. -run '^TestHeaderSanitizationNilInputs$' -count=1
go test ./internal/g/../config/g7/.. -run '^TestConfigRejectsNegativeQuota$' -count=1
go test ./internal/g/../config/g7/.. -run '^TestConfigCapsSegmentSize$' -count=1
go test ./internal/g/../security/g7/.. -run '^TestRedactHeadersSkipsEmptyKey$' -count=1
go test ./internal/g/../domain/g7/.. -run '^TestHeaderSizeIgnoresEmptyKey$' -count=1
go test ./internal/g/../config/g7/.. -run '^TestConfigCapsQuotaSize$' -count=1
```

## Error

The failing baseline includes:

```text
panic: assignment to entry in nil map
zz_segment_limit_test.go:7: oversized segment accepted
invalid quota and empty-header boundary cases were accepted
```
