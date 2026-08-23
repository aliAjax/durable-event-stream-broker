# Bug Reproduction

## Bug

An append that is canceled while in progress still mutates the partition and keeps the tenant quota reservation. Tenant selection also ignores the preferred header when both header and query values are present, and negative reserve/release values corrupt usage accounting.

## Trigger

Exercise the append cancellation, tenant selection, and accounting boundary cases:

```bash
go test ./internal/e/../service/e5/.. -run '^TestAppendCancellationStopsMutation$' -count=1
go test ./internal/e/../domain/e5/.. -run '^TestTenantRejectsNegativeBudget$' -count=1
go test ./internal/e/../transport/e5/../http/e5/.. -run '^TestTenantHeaderTakesPrecedence$' -count=1
go test ./internal/e/../domain/e5/.. -run '^TestTenantIgnoresNegativeRelease$' -count=1
```

## Error

The failing baseline includes:

```text
zz_append_context_test.go:19: canceled append succeeded
zz_tenant_header_test.go:11: tenant header/query precedence was not honored
negative reserve/release changed tenant usage
```
