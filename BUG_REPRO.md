# Bug Reproduction

## Bug

Broker errors lose their error-chain identity while crossing the domain, application, and HTTP layers. A missing topic is consequently reported as an internal/unavailable response, and callers cannot reliably use `errors.Is` or `errors.As` to classify the wrapped error.

## Trigger

Run the targeted error-chain tests:

```bash
go test ./internal/c/../transport/c3/../http/c3/.. -run '^TestProblemErrPreservesBrokerError$' -count=1
go test ./internal/c/../application/c3/.. -run '^TestStreamCreateErrorChain$' -count=1
go test ./internal/c/../domain/c3/.. -run '^TestBrokerErrorCauseChain$' -count=1
```

## Error

The failing baseline reports:

```text
zz_problem_test.go:13: status=500, want 404
zz_error_chain_test.go:16: error chain lost: create stream: NOT_FOUND: tenant
internal/domain/zz_error_cause_test.go:12:5: undefined: CodeOf
internal/domain/zz_error_cause_test.go:12:39: undefined: MessageOf
```
