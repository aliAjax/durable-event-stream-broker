# Failure Drills

- Reopen a truncated segment and verify valid records survive while the bad tail is removed.
- Retry an identical `Idempotency-Key` and verify the offset does not advance.
- Commit an older offset and verify `CONFLICT`.
- Expire a member lease and verify `FENCED`.
- Promote an offline replica and verify `UNAVAILABLE`; reconcile offsets after a valid failover.
- Exhaust tenant quota or publish tokens and verify `QUOTA_EXCEEDED`.
