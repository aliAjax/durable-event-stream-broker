# Bug Reproduction

## Bug

The in-memory repository does not own the tenant and topic objects stored at its create boundary, and its tenant, topic, and topic-list reads expose the same mutable objects. Caller changes to names, quota fields, partition records, payload bytes, or headers therefore alter later repository reads across requests.

## Trigger

Run the five targeted repository ownership checks against the red branch. They mutate objects after creation and mutate values returned by single-item and list reads, then read the same repository state again.

## Error

The buggy baseline reports:

```text
TestRepositoryOwnsCreatedTenant: created tenant aliases caller input
TestRepositoryTenantReadIsSnapshot: tenant read leaked repository state
TestRepositoryOwnsCreatedTopic: panic: created topic aliases caller input
TestRepositoryTopicReadIsSnapshot: topic read leaked repository state
TestRepositoryTopicListIsSnapshot: topic list leaked repository state
```

All five checks exit non-zero on the original repository implementation.
