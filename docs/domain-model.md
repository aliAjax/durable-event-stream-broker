# Domain Model

Tenant owns byte/rate quotas. Topic belongs to one tenant and contains fixed partitions. A partition is an append-only ordered aggregate whose offset increases monotonically. Producer sequence and idempotency key suppress retried batches. ConsumerGroup owns member leases, generation, assignment and committed offsets. ReplicaSet owns leader epoch, ISR and fencing decisions. Transactions transition from open to committed, aborted or expired.
