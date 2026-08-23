# Operations Runbook

Check `/healthz` and `/readyz`; inspect JSON logs for duration and error code. On quota pressure inspect tenant usage and lower producer rate. On restart, segment recovery scans framed records and truncates only an incomplete tail. Replica promotion requires ISR membership and a new epoch. Compaction and retention are safe to rerun and stop through context cancellation. Backups include segments, manifest and checkpoints; restore into a new directory and CRC-scan before switching traffic.
