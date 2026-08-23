#!/usr/bin/env bash
set -euo pipefail
base="${BROKER_URL:-http://127.0.0.1:8080}"
curl -fsS "$base/healthz" >/dev/null
curl -fsS -XPOST "$base/api/v1/tenants" -H 'content-type: application/json' -d '{"id":"smoke","name":"Smoke","quota_bytes":1000000}' >/dev/null
curl -fsS -XPOST "$base/api/v1/streams" -H 'content-type: application/json' -d '{"tenant_id":"smoke","id":"smoke-events","name":"smoke-events","partitions":1}' >/dev/null
curl -fsS -XPOST "$base/api/v1/topics/smoke-events/records:append" -H 'content-type: application/json' -H 'Idempotency-Key: smoke-1' -d '{"tenant_id":"smoke","partition":0,"producer_id":"smoke-producer","sequence":0,"records":[{"key":"key","value":"c21va2U="}]}' >/dev/null
curl -fsS "$base/api/v1/topics/smoke-events/records?partition=0&after_offset=-1" >/dev/null
echo "smoke ok"
