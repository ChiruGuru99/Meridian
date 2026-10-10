# Progress

Environment notes (for resuming):
- Docker, JDK 21, Maven, Go and make run inside **WSL Ubuntu** (`wsl -d Ubuntu`), with the repo mounted at `/mnt/c/Users/user/git/Meridian`. Windows has no Docker CLI.
- If `go build` complains about missing C headers in WSL, use `CGO_ENABLED=0` (libc6-dev may not be installed yet).
- Ports: gateway 8080, gateway admin 8082, ledger 8081, recon 8083, console 3000, grafana 3001, prometheus 9090, jaeger 16686, postgres 5432, kafka 19092.
- Stop the compose stack before `mvn verify` on this 4 GB VM; Testcontainers starts its own Postgres and Redpanda.

| Phase | Status | Verified by |
|-------|--------|-------------|
| 1. Design + scaffold + compose + CI | done | `DESIGN.md`; `docker compose up -d --wait` brings all 10 containers healthy |
| 2. Thin vertical slice | done | `make e2e` → `TestTransferEndToEnd` PASS (gateway → gRPC → ledger → Postgres); `/api/invariants` ok |
| 3. Ledger complete + invariant/property tests | done | `mvn verify`: 27 tests green (6 unit, 16 Testcontainers integration incl. raw-SQL attacks, 3 jqwik properties incl. model-based, 4,000-transfer/32-thread concurrency test, outbox→Redpanda relay test); stack rebuilt, e2e green |
| 4. Gateway: payments, idempotency, state machine | next | |
| 5. Outbox, events, saga, idempotent consumers | | |
| 6. Reconciliation + statement simulator | | |
| 7. Observability | | |
| 8. Concurrency, failure, load tests + PERFORMANCE.md | | |
| 9. ops-console | | |
| 10. Seed + `make demo` | | |
| 11. Clean-clone check | | |

Observations to revisit in phase 8:
- The ledger concurrency test reached ~190 transfers/s on 5 hot accounts (32 threads). Balance row locks are held across the posting and outbox inserts; taking the locks last should shorten the critical section.
