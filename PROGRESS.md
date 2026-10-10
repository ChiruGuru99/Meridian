# Progress

Environment notes (for resuming):
- Docker, JDK 21, Maven, Go and make run inside **WSL Ubuntu** (`wsl -d Ubuntu`), with the repo mounted at `/mnt/c/Users/user/git/Meridian`. Windows has no Docker CLI.
- If `go build` complains about missing C headers in WSL, use `CGO_ENABLED=0` (libc6-dev may not be installed yet).
- Ports: gateway 8080, gateway admin 8082, ledger 8081, recon 8083, console 3000, grafana 3001, prometheus 9090, jaeger 16686, postgres 5432, kafka 19092.

| Phase | Status | Verified by |
|-------|--------|-------------|
| 1. Design + scaffold + compose + CI | done | `DESIGN.md`; `docker compose up -d --wait` brings all 10 containers healthy |
| 2. Thin vertical slice | done | `make e2e` → `TestTransferEndToEnd` PASS (gateway → gRPC → ledger → Postgres); `/api/invariants` ok |
| 3. Ledger complete + invariant/property tests | next | |
| 4. Gateway: payments, idempotency, state machine | | |
| 5. Outbox, events, saga, idempotent consumers | | |
| 6. Reconciliation + statement simulator | | |
| 7. Observability | | |
| 8. Concurrency, failure, load tests + PERFORMANCE.md | | |
| 9. ops-console | | |
| 10. Seed + `make demo` | | |
| 11. Clean-clone check | | |
