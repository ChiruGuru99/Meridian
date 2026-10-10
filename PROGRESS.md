# Progress

Environment notes (for resuming):
- Docker, JDK 21, Maven, Go and make run inside **WSL Ubuntu** (`wsl -d Ubuntu`), with the repo mounted at `/mnt/c/Users/user/git/Meridian`. Windows has no Docker CLI.
- If `go build` complains about missing C headers in WSL, use `CGO_ENABLED=0` (libc6-dev may not be installed yet).
- Ports: gateway 8080, gateway admin 8082, ledger 8081, recon 8083, console 3000, grafana 3001, prometheus 9090, jaeger 16686, postgres 5432, kafka 19092.
- The Ubuntu distro is terminated (dockerd stopped) a few seconds after the last `wsl.exe` session exits. Keep one session open: `wsl -d Ubuntu -- sleep infinity` in the background.
- Stop the compose stack before `mvn verify` on this 4 GB VM; Testcontainers starts its own Postgres and Redpanda.

| Phase | Status | Verified by |
|-------|--------|-------------|
| 1. Design + scaffold + compose + CI | done | `DESIGN.md`; `docker compose up -d --wait` brings all 10 containers healthy |
| 2. Thin vertical slice | done | `make e2e` → `TestTransferEndToEnd` PASS (gateway → gRPC → ledger → Postgres); `/api/invariants` ok |
| 3. Ledger complete + invariant/property tests | done | `mvn verify`: 27 tests green (6 unit, 16 Testcontainers integration incl. raw-SQL attacks, 3 jqwik properties incl. model-based, 4,000-transfer/32-thread concurrency test, outbox→Redpanda relay test); stack rebuilt, e2e green |
| 4. Gateway: payments, idempotency, state machine | done | `go test ./...`: state-machine table tests + rapid property; HTTP integration tests on Testcontainers Postgres+Redis with a fault-injecting fake ledger (lifecycle, decline, void, replay, key reuse, 30 racing requests → one effect, ledger outage → recovery, lost response → no double post, lock takeover, ownership, strict decoding, rate limit + fail-open, rapid random ops with retries and faults). Mutation check: a non-deterministic ledger key is caught by 2 tests. e2e: 3 tests green on the rebuilt stack |
| 5. Outbox, events, saga, idempotent consumers | done | gateway relay test on Testcontainers Postgres+Redpanda (pending while broker down, published only after ack, traceparent carried); ledger relay test (phase 3); inbox dedupe tests (phase 4); e2e `TestPayoutSagaEndToEnd`: payout settles via rail-sim over Kafka, IBAN *9999 is rejected (AC04) and compensated, then rail results are re-delivered twice plus a forged contradictory result through Kafka with no state or balance change |
| 6. Reconciliation + statement simulator | next | |
| 7. Observability | | |
| 8. Concurrency, failure, load tests + PERFORMANCE.md | | |
| 9. ops-console | | |
| 10. Seed + `make demo` | | |
| 11. Clean-clone check | | |

Observations to revisit in phase 8:
- The ledger concurrency test reached ~190 transfers/s on 5 hot accounts (32 threads). Balance row locks are held across the posting and outbox inserts; taking the locks last should shorten the critical section.
