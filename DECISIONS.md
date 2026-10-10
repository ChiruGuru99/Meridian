# Decisions

Each entry gives the choice, the alternatives considered, and the reason. Entries are in the order they were made.

### D1. Signed single-column postings (debit > 0, credit < 0) with normal-balance presentation
**Alternatives:** separate `debit`/`credit` columns; one sign convention with no account types (TigerBeetle-style debits/credits per account).
**Why:** With a single signed column, the core invariant becomes one SQL aggregate (`sum(amount) = 0` per transaction and currency), which a deferred trigger can enforce cheaply. Account types (ASSET/LIABILITY/…) set the normal balance, so stored balances read the way an accountant and a customer expect: a customer holding €100 shows `+10000`, not `-10000`. A reviewer from a bank can map every row to Dr/Cr without a translation table.

### D2. Ledger is a generic posting engine; the gateway owns payment accounting
**Alternatives:** semantic ledger RPCs (`Authorise`, `Capture`, …) inside the ledger.
**Why:** The ledger's job is to make bad money movements impossible: unbalanced entries, overdrafts, rewrites. That job does not change when the product changes. Keeping payment semantics in the gateway means a new product (for example FX or fees) needs no ledger release, and the ledger's test surface stays small and exhaustive. The cost is that the gateway must build correct postings. That code is isolated in `paymentPostings` and covered by end-to-end balance assertions.

### D3. Pessimistic row locks in sorted key order for balances
**Alternatives:** optimistic concurrency (`version` column with retry); a single serialized writer per account (actor/queue); SERIALIZABLE isolation.
**Why:** Payment workloads have hot accounts (merchants, system clearing accounts). Under optimistic concurrency, contention on a hot account becomes a retry storm. With `SELECT … FOR UPDATE`, contention becomes a short queue, and acquiring locks in `(account_id, currency)` order makes deadlocks impossible by construction. The `version` column is still incremented, so a future read-modify-write API could use it. SERIALIZABLE would also be correct, but it pushes retries onto every caller.

### D4. Database constraints as the last line of defence
**Alternatives:** application-only validation.
**Why:** Application checks give good error messages. Database constraints guarantee the invariants even against a future bug, a manual SQL session or a new service writing to the same tables. The ledger uses `CHECK (balance >= -overdraft_limit)`, a deferred constraint trigger for balanced transactions, triggers that reject UPDATE, DELETE and TRUNCATE on postings and transactions, and a separate `ledger_app` role without UPDATE or DELETE grants. Every one of these has a test that bypasses the application and gets rejected.

### D5. Idempotency stored in Postgres (not Redis), deterministic ledger keys
**Alternatives:** Redis `SETNX` for idempotency keys; storing only the response without a link to the resource.
**Why:** An idempotency record must be exactly as durable as the money movement it protects. Redis eviction or failover could lose a key and allow a double charge. The gateway claims the key in the same Postgres transaction that creates the resource. Every ledger call then uses a key derived from (resource id, step), for example `payment:{id}:capture`. If the gateway crashes after the ledger call, a retry or the recovery loop repeats the call, and the ledger returns the original transaction. This gives exactly one financial effect without distributed transactions.

### D6. In-flight states plus a recovery loop instead of a workflow engine
**Alternatives:** Temporal or Cadence; a generic saga framework; a message-driven choreography.
**Why:** The saga has three steps and one compensation. A workflow engine would add a cluster to operate and a programming model to learn, while hiding the very mechanics this project is meant to show. Every in-flight state (`AUTHORISING`, `RESERVING`, `SETTLING`, `COMPENSATING`, …) has exactly one idempotent driver. The request handler calls it inline, and a recovery loop calls it again for rows stuck longer than 5 s. The whole orchestration fits in `transfers.go`.

### D7. Transactional outbox with a polling relay, retained rows as the audit timeline
**Alternatives:** CDC with Debezium; publishing directly after commit; deleting outbox rows after publishing.
**Why:** Polling with `FOR UPDATE SKIP LOCKED` needs no extra infrastructure, supports several relay replicas, and gives at-least-once delivery with per-key ordering, because the relay stops at the first failed send. Debezium would cut publish latency from about 100 ms to tens of ms, but it adds Kafka Connect to run. The rows are kept, which gives every payment a free, transactionally consistent event timeline for the ops console. At production volume they would be partitioned by month and archived.

### D8. Idempotent consumers via an inbox table (gateway) and natural keys (reconciliation)
**Alternatives:** Kafka exactly-once transactions.
**Why:** Kafka EOS only covers Kafka-to-Kafka flows. Our consumers' effects land in Postgres. Recording the message id in the same database transaction as the effect, and committing offsets only afterwards, gives effectively-once processing that survives rebalances and redeliveries.

### D9. Simulated rail as a separate service on Kafka
**Alternatives:** a synchronous HTTP call to a fake bank inside the saga.
**Why:** Real rails such as SEPA and Faster Payments accept an instruction and confirm it asynchronously. Modelling the rail asynchronously over Kafka makes the saga realistic, because the payout really is "in flight" at the rail. It also gives the broker-outage failure test something to stall. The rail deduplicates instructions by transfer id in Redis and replays its stored answer, so it never "pays twice".

### D10. Rate limiting fails open
**Alternatives:** fail closed (reject all traffic when Redis is unavailable).
**Why:** Rate limiting protects capacity. It does not protect correctness: idempotency and ledger constraints do that. Rejecting every payment because a cache is down would turn a degraded dependency into a full outage. Limiter errors are counted (`gateway_rate_limiter_errors_total`) so the alerting can page.

### D11. Clients may only debit accounts they opened
**Alternatives:** no ownership model; full RBAC.
**Why:** Without an ownership check, any API key could move any customer's money. That is a broken access control at the trust boundary. The minimal model (`client_accounts`: the opener owns the account; only owners can be the payer, transfer source or deposit target) closes it without building an IAM system. System accounts can never be named as a source.

### D12. One Postgres instance, one database and role per service
**Alternatives:** one shared schema; one Postgres container per service.
**Why:** Separate databases and credentials keep each service's data private: there are no cross-service joins, and the ledger is reachable only through its API. One instance keeps the laptop footprint small (the dev VM has 4 GB). Splitting into separate instances later is a connection-string change.

### D13. Spring Boot 4.1 with JdbcClient (no JPA) and grpc-java directly
**Alternatives:** Spring Boot 3.5 (out of OSS support by mid-2026); JPA/Hibernate; the Spring gRPC starter.
**Why:** A ledger is a small amount of very important SQL, and it should read as SQL, with explicit locking, `ON CONFLICT` and `RETURNING`. An ORM's dirty checking and flush ordering are a liability here. grpc-java takes about 40 lines to host inside the Spring lifecycle and has no extra abstraction to learn.

### D14. Go standard library HTTP routing, pgx, franz-go, client_golang
**Alternatives:** chi, gin or echo; sqlc or GORM; sarama or confluent-kafka-go (cgo).
**Why:** Go 1.22+ `ServeMux` has method and path-parameter routing, so a router dependency adds nothing. pgx is the de-facto Postgres driver. franz-go is pure Go (no cgo), well maintained, and supports idempotent producers and manual offset commits. Prometheus' native client is the smallest path to RED metrics.

### D15. Phase 1 scaffold verified together with the phase 2 slice
**Why:** The scaffold's `docker-compose.yml` was written with the slice services already in it, so the first `docker compose up` that could pass was the slice's. Both were verified in the same run: all containers healthy, `TestTransferEndToEnd` green. They were then committed as two commits for history.
