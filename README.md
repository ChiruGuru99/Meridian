# Meridian

Meridian is a simulated payments platform built around a double-entry ledger. It demonstrates payment authorisation, capture and refunds, internal transfers, asynchronous payouts with compensation, transactional outbox delivery, idempotent recovery, and bank reconciliation.

> **Simulation only.** Meridian does not connect to a real bank or payment rail and must not be used to process real money or customer data.

## Components

| Component | Stack | Responsibility |
|---|---|---|
| `ledger-service` | Java 21, Spring Boot, PostgreSQL | Balanced postings, account balances, immutable entries, reversals and ledger outbox |
| `payment-gateway` | Go, PostgreSQL, Redis, Kafka | Public payments API, state machines, idempotency, rate limiting and payout sagas |
| `rail-sim` | Go, Kafka, Redis | Simulated asynchronous payout rail |
| `reconciliation` | Python, PostgreSQL, Kafka | Ledger event ingestion, bank statement simulation and reconciliation |
| `infra` | OpenTelemetry, Prometheus, Grafana, Jaeger | Metrics, dashboards and tracing |

See [DESIGN.md](DESIGN.md) for invariants, APIs, state machines and failure behavior. Implementation progress and architectural choices are documented in [PROGRESS.md](PROGRESS.md) and [DECISIONS.md](DECISIONS.md).

## Prerequisites

- Docker with Compose v2
- GNU Make
- JDK 21 and Maven
- Go 1.26+
- Python 3.12+

## Run the stack

```sh
make up
```

The public gateway is available at `http://localhost:8080`. Grafana, Prometheus and Jaeger are published at `http://localhost:3001`, `http://localhost:9090` and `http://localhost:16686`.

The gateway admin API (`8082`), ledger HTTP API (`8081`) and reconciliation API (`8083`) are bound to loopback by the Compose configuration.

Stop the stack without deleting data:

```sh
make down
```

Delete all local data and volumes:

```sh
make clean
```

## Authentication

Public API requests use:

```http
Authorization: Bearer <api-key>
Idempotency-Key: <unique-key>
```

The Compose development client keys are:

- `demo-key-6f1c2a9e7b3d4f58`
- `load-key-93ab1c7d5e2f4a60`

The gateway admin, ledger gRPC/HTTP, and reconciliation read APIs use separate development-only credentials configured in `docker-compose.yml`. Override `ADMIN_API_KEY`, `LEDGER_GRPC_API_KEY`, `LEDGER_HTTP_API_KEY` and `RECON_API_KEY` before deploying anywhere beyond local development.

Funding enters through the authenticated admin endpoint:

```http
POST http://localhost:8082/ops/deposits
Authorization: Bearer <admin-key>
Idempotency-Key: <unique-key>
Content-Type: application/json

{
  "account_id": "acc_...",
  "amount": {"amount_minor": 10000, "currency": "EUR"},
  "reference": "simulated bank notification"
}
```

The public client API intentionally does not expose deposit creation.

## API overview

Public gateway endpoints:

- `POST /v1/accounts`
- `GET /v1/accounts/{id}`
- `POST /v1/payments`
- `POST /v1/payments/{id}/authorise`
- `POST /v1/payments/{id}/capture`
- `POST /v1/payments/{id}/cancel`
- `POST /v1/payments/{id}/refunds`
- `GET /v1/payments/{id}`
- `POST /v1/transfers`
- `GET /v1/transfers/{id}`

Payment authorization is operation-specific: payer credentials may authorise or cancel, while payee credentials may capture or refund. Clients may debit only accounts they own.

## Test

```sh
make test
```

Run the subsystem suites independently:

```sh
make test-ledger
make test-gateway
make test-recon
```

With the Compose stack running:

```sh
make e2e
```

Pull requests are checked by the test suites, CodeQL code scanning, and GitHub's
dependency review. Dependabot monitors GitHub Actions, Maven, Go, Python, and
container dependencies.

## Reconciliation

Generate a simulated statement and run reconciliation:

```sh
make recon
```

The generator deliberately injects discrepancies and writes a truth file next to the statement. The reconciliation job persists opening, movement and closing totals plus each classified break.

## Current scope

The operations console, chaos suite, demo seed/load tooling and performance phase are not implemented yet. CI covers the ledger, gateway, reconciliation and end-to-end flows that exist in this repository.

## License

Licensed under the [MIT License](LICENSE).
