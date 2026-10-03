# Fstack

Fstack is an open-core control plane for creating and operating isolated, branded AI businesses.

The public core owns:

- organization, business, environment, workload, deployment and binding models;
- PostgreSQL migrations and tenant-isolation invariants;
- durable, idempotent provisioning operations and audit events;
- provider-neutral workload adapter contracts;
- local/self-hosted API and worker entry points;
- standard OpenTelemetry conventions and OTLP integration boundaries;
- export/import and installation-local administration contracts.

Crusher is the first managed workload used to prove these abstractions. Remesher will be the second proof before Fstack generalizes its adapter surface further.

## Boundary

Managed fleet orchestration, hosted credential brokerage, billing operations, cross-installation support tooling and production automation are not part of this repository. They live in the private `fstack-cloud` repository and integrate through public Fstack contracts.

Fstack uses PostgreSQL exclusively for persisted state. There is no SQLite runtime or test path.

## First vertical slice

The initial delivery accepts an idempotent request for a branded business and atomically records its organization, default production environment, Crusher workload, desired deployment, secret references, provisioning operation and audit event. External infrastructure execution follows as a separately claimed durable operation; an API retry must never create a duplicate business or side effect.

See [`docs/architecture/first-crusher-slice.md`](docs/architecture/first-crusher-slice.md) for the frozen acceptance contract.

## Development

Requirements: Python 3.12+, `uv`, Docker and PostgreSQL 17.

```bash
uv sync --frozen
export FSTACK_DATABASE_URL=postgresql+psycopg://fstack:password@localhost:5432/fstack
export FSTACK_OPERATOR_TOKEN=replace-with-a-long-random-token
uv run alembic upgrade head
uv run uvicorn fstack.main:app --reload
```

Persistence tests require an explicit PostgreSQL URL; they never fall back to SQLite:

```bash
FSTACK_TEST_DATABASE_URL=postgresql://fstack:password@localhost:5432/fstack_test \
  uv run pytest -q
```
