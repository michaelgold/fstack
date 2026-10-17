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

The initial delivery accepts an idempotent request for a branded business and atomically records its organization, default production environment, Crusher workload, desired deployment, secret references, provisioning operation, audit event and Temporal dispatch intent. The Temporal worker drains that transactional outbox, starts deterministic workflow `provisioning/<operation UUID>`, prepares one idempotent runtime submission, applies it to local Docker, verifies migration plus database and HTTP readiness, and atomically finalizes PostgreSQL.

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

Temporal is the durable provisioning workflow engine. Run a local Temporal service, then
start the Fstack provisioning worker and transactional outbox dispatcher with:

```bash
export FSTACK_TEMPORAL_TARGET=127.0.0.1:7233
export FSTACK_TEMPORAL_NAMESPACE=default
export FSTACK_TEMPORAL_TASK_QUEUE=fstack-provisioning
export FSTACK_DOCKER_NETWORK=fstack-local
export FSTACK_LOCAL_SECRET_ROOT=/var/lib/fstack/source-secrets
export FSTACK_SECRET_STAGE_ROOT=/var/lib/fstack/runtime-secrets
uv run python -m fstack.temporal_worker
```

Each operation uses the deterministic Workflow ID `provisioning/<operation UUID>`. The
worker retries pending dispatch intents until Temporal accepts that deterministic start.
Only `secret://local-file/...` database references are resolved by the local Docker slice.
Source and staged JSON files must satisfy the private ownership/mode contract documented in
the Temporal architecture. Object-storage references remain sealed desired state only.
See [`docs/architecture/temporal-provisioning.md`](docs/architecture/temporal-provisioning.md)
for the durability and ownership boundary.

### Installation-managed local Caddy ingress

Fstack does not install, upgrade, or own Caddy. The installation must provide one Docker
container running the release-certified Caddy image, an existing `fstack` HTTP server with a
`routes` array and installation-owned listener, a shared Unix admin socket, and network
connectivity. Configure the following fixed worker environment values; none is deployment
request data:

```bash
export FSTACK_CADDY_ADMIN_ROOT=/replace/with/trusted-caddy-admin-root
export FSTACK_CADDY_ADMIN_UID=replace-with-socket-owner-uid
export FSTACK_CADDY_ADMIN_GID=replace-with-socket-group-gid
export FSTACK_CADDY_PROBE_ADDRESS=replace-with-fixed-caddy-network-ip:replace-with-listener-port
export FSTACK_CADDY_CONTAINER_NAME=replace-with-canonical-caddy-container-name
```

The socket entry is fixed as `caddy-admin.sock`. It must be a Unix socket owned by the
configured UID/GID with mode exactly `0220`; every other mode is rejected. The worker must
have the configured GID as a supplemental group. The trusted admin root must be owned by
root or the worker EUID and must never be writable by group or other. Caddy admin access
requires Linux descriptor/inotify primitives and `/proc/self/fd`; unsupported hosts fail
closed. Do not expose an unauthenticated Caddy TCP admin endpoint.

Caddy and deterministic Crusher containers must join the exact `FSTACK_DOCKER_NETWORK`.
Caddy must resolve the Crusher container name and reach port `8000`; the worker must reach
`FSTACK_CADDY_PROBE_ADDRESS` over the installation network, not worker loopback. Share the
admin socket separately through a confined volume. Before reconciliation, Fstack verifies
that both containers are running on the expected network, the Caddy network IP equals the
probe IP, the Crusher endpoint exposes its deterministic name or alias, and the listener
accepts TCP connections.

The first compatibility baseline is Caddy **v2.11.7 at the exact image digest recorded by
the release canary**. Use that certified version/digest until another pair is re-certified.
Do not infer support from a tag or major version, and do not copy a guessed digest from
documentation.

A successful operation means both runtime and ingress were durably finalized. Routed HTTP
readiness proves only the configured Caddy-to-Crusher path; it does not prove DNS, TLS,
certificate lifecycle, or Internet reachability. See
[`docs/architecture/local-caddy-ingress-slice.md`](docs/architecture/local-caddy-ingress-slice.md)
for the complete contract and exclusions.

Host Caddy may use the installation-local, bodyless
`GET /internal/caddy/ask?domain=<canonical-hostname>` decision endpoint for on-demand TLS.
It returns success only for a deployment whose joint provisioning operation durably
succeeded. The public API ingress must not expose this internal path. See
[`docs/architecture/provisioned-host-certificate-authorization.md`](docs/architecture/provisioned-host-certificate-authorization.md)
for the authorization and host-Caddy wiring contract.

### First managed billing plane

Managed installations may require a current provider-neutral entitlement before accepting a
new Crusher provisioning operation. The first plan permits one active Crusher deployment and
emits one immutable deployment-activation usage fact at joint runtime-and-ingress success.
Self-hosted installations retain an explicit billing-disabled mode. Commercial policy, prices,
Lago identifiers, usage export and reconciliation remain private `fstack-cloud` concerns. See
[`docs/architecture/first-billing-plane-slice.md`](docs/architecture/first-billing-plane-slice.md)
for the frozen ownership, outage, quota and evidence contract.

Persistence tests require an explicit PostgreSQL URL; they never fall back to SQLite:

```bash
FSTACK_TEST_DATABASE_URL=postgresql://fstack:password@localhost:5432/fstack_test \
  uv run pytest -q
```
