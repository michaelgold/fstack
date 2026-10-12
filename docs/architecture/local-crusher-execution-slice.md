# Local Crusher execution slice

Status: superseded on October 5, 2026 by the Temporal-only authority model in
[`temporal-provisioning.md`](temporal-provisioning.md). This document preserves the
rejected PostgreSQL lease-worker design for historical review only; it is not an
implementation contract.

## Product outcome

An installation operator can inspect a previously requested provisioning operation. A
single-run Fstack worker can claim one pending Crusher operation, run the requested
immutable Crusher image on the local Docker Engine, verify its database migration,
application-role database connectivity and container health, then durably report success
or a sanitized failure.

The slice proves one concrete Crusher/Docker path. It does not create a generic adapter
framework and does not provision PostgreSQL, object storage, DNS, ingress, billing or
cloud infrastructure.

## Authority and API

The existing installation operator bearer token remains the only authority. Operation
UUIDs are identifiers, not authorization.

Add:

```text
GET /v1/provisioning/operations/{operation_id}
```

The route authenticates before lookup and returns `Cache-Control: no-store`.

A successful response contains exactly these keys:

- `operation_id`, `organization_id`, `business_id`, `environment_id`, `workload_id` and
  `deployment_id` as UUIDs;
- `state`: `pending`, `running`, `succeeded` or `failed`;
- `created_at`, `started_at` and `completed_at` as RFC 3339 UTC timestamps or null;
- nonnegative `attempt_count`;
- `last_error`, exactly null or `{"code":"<sanitized-code>","retryable":<boolean>}`.

`replayed` is absent because it describes a POST response, not durable operation state.

It never returns request fingerprints, secret references or values, attempt IDs, worker
IDs, fencing tokens, Docker IDs, host paths or raw Docker output.

An unknown UUID returns:

```json
{"code":"operation_not_found","message":"operation was not found"}
```

with status `404`. Persistence failures use the existing sanitized
`persistence_failed` envelope.

## Database evolution

`20261003_0001` is released and byte-immutable. Add successor revision
`20261004_0002`.

### Provisioning operations

Extend `provisioning_operations` with:

- `updated_at timestamptz NOT NULL`;
- `started_at timestamptz NULL`;
- `completed_at timestamptz NULL`;
- `attempt_count bigint NOT NULL DEFAULT 0`;
- `current_attempt_id uuid NULL`;
- `last_error_code varchar(64) COLLATE "C" NULL`;
- `last_error_retryable boolean NULL`.

The upgrade backfills `updated_at = created_at`. `started_at` records the first-ever
transition to running and is not replaced by later retry attempts.

Replace the pending-only check with one fail-closed lifecycle check:

- `pending`: no current attempt and no completion time. A prior start time and retryable
  error are allowed after a retryable attempt.
- `running`: current attempt and start time required; completion and error absent.
- `succeeded`: no current attempt, completion required and error absent.
- `failed`: no current attempt, completion and a nonretryable error required.

`attempt_count >= 0`. Timestamps are monotonic and terminal rows cannot regress or change
identity/outcome fields. Allowed state transitions are:

```text
pending -> running
running -> pending | succeeded | failed
succeeded -> succeeded
failed -> failed
```

### Provisioning attempts

Create `provisioning_attempts` with:

- UUID primary key;
- direct `organization_id` and `operation_id`;
- positive `attempt_no` and `fencing_token` bigint values;
- `worker_id` UUID;
- state: `active`, `succeeded`, `failed` or `abandoned`;
- `lease_expires_at`, `created_at` and nullable `terminal_at` timestamps;
- nullable sanitized `error_code` and `retryable`.

PostgreSQL enforces tenant agreement to the operation, uniqueness of attempt number and
fencing token per operation, and at most one active attempt per operation. Attempts insert
only as active. An active attempt may become terminal once; terminal attempts are
immutable. The operation's current attempt uses a tenant-agreeing composite foreign key.

Attempt bundles are exact:

- `active`: no terminal time, error code or retryability;
- `succeeded`: terminal time required, no error code or retryability;
- `failed`: terminal time, sanitized error code and retryability required;
- `abandoned`: terminal time required, no error code or retryability.

At every committed boundary, a running operation has exactly one current attempt and it
is active; every active attempt is the current attempt of its running operation; pending
and terminal operations have no current attempt. Deferred constraint triggers enforce the
cross-row relationship while operation and attempt transition atomically.

### Concrete Docker effect

Create one concrete `crusher_docker_effects` row per operation/deployment with:

- UUID primary key and direct organization, operation and deployment IDs;
- deterministic lowercase hexadecimal `effect_key` and `spec_fingerprint`;
- deterministic API and migration container names;
- state: `prepared`, `started`, `confirmed` or `failed`;
- exact owner attempt ID and fencing token;
- nullable API container ID and sanitized failure code;
- prepared, started, confirmed and failed timestamps.

Expose uniqueness on operations for `(organization_id, id, deployment_id)`. The effect's
`(organization_id, operation_id, deployment_id)` references that tuple, proving the effect
belongs to the deployment named by the operation. PostgreSQL also enforces the direct
deployment tenant relation and exact attempt/fence, one effect per operation/deployment,
globally unique effect and container names, lowercase hexadecimal identities, and
coherent state/timestamp/result bundles.

Allowed transitions are:

```text
prepared -> started
prepared -> failed
started -> confirmed | failed
```

A prepared or started effect may transfer exactly to a strictly higher fencing token when
the previous owning attempt is abandoned, or failed with `retryable = true`, and the new
attempt is the operation's exact current active owner. Its deterministic identity and
sealed specification must remain unchanged. A confirmed effect is consumed read-only
without ownership transfer. Confirmed and failed effects are immutable. A prepared effect
may fail before container creation; that bundle requires `failed_at` and a sanitized
failure code but no container ID.

### Audit events

Permit exactly:

- `provisioning.requested`;
- `provisioning.started`;
- `provisioning.succeeded`;
- `provisioning.failed`.

Keep one event of each type per operation. Payloads contain only attempt numbers,
deterministic container names and sanitized error codes. Attempts, rather than audit rows,
record retries.

## Worker command and claim

Add a public command:

```text
python -m fstack.worker --once
```

One invocation processes at most one operation and exits successfully when no work is
claimable.

Claiming runs in one PostgreSQL transaction:

1. Select the oldest pending operation, or running operation whose active lease is
   expired, using `FOR UPDATE SKIP LOCKED`, ordered by creation time and UUID.
2. Lock the operation before attempt/effect rows.
3. After all required locks are held, sample a fresh PostgreSQL `clock_timestamp()`.
4. Re-read the exact operation state, current attempt and fencing token after locking. A
   recovery claim requires that same attempt to remain active and current with
   `lease_expires_at <= sampled_time`; otherwise skip it without writes. Only then mark
   the expired attempt abandoned.
5. Create a new active attempt with strictly increasing attempt number and fencing token,
   increment operation `attempt_count`, set the current attempt and transition to running.
6. Insert or adopt the one Docker effect, seal its exact execution spec and commit before
   secret-file or Docker access.

Lease duration is 120 seconds. The worker renews immediately before each mutation and at
least every 20 seconds while streaming an image pull or polling migration, container
health or readiness. Read-only polling occurs at most every five seconds. Heartbeat and
finalization lock operation then attempt,
sample a fresh database clock after locks are acquired, and require the exact current
worker, attempt and fence with `lease_expires_at > sampled_time`. This fences PostgreSQL
writes. Docker cannot enforce the token: the worker must validate its lease immediately
before image pull, container create/start, and Docker exec create/start; bound every
Docker call; issue no subsequent mutation after lease loss; and reconcile any ambiguous
or delayed result by deterministic name. An exec create and its exec start each receive a
fresh lease check when the client exposes them as separate calls. A stale
worker can therefore finish an already-issued Docker call, but cannot durably finalize or
start another one.

No database lock is held while reading secret files or calling Docker.

## Deterministic execution identity

```text
api container:       fstack-crusher-<deployment UUID without hyphens>
migration container: fstack-crusher-migrate-<deployment UUID without hyphens>
effect key: sha256("fstack.local-crusher-effect.v1\0" + operation UUID)
```

The sealed spec fingerprint is canonical SHA-256 over:

- discriminator `fstack.local-crusher-spec.v1`;
- operation and deployment UUIDs;
- requested immutable Crusher image digest;
- public domain;
- database and object-storage secret references;
- deterministic container names;
- configured Docker network;
- fixed commands, mount destinations and restart policy.

The fingerprint includes a canonical complete Docker create policy for both containers:
image digest, explicit overrides, fixed source hashes, mount destinations, network and
security fields. Image environment and image labels are represented before Docker access
by the literal marker `inherit-from-requested-digest`, not by undiscovered values. The
requested digest cryptographically commits those inherited fields. Unspecified privilege
additions, mounts, networks, ports or environment overrides are explicit empty sets.

Canonical serialization is UTF-8 JSON with sorted keys, ASCII escaping, no insignificant
whitespace, arrays preserving declared order, lowercase canonical UUIDs and lowercase
digest/fingerprint hex. SHA-256 covers those exact bytes. The complete policy can therefore
be sealed in PostgreSQL before image pull or inspection.

Before new Docker mutation, desired rows must reproduce the sealed fingerprint.

## Local secret contract

This slice resolves only database references with provider `local-file`:

```text
secret://local-file/<segment>[/<segment>...]
```

Every segment must already satisfy the released lowercase slug grammar. Empty, dot,
absolute, percent-encoded, separator-containing and traversal segments are therefore
invalid before filesystem access.

`FSTACK_LOCAL_SECRET_ROOT` is required by the worker. The reference maps beneath that root
to a directory containing exactly the consumed files:

```text
owner.json
application.json
```

`owner.json` contains exactly:

- `owner_url`;
- `application_user`;
- `application_password`.

`application.json` contains exactly:

- `application_url`;
- `magic_link_secret`;
- `public_base_url`;
- `dashboard_base_url`.

The worker opens the configured root and every descendant with directory-relative,
non-following operations and rejects symlinks in every source component. The source root
and all components through each leaf must be owned by the worker UID and not group/world
writable. Leaves must be worker-owned regular files with no group/world permissions. JSON
is UTF-8, at most 64 KiB per file, duplicate-key-free, exact-keyed and contains nonempty
strings only.

The local worker's operating-system UID and principals with Docker-socket access are
installation authority and are explicitly trusted; this slice cannot isolate secrets from
a same-UID or Docker-root-equivalent attacker. Filesystem race resistance is against less
privileged users and replacement through untrusted path components.

After validation, the worker copies bytes from the still-open source descriptors into the
deterministic directory
`FSTACK_SECRET_STAGE_ROOT/<deployment UUID without hyphens>/`. The pre-provisioned stage
root is worker-owned mode `0700`, has no writable ancestor available to less privileged
users, and is outside the reference-addressable source tree. The deployment directory is
created with directory-relative exclusive non-following operations at mode `0700`; staged
`owner.json` and `application.json` are created at mode `0444` inside the confined
directory so cap-dropped container root can read each bind-mounted leaf, fsynced with their
parent, and never rewritten in place. A pre-existing snapshot is accepted only when ownership,
mode, regular-file identity and bytes exactly match the newly resolved bundle. Otherwise
execution fails closed with `unsafe_secret_file`. Snapshots are installation-local runtime
secret material, retained while their deterministic containers exist and never represented
in PostgreSQL or public APIs.

Before Docker client construction, semantic bundle validation requires:

- `owner_url` and `application_url` use the same exact accepted database scheme:
  `postgresql`, legacy `postgres`, or SQLAlchemy psycopg 3 `postgresql+psycopg`; no other
  driver suffix is accepted. Both are canonical PostgreSQL URLs accepted by
  SQLAlchemy/Crusher, with no query, fragment or omitted database name;
- `owner_url` contains owner userinfo;
- the decoded application URL username/password exactly equal `application_user` and
  `application_password`;
- owner and application URLs identify the same normalized host, port and database;
- application and owner usernames differ;
- public/dashboard values are canonical HTTP(S) base URLs without credentials, query or
  fragment;
- `magic_link_secret` is nonempty.

Malformed or inconsistent bundles are permanent `invalid_secret_bundle`. Unsafe files are
permanent `unsafe_secret_file`; missing files are retryable `secret_unavailable`;
unsupported providers are permanent `unsupported_secret_reference`. Parsed URLs and
validation details are never logged or returned.

The worker mounts only the immutable staged `owner.json` into the migration container at
`/run/secrets/fstack/owner.json` and only the immutable staged `application.json` into the
API container at `/run/secrets/fstack/application.json`, read-only. Immediately before
Docker create it reopens the stage directory with non-following directory-relative
operations and revalidates ownership, mode, regular-file identity and expected bytes. A
less-privileged process cannot replace the stage path between this check and daemon mount
resolution because it has no write/search authority to the stage root or deployment
directory. Secret values never enter Fstack PostgreSQL, audit payloads, API responses,
Docker labels, Docker command arguments, Docker configuration environment or Fstack logs.

`object_storage_secret_ref` remains sealed desired state but is not resolved because the
current Crusher runtime has no object-storage contract.

## Docker topology

`FSTACK_DOCKER_NETWORK` names an existing installation-managed Docker network. This slice
does not create or delete networks, volumes, databases or arbitrary containers.

Both deterministic containers use the exact requested Crusher digest and labels:

- `io.fstack.managed=true`;
- operation, organization and deployment UUIDs;
- effect key and spec fingerprint.

The canonical common Docker create fields are:

- platform: the local daemon's native Linux platform; no cross-platform emulation;
- entrypoint: inherited exactly from the requested immutable image digest and then sent
  explicitly in the create request;
- user: `0:0`; working directory: `/app`;
- container environment policy: `inherit-from-requested-digest`, with no Docker
  environment overrides;
- label policy: immutable image labels from the requested digest plus exactly the Fstack
  labels above, with Fstack keys required to have the sealed values;
- one attachment to `FSTACK_DOCKER_NETWORK`, with the deterministic container name as its
  only alias; no other networks;
- no published ports, links, devices, device requests, IPC/PID/UTS sharing or extra hosts;
- `privileged=false`, `readonly_rootfs=false`, `cap_add=[]`, `cap_drop=['ALL']`,
  `security_opt=['no-new-privileges:true']`;
- no tmpfs, volumes or mounts except the one exact read-only secret-file bind;
- logging driver `json-file` with an explicit empty option map;
- `auto_remove=false`, no init override, and the stop signal inherited exactly from the
  requested immutable image digest and sent explicitly in the create request.

The migration-specific fields are:

- command: `['/app/.venv/bin/python', '-c', MIGRATION_LAUNCHER_V1]`;
- healthcheck disabled with `['NONE']`;
- restart policy `no`;
- exact bind source
  `FSTACK_SECRET_STAGE_ROOT/<deployment hex>/owner.json` to exact target
  `/run/secrets/fstack/owner.json`.

The API-specific fields are:

- command: `['/app/.venv/bin/python', '-c', API_LAUNCHER_V1]`;
- restart policy `unless-stopped` with zero maximum retry count;
- healthcheck command `['CMD', '/app/.venv/bin/python', '-c', HTTP_PROBE_V1]`, interval
  30 seconds, timeout 5 seconds, start period 10 seconds and 3 retries;
- exact bind source
  `FSTACK_SECRET_STAGE_ROOT/<deployment hex>/application.json` to exact target
  `/run/secrets/fstack/application.json`.

After a lease-fenced pull, the worker requires the local image's content digest to equal
the requested digest, reads that image's environment and labels, and derives the expected
inspection by applying the already-sealed inheritance policy. This metadata acquisition
does not alter the fingerprint. Every policy field above, including explicit empty sets
and all four launcher/probe source hashes, is part of the canonical spec fingerprint. No
host port is published and no DNS or proxy configuration changes.

The migration container mounts only `owner.json` and runs, in order:

```text
python -m crusher.db.application_role
python -m crusher.migrations.core.deploy
python -m crusher.db.application_role
```

The canonical migration command is
`['/app/.venv/bin/python', '-c', MIGRATION_LAUNCHER_V1]`, where the versioned source is
exactly:

```python
import json, os, subprocess, sys
with open('/run/secrets/fstack/owner.json', encoding='utf-8') as stream:
    bundle = json.load(stream)
environment = os.environ.copy()
database_prefix = 'CRUSHER_DATABASE_'
environment[database_prefix + 'OWNER_URL'] = bundle['owner_url']
environment[database_prefix + 'URL'] = bundle['owner_url']
environment[database_prefix + 'APPLICATION_USER'] = bundle['application_user']
environment[database_prefix + 'APPLICATION_' + 'PASSWORD'] = bundle['application_password']
for command in (
    ['/app/.venv/bin/python', '-m', 'crusher.db.application_role'],
    ['/app/.venv/bin/python', '-m', 'crusher.migrations.core.deploy'],
    ['/app/.venv/bin/python', '-m', 'crusher.db.application_role'],
):
    result = subprocess.run(command, env=environment, check=False)
    if result.returncode != 0:
        sys.exit(result.returncode)
```

The canonical API command is `['/app/.venv/bin/python', '-c', API_LAUNCHER_V1]`, where the
versioned source is exactly:

```python
import json, os
with open('/run/secrets/fstack/application.json', encoding='utf-8') as stream:
    bundle = json.load(stream)
crusher_prefix = 'CRUSHER_'
os.environ[crusher_prefix + 'DATABASE_URL'] = bundle['application_url']
os.environ[crusher_prefix + 'MAGIC_LINK_' + 'SECRET'] = bundle['magic_link_secret']
os.environ[crusher_prefix + 'PUBLIC_BASE_URL'] = bundle['public_base_url']
os.environ[crusher_prefix + 'DASHBOARD_BASE_URL'] = bundle['dashboard_base_url']
os.environ[crusher_prefix + 'DASHBOARD_MAGIC_LINK_DELIVERY'] = 'inline'
os.environ[crusher_prefix + 'MTA_ROUTE_CACHE_BACKEND'] = 'memory'
os.execv('/app/.venv/bin/uvicorn', [
    '/app/.venv/bin/uvicorn', 'crusher.api:create_app', '--factory',
    '--host', '0.0.0.0', '--port', '8000',
])
```

Neither launcher prints the bundle or places its values in Docker configuration.

Success requires all of:

1. migration container exits zero;
2. after an immediate lease check, bounded `docker exec` runs exactly
   `['/app/.venv/bin/python', '-c', DATABASE_PROBE_V1]` in the running API container. The
   fixed probe reads `/run/secrets/fstack/application.json`, converts only an exact
   `postgresql+psycopg://` prefix to libpq's `postgresql://`, leaves exact
   `postgresql://` and `postgres://` prefixes unchanged, opens
   `psycopg.connect(application_url, connect_timeout=5)`, executes literal `SELECT 1`,
   requires exact row `(1,)`, closes the connection and exits zero without printing. Any
   parsing, prefix, connection or query failure exits one without output;
3. Docker reports the API container health status `healthy` under the frozen healthcheck;
4. after another immediate lease check, bounded `docker exec` runs exactly
   `['/app/.venv/bin/python', '-c', HTTP_PROBE_V1]`. The fixed probe requests
   `http://127.0.0.1:8000/health` with a five-second timeout, requires HTTP 200, reads at
   most 1025 bytes, requires the body to be no more than 1024 bytes, decodes UTF-8 JSON and
   requires exact object `{"status":"ok","service":"crusher"}`, with no extra keys.

`DATABASE_PROBE_V1` is exactly:

```python
import json, psycopg, sys
status = 1
try:
    with open('/run/secrets/fstack/application.json', encoding='utf-8') as stream:
        bundle = json.load(stream)
    application_url = bundle['application_url']
    if application_url.startswith('postgresql+psycopg://'):
        application_url = 'postgresql://' + application_url.removeprefix('postgresql+psycopg://')
    elif not application_url.startswith(('postgresql://', 'postgres://')):
        application_url = None
    if application_url is not None:
        with psycopg.connect(application_url, connect_timeout=5) as connection:
            with connection.cursor() as cursor:
                cursor.execute('SELECT 1')
                if cursor.fetchone() == (1,):
                    status = 0
except Exception:
    pass
if status != 0:
    sys.exit(1)
```

`HTTP_PROBE_V1` is exactly:

```python
import json, sys, urllib.request
with urllib.request.urlopen('http://127.0.0.1:8000/health', timeout=5) as response:
    if response.status != 200:
        sys.exit(1)
    body = response.read(1025)
if len(body) > 1024:
    sys.exit(1)
payload = json.loads(body.decode('utf-8'))
if type(payload) is not dict or payload != {'status': 'ok', 'service': 'crusher'}:
    sys.exit(1)
```

Each versioned source is the UTF-8 bytes shown in its code block, with LF line endings and
the displayed final newline. Its literal SHA-256 is:

- `MIGRATION_LAUNCHER_V1`: `5cc3ebc8fdbff87500470236433ef65ca72607f9edc821e29fc8b2c5fcf1c592`;
- `API_LAUNCHER_V1`: `33bb430f5625620c4c7c5b1ffefbe11feb2b97650590b61ffcc4b9e3e82186ca`;
- `DATABASE_PROBE_V1`: `3e33cea9bce2ff5a450733505ad9e5f120e265057ca6a4f9388716b41e5d6615`;
- `HTTP_PROBE_V1`: `a6045ff06180fe16a7ca13efcf1744d377c61a9f66e00d789feecbd6bb7f4d1f`.

Implementation tests extract the exact constants used for Docker commands, recompute all
four hashes and require these literal values.

Each probe exec create and exec start is a Docker mutation for lease-fencing purposes.

Crusher's shallow health endpoint alone is not readiness.

## Docker reconciliation

The effect row is persisted before Docker mutation. Reconciliation uses exact deterministic
names and labels.

- Missing migration container: create it once. Existing matching migration container may
  be started again when retryable and must exit zero before API work.
- Missing API container: create it once.
- Existing managed container is adopted only when normalized Docker inspection exactly
  equals the complete canonical create spec, including image, entrypoint, command,
  healthcheck, environment, user, working directory, labels, mounts, network attachments,
  aliases, restart policy, ports and every privilege/security field. Extra mounts,
  networks, ports, devices, capabilities, security options or environment entries are
  conflicts.
- Existing same-name unmanaged or mismatched container: permanent
  `docker_name_conflict` or `docker_spec_conflict`; never delete, rename or replace it.
- API container never started: start that same container.
- API container already running: inspect and probe it without another create/start.
- API container that previously started and exited/dead: permanent `container_exited`; do
  not restart or replace it in this slice.
- Ambiguous Docker failures are followed by deterministic-name inspection before outcome
  classification.

Immediately before each create or start, the worker revalidates its lease in PostgreSQL.
Every Docker call has a bounded timeout. After every create/start result, including a
timeout or transport error, the worker performs a final stable deterministic-name
inspection before classification. Duplicate create is safe because the name is globally
deterministic; duplicate migration start is safe because the same container reruns the
idempotent role/migration/role command; API start is issued only when inspection proves it
has never started. Delayed completion by a stale worker is treated as an ambiguous outcome
for the new owner to reconcile, never as proof of absence or permission to create a second
container.

A confirmed effect is authoritative and finalizes without another Docker mutation.

## Errors and retry policy

Retryable codes:

- `secret_unavailable`;
- `docker_unavailable`;
- `image_unavailable`;
- `migration_unavailable`;
- `readiness_unavailable`;
- `worker_internal_error`.

Permanent codes:

- `unsupported_secret_reference`;
- `unsafe_secret_file`;
- `invalid_secret_bundle`;
- `docker_name_conflict`;
- `docker_spec_conflict`;
- `migration_failed`;
- `container_exited`.

Retryable failure terminates the attempt and returns the operation to pending while
retaining the durable effect for reconciliation. Permanent failure atomically terminates
the effect, attempt and operation. Raw exception text, Docker output and secret-shaped
values are discarded.

## Frozen acceptance tests and traceability

Every normative section above maps to at least one numbered test below.

1. **Migration equivalence:** fresh base-to-head and real `0001 -> 0002` upgrades produce
   identical columns, checks, composite FKs, indexes, triggers and backfills.
2. **Operation lifecycle:** PostgreSQL rejects invalid null bundles, skipped transitions,
   terminal mutation and current-attempt inconsistencies; `started_at` remains first-ever.
3. **Attempt lifecycle:** PostgreSQL rejects invalid state bundles, duplicate active
   attempts, nonpositive/reused numbers or fences, terminal mutation and an active attempt
   not referenced by its running operation.
4. **Effect lifecycle and agreement:** PostgreSQL rejects cross-tenant or same-tenant
   wrong-deployment effects, invalid bundles, illegal ownership transfer and terminal
   mutation; prepared failure and retryable prepared/started transfer are accepted only for
   the exact current higher-fenced attempt.
5. **GET pending:** POST followed by GET returns exactly the frozen keys, same public IDs,
   pending state, zero attempts, UTC timestamps, no `replayed` and no secret/authority
   fields.
6. **GET state matrix:** exact running, retry-pending, succeeded and failed responses prove
   timestamp and `last_error` bundles.
7. **GET boundary:** authentication precedes lookup; unknown UUID, no-store and persistence
   error envelopes are exact and sanitized.
8. **Audit contract:** event types, one-per-operation cardinality and bounded sanitized
   payload keys are database- and route-tested.
9. **No work:** `--once` exits successfully without rows or Docker construction when no
   operation is claimable.
10. **Concurrent claims:** two PostgreSQL workers race; one active attempt, one current
    attempt and one effect exist.
11. **Post-lock expiry revalidation:** a candidate seen expired but heartbeated before lock
    acquisition is skipped; a truly expired exact current attempt is abandoned and the
    replacement receives a strictly higher fence.
12. **Heartbeat/finalization lock waits:** calls beginning while valid but acquiring locks
    after expiry reject without partial writes.
13. **No locks across I/O:** instrumented secret reads and every Docker call prove no Fstack
    transaction or row lock remains open.
14. **Secret reference and confinement matrix:** unsupported/malformed references,
    traversal, symlinks, nonregular files, wrong owner/mode, component replacement and
    identity changes fail before Docker client construction. Staging tests race a
    less-privileged process before and after final revalidation and prove it cannot replace
    the mounted snapshot; same-UID/Docker-authority trust is asserted as a deployment
    precondition rather than claimed as an isolation boundary.
15. **Secret parser matrix:** exact keys, 64-KiB bounds, UTF-8, duplicate keys, scalar types,
    empty values, PostgreSQL URL canonicality, role/database agreement and base-URL rules
    map only to stable codes.
16. **Launcher contract:** exact mount destinations, read-only flags, argv and every
    `CRUSHER_*` mapping are asserted without secret-bearing Docker environment or command
    arguments; launcher/probe source hashes are fixed; database and HTTP probes run inside
    the API container with exact argv, bounds and results.
17. **Canonical Docker specs:** expected migration/API specs include every execution and
    privilege field and every explicit or inherited value. Mismatch tests cover
    entrypoint, healthcheck, environment, user,
    mounts, extra networks/ports, privileged/capability/security/read-only-rootfs/device
    fields and always leave the container untouched.
18. **Single-worker success:** with real PostgreSQL and Docker, one worker creates exactly
    the deterministic migration and API containers, proves migration, application-role
    `SELECT 1`, Docker health and exact Crusher health, then atomically confirms effect,
    attempt, operation and audit state.
19. **Prepared paths:** retryable pre-Docker failure transfers the prepared effect to a
    higher fence; permanent pre-Docker failure performs prepared-to-failed atomically.
20. **Crash matrix:** recovery after effect prepare, migration create/start/exit, API
    create, API start, health, effect confirmation and before operation finalization
    converges without duplicate external mutation.
21. **Delayed stale Docker calls:** lease loss during bounded create/start prevents further
    stale mutation and finalization; image pull, container create/start and exec
    create/start each exercise the immediate fence. A streamed image pull lasting over 20
    seconds, a long migration and a long health wait each prove renewal at no more than
    20-second intervals; injected renewal failure proves the stream/call is abandoned or
    allowed to finish ambiguously but no later Docker mutation occurs. The new owner
    performs final inspection and reconciles the one deterministic container.
22. **Ambiguous Docker outcomes:** create/start timeout and hostile Docker output trigger
    deterministic-name inspection, stable sanitized errors and no raw output persistence.
23. **Container state policy:** exact matching containers are adopted; unmanaged or
    mismatched names and previously started exited/dead API containers remain untouched
    and fail permanently; a never-started API container is started once.
24. **Retry policy:** each retryable code returns the operation to pending with a terminal
    failed attempt and transferable unchanged effect; each permanent code atomically
    terminates effect, attempt and operation.
25. **Secret non-disclosure:** independent markers are absent from every database text/JSON
    value, audit payload, API response, captured Fstack log, Docker label, command,
    configuration environment and error traceback.
26. **Exclusion enforcement:** the worker creates no networks, volumes, databases, extra
    containers or host port bindings and performs no delete/rename/DNS/proxy mutation.
27. **Idempotent terminal replay:** repeated worker runs after success perform no Docker
    mutation and leave one succeeded attempt, one confirmed effect and one terminal audit
    event.
28. **Live smoke:** a digest-pinned Crusher image, existing Docker network,
    pre-provisioned test PostgreSQL database and exact local-file bundles converge to
    succeeded and remain converged on replay.
29. **PostgreSQL-only gate:** every persistence/concurrency test requires explicit
    PostgreSQL and no SQLite path is introduced.

## Explicit exclusions

- PostgreSQL server/database creation or deletion;
- object-storage resolution or provisioning;
- public port publication, Caddy, DNS or TLS;
- continuous container supervision, upgrades, rollback or deletion;
- Kubernetes, RunPod or other cloud execution;
- billing, quotas and entitlements;
- generic adapters, plugin discovery or Remesher support;
- portal/UI work.
