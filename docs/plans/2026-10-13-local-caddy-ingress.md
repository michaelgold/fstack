# Local Caddy Ingress Implementation Plan

> **For Hermes:** Use subagent-driven-development skill to implement this plan task-by-task.

**Goal:** Make newly provisioned Crusher deployments succeed only after their internally
ready local-Docker runtime is reachable through one exact route on an
installation-managed Caddy HTTP listener.

**Architecture:** Keep Temporal as the sole retry authority. Add an immutable PostgreSQL
ingress-submission record, a v2 provisioning workflow with sanitized operation/submission
UUID payloads, an executable Docker topology preflight, a confined Unix-socket Caddy JSON
client, an exact routed-listener probe and atomic joint terminal finalizers that resolve
submissions by operation ID. Preserve the
existing POST contract, exact released v1 workflow/activity/history and audit shapes, and
every released migration byte-for-byte.

**Tech Stack:** Python 3.12+, FastAPI, SQLAlchemy Core, PostgreSQL 17, Alembic, Temporal
Python SDK, Docker CLI/Engine, Caddy native JSON admin API, pytest, Ruff and mypy.

**Frozen contract:** `docs/architecture/local-caddy-ingress-slice.md`

---

## Non-negotiable implementation rules

- Follow strict vertical TDD: one failing behavior, observe RED, write the minimum GREEN,
  refactor only while green.
- Do not edit these released migrations:
  - `migrations/versions/20261003_0001_initial_control_plane.py`
  - `migrations/versions/20261004_0002_operation_lifecycle_foundations.py`
  - `migrations/versions/20261006_0003_workflow_dispatch_outbox.py`
- Add successor revision exactly:
  - file: `migrations/versions/20261013_0004_local_caddy_ingress.py`
  - `revision = "20261013_0004"`
  - `down_revision = "20261006_0003"`
- PostgreSQL is the only database. Do not add SQLite code, fixtures or dependencies.
- Before running any PostgreSQL-marked test command in this plan, export the
  `FSTACK_TEST_DATABASE_URL` environment variable to an isolated database whose name ends in
  `_test`; commands intentionally inherit it rather than embedding credentials in documentation.
- Do not add a generic ingress abstraction. Implement only installation-managed local
  Caddy.
- Target Caddy v2.11.7 admin semantics. Installation compatibility is limited to the exact
  v2.11.7 image digest certified by the release canary until another version/digest passes
  the same canary; never infer compatibility from a tag or major version, and never guess or
  hard-code the digest in documentation.
- Do not add DNS, TLS, route update/delete, rollback, compensation, domain mutation,
  container supervision, portal, billing, Remesher or object-storage resolution.
- Never include raw provider bodies/text, socket/listener paths, container IDs/names, Docker
  network/endpoint/address/alias or inspection details, route IDs/config, fingerprints,
  secret references/values or host paths in API output, new audit
  shapes, v2/new Temporal payloads/errors/history, logs or telemetry. The sole exception is
  exact replay/continued execution of released v1 Temporal payloads/history and exact
  released v1 audit shapes; do not add fields to them or leak their provider, submission-key,
  fingerprint or deterministic-container values into APIs, logs or telemetry.
- Every commit named below is a future implementation boundary. Do not combine unrelated
  RED/GREEN cycles inside one commit.
- Upgrade in maintenance mode within this one release: stop/quiesce API, dispatchers and
  workers; prove all old processes and pollers exited; apply `0004`; start the v2-capable
  worker and dispatcher; prove v1/v2 registration and polling; only then enable API traffic.
  A v2 row must never be visible to old code.
- Do not change CI credentials or weaken CI. Redacted reviewer output is not evidence of a
  credential failure. Canonical GitHub Actions run `38330810746` on merge SHA
  `2169276635c545b1bbc53fb2d523833cbc888899` passed 497 PostgreSQL tests. Runtime gates may
  prove that a non-redacted credential works, but must never write it to source, docs or
  evidence.

## Exact contracts to implement

### PostgreSQL `ingress_submissions`

Create exactly these columns:

| Column | PostgreSQL type | Null | Rule |
|---|---|---:|---|
| `id` | `uuid` | no | primary key |
| `organization_id` | `uuid` | no | exact operation/deployment agreement |
| `operation_id` | `uuid` | no | one ingress submission per operation |
| `deployment_id` | `uuid` | no | immutable desired deployment |
| `provider` | `varchar(32) COLLATE "C"` | no | exactly `local_caddy` |
| `generation` | `bigint` | no | exactly `1` |
| `submission_key` | `varchar(64) COLLATE "C"` | no | lowercase SHA-256 |
| `route_fingerprint` | `varchar(64) COLLATE "C"` | no | lowercase SHA-256 |
| `route_id` | `varchar(200) COLLATE "C"` | no | deterministic global Caddy ID |
| `state` | `varchar(32) COLLATE "C"` | no | lifecycle below |
| `failure_code` | `varchar(64) COLLATE "C"` | yes | sanitized terminal code only |
| `prepared_at` | `timestamptz` | no | default `CURRENT_TIMESTAMP` |
| `submitted_at` | `timestamptz` | yes | route adopted/created |
| `confirmed_at` | `timestamptz` | yes | final listener readiness commit |
| `failed_at` | `timestamptz` | yes | terminal failure commit |

Constraints and guards:

- composite deferrable FK
  `(organization_id, operation_id, deployment_id)` to
  `provisioning_operations(organization_id, id, deployment_id)`;
- composite deferrable FK `(organization_id, deployment_id)` to
  `deployments(organization_id, id)`;
- unique `operation_id`;
- unique `submission_key`;
- unique `(provider, deployment_id, generation)`;
- unique `route_id`;
- `provider = 'local_caddy'`;
- `generation = 1`;
- both hash fields match `^[0-9a-f]{64}$`;
- `route_id = 'fstack-crusher-ingress-' || replace(deployment_id::text, '-', '')`;
- `failure_code IS NULL OR failure_code ~ '^[a-z][a-z0-9_]{0,63}$'`;
- exact state bundles:
  - `prepared`: no submitted/confirmed/failed timestamp, no failure code;
  - `submitted`: submitted timestamp only, no failure code;
  - `confirmed`: submitted and confirmed timestamps, no failure code;
  - `failed`: failed timestamp and failure code, never confirmed; submitted timestamp may
    be null or present;
- timestamps are monotonic;
- insert state must be `prepared`;
- allowed transitions are `prepared -> submitted|failed` and
  `submitted -> confirmed|failed`;
- identity columns and `prepared_at` are immutable;
- submitted facts are immutable after `submitted`;
- terminal rows cannot update or delete.

Use constants:

```python
PROVIDER = "local_caddy"
GENERATION = 1
SUBMISSION_VERSION = "fstack.local-caddy-ingress-submission.v1"
ROUTE_SPEC_VERSION = "fstack.local-caddy-route.v1"
```

Derive:

```text
submission_key = sha256(SUBMISSION_VERSION + NUL + canonical operation UUID)
route_id       = fstack-crusher-ingress-<deployment hex>
container      = fstack-crusher-<deployment hex>
```

Fingerprint canonical JSON:

```json
{
  "route": {
    "@id": "...",
    "match": [{"host": ["..."]}],
    "handle": [{"handler": "reverse_proxy", "upstreams": [{"dial": "...:8000"}]}]
  },
  "version": "fstack.local-caddy-route.v1"
}
```

Serialize with sorted keys, UTF-8, ASCII escaping enabled and separators `(',', ':')`.
Only the inner `route` object is sent to Caddy.

### Workflow and activity names

Keep all existing v1 names unchanged. Add:

```text
workflow: fstack.provision_crusher.v2
activity: fstack.prepare_local_crusher_ingress.v2
activity: fstack.apply_local_crusher_runtime.v2
activity: fstack.preflight_local_caddy_topology.v1
activity: fstack.reconcile_local_caddy_ingress.v1
activity: fstack.probe_local_caddy_ingress.v1
activity: fstack.finalize_local_crusher_ingress_success.v2
activity: fstack.finalize_local_crusher_ingress_failure.v2
```

Use these exact v2 payloads:

```python
@dataclass(frozen=True)
class ProvisionCrusherV2Input:
    operation_id: str


@dataclass(frozen=True)
class ProvisioningSubmissionIds:
    operation_id: str
    runtime_submission_id: str
    ingress_submission_id: str


@dataclass(frozen=True)
class ProvisioningFailureV2:
    operation_id: str
    scope: Literal["runtime", "ingress"]
    error_code: str


@dataclass(frozen=True)
class ProvisionCrusherV2Result:
    operation_id: str
    state: Literal["confirmed", "failed"]
```

Do not put submission IDs in `ProvisioningFailureV2`; the finalizer resolves and locks both
rows by `operation_id`. Do not put providers, external IDs, names, hostnames, paths, route
IDs, fingerprints or configuration in any v2 payload.

`prepare` accepts `ProvisionCrusherV2Input` and returns `ProvisioningSubmissionIds`. Apply,
topology preflight, reconcile, routed probe, and success finalization each accept exactly
`ProvisioningSubmissionIds`; the first four return no payload, and success finalization
returns `ProvisionCrusherV2Result`. Failure finalization accepts only
`ProvisioningFailureV2`. Activities load every provider, installation, deployment, route,
container, network, host and probe detail internally.

Keep the released v1 dataclasses, activity inputs/results and workflow result byte-compatible.
Their exact existing fields include provider, generation, submission key, spec fingerprint
and deterministic API/migration container names. This is a replay and pending-v1-dispatch
compatibility requirement, not permission to introduce those fields into v2 or expose them
through APIs, logs, telemetry or broadened audits.

Use the existing deterministic Workflow ID `provisioning/<operation UUID>`. New dispatch
rows use workflow type `fstack.provision_crusher.v2`; existing rows retain v1. The dispatcher
must select the stored workflow type and start the matching implementation.

### Activity ordering and retry policy

`ProvisionCrusherV2Workflow` executes:

1. prepare/start bundle, Start-to-Close 2 minutes, retry 1s to 10s; this one PostgreSQL
   transaction prepares/replays runtime plus ingress, transitions `pending -> running`, and
   inserts/replays the exact v2 `provisioning.started` audit;
2. apply runtime, Start-to-Close 30 minutes, retry 2s to 30s;
3. preflight local Caddy topology, Start-to-Close 1 minute, retry 1s to 10s;
4. reconcile Caddy, Start-to-Close 2 minutes, retry 1s to 10s;
5. probe routed listener, Start-to-Close 1 minute, retry 1s to 10s;
6. finalize success, Start-to-Close 2 minutes, retry 1s to 10s.

Retryable activity errors remain workflow failures-in-progress. A prepare error is
nonretryable only when it is a durable exact-bundle conflict resolvable by operation ID; it
calls the v2 failure finalizer with scope `runtime`. Database/provider infrastructure failure
before durable preparation remains retryable and does not call the finalizer. A nonretryable
runtime error calls with scope `runtime`; a nonretryable topology, reconcile or probe error
calls with scope `ingress`. The finalizer itself retries as infrastructure work, resolves both
submission IDs from PostgreSQL, and is idempotent.

### Caddy request framing and limits

Implement one private bounded HTTP/1.1 codec shared by admin and listener-probe code; do not
reuse the Docker private methods by inheritance. Each request receives one absolute
monotonic deadline before connect. The same deadline spans connect, every partial write
until the complete request is sent, status and headers, every body chunk and trailer, EOF,
and trailing-byte validation. No phase, retry, short write or successful read resets it.

Admin request limits:

```text
connect cap: 5.0 seconds
absolute total deadline: 15.0 seconds per request
header cap: 65,536 bytes
body cap: 65,536 bytes
```

Probe limits:

```text
connect cap: 5.0 seconds
absolute total deadline: 10.0 seconds
header cap: 16,384 bytes
body cap: 1,024 bytes
```

Admin requests:

```text
GET /id/<percent-encoded route ID> HTTP/1.1
Host: localhost
Connection: close

GET /config/apps/http/servers/fstack/routes HTTP/1.1
Host: localhost
Connection: close

GET /config/apps/http/servers HTTP/1.1
Host: localhost
Connection: close

POST /config/apps/http/servers/fstack/routes HTTP/1.1
Host: localhost
Content-Type: application/json
Content-Length: <exact bytes>
If-Match: <exact Etag>
Connection: close

<canonical route JSON>
```

Accept only one complete HTTP/1.0 or HTTP/1.1 response. Support either one valid
`Content-Length`, exactly `Transfer-Encoding: chunked`, or close-delimited bodies. Reject
ambiguous/malformed framing and cap trailers inside the header budget. Never retain or
surface response bodies after classification.

### Caddy response classification contract

Treat direct routes-array GET as two separate request contexts:

| Response | Before mutation | During post-mutation confirmation/readback |
|---|---|---|
| `200` | Parse an exact JSON array plus exactly one valid `Etag`, then apply the complete whole-array classifier. | Parse an exact JSON array and require exactly one exact desired route plus a safe whole array. Exact conflicts keep their permanent codes. A safe array without the route or any malformed, invalid or otherwise unresolved `200` confirmation is retryable `caddy_mutation_ambiguous`, whether the GET follows POST `200`, POST `400`/`404`, or ID-based ambiguity resolution. |
| `400`, `404` | Discard the body and run the bounded parent servers GET in pre-mutation context. | Discard the body and run the bounded parent servers GET in post-mutation context. |
| `401`, `403` | Permanent `caddy_admin_forbidden`. | Permanent `caddy_admin_forbidden`. |
| `300`-`399`, every other `400`-`499` | Permanent `caddy_admin_incompatible`. | Permanent `caddy_admin_incompatible`. |
| `100`-`199`, `201`-`299`, `500`-`599`, transport, timeout, framing | Retryable `caddy_unavailable`. | Retryable `caddy_mutation_ambiguous`. |

Malformed or out-of-range direct routes status lines are retryable `caddy_unavailable`
before mutation and retryable `caddy_mutation_ambiguous` after mutation. In either context,
parent `200` requires an object with an object `fstack` containing an array `routes`, then
uses the complete whole-array classifier. In post-mutation context, parent proof that the
hierarchy is missing remains permanent `caddy_prerequisite_missing`; exact conflicts retain
their permanent codes; an exact safe route confirms success; and any parent evidence that
cannot resolve the mutation, including a present safe array without the route, is retryable
`caddy_mutation_ambiguous`. Parent statuses otherwise retain the existing closed mapping:
`400`/`404` prerequisite missing, `401`/`403` forbidden, `3xx` and other `4xx` incompatible,
and retryable categories map to `caddy_unavailable` before mutation or
`caddy_mutation_ambiguous` after mutation.

### Installation configuration

Add these required worker environment variables:

```text
FSTACK_CADDY_ADMIN_ROOT=/run/fstack-caddy
FSTACK_CADDY_ADMIN_UID=<decimal uid>
FSTACK_CADDY_ADMIN_GID=<decimal gid>
FSTACK_CADDY_PROBE_ADDRESS=<fixed-caddy-ip>:<listener-port>
FSTACK_CADDY_CONTAINER_NAME=<canonical fixed container name>
```

The relative socket entry is code-owned and fixed as `caddy-admin.sock`; do not make it
request-controlled or environment-configurable. Validate UID/GID as canonical unsigned
decimal integers. Validate probe address as one canonical IPv4 or bracketed IPv6 literal
plus port; reject hostnames, URLs, zone IDs, credentials, paths and unspecified addresses.
Validate the Caddy container name as one canonical fixed installation value; it is never
request data and is not discovered by labels, prefix, image, or network membership.
The socket mode is exactly `0220`; reject every other mode, and require the worker process to
have the configured GID as a supplemental group. The trusted root is confined and not
group/other writable. Caddy must join/reach `FSTACK_DOCKER_NETWORK` and resolve deterministic
Crusher names; the worker must reach the fixed Caddy listener IP. Share the admin socket
separately through a confined volume. Verify topology before provisioning. Do not log these
values.

---

### Task 1: Freeze migration lineage and new schema expectations

**Objective:** Add tests that protect released migrations and define the exact successor
schema before writing the migration.

**Files:**
- Modify: `tests/test_postgres_schema.py`
- Test: `tests/test_postgres_schema.py`

**RED**

1. Add byte-hash assertions for all three released migration files. Preserve the existing
   `20261004_0002` hash assertion and calculate/freeze hashes for `0001` and `0003` from the
   current branch.
2. Add `test_local_caddy_ingress_schema_is_exact` asserting the exact
   `ingress_submissions` column set, FK names, unique constraints and check/trigger names.
3. Assert no attempt/lease/polling columns or tables were introduced.
4. Assert Alembic head is exactly `20261013_0004`.

Run:

```bash
uv run pytest tests/test_postgres_schema.py -q
```

Expected: RED because revision `20261013_0004` and `ingress_submissions` do not exist.

**GREEN**

Create only the migration skeleton and table/guards required by the exact schema contract.
Do not yet alter dispatch/audit constraints beyond what is required for migration execution.

Run the same test; expected: the exact-schema assertions pass, with later workflow-type tests
still absent.

**Commit boundary:**

```bash
git add migrations/versions/20261013_0004_local_caddy_ingress.py tests/test_postgres_schema.py
git commit -m "feat: add immutable local Caddy ingress submissions"
```

### Task 2: Cut new dispatch rows to v2 without rewriting v1 history

**Objective:** Make migration `0004` preserve old dispatch rows while assigning v2 to future
operations.

**Files:**
- Modify: `migrations/versions/20261013_0004_local_caddy_ingress.py`
- Modify: `tests/test_postgres_schema.py`
- Modify: `tests/test_provisioning_api_postgres.py`

**RED**

Add PostgreSQL tests that:

1. downgrade to `20261006_0003`, create an operation, then upgrade to head and assert its
   dispatch row remains `fstack.provision_crusher.v1`;
2. create an operation after head and assert its row is
   `fstack.provision_crusher.v2`;
3. assert the Workflow ID remains `provisioning/<operation UUID>` for both;
4. assert dispatch identity/history remains immutable;
5. assert downgrade fails closed once any `ingress_submissions` row exists;
6. assert downgrade fails closed when any v2 dispatch row exists, including a separate test
   for a still-`pending` v2 row with `dispatched_at IS NULL` and one for `dispatched`;
7. assert pristine downgrade/upgrade roundtrip works.

Run the two test files. Expected: RED because the trigger/check still accepts only v1.

**GREEN**

In migration `0004`:

- before upgrade DDL, take fail-fast `ACCESS EXCLUSIVE ... NOWAIT` locks, where PostgreSQL
  permits, on the complete dependency set of **pre-existing tables only**:
  `organizations`, `deployments`, `provisioning_operations`, `runtime_submissions`,
  `workflow_dispatches`, and `audit_events`; the newly created `ingress_submissions` table is
  transactionally protected by its own DDL and must not be named in the pre-DDL lock set;
- before downgrade DDL, take the same fail-fast lock set plus `ingress_submissions`;
- do not claim functions can be table-locked: locking every trigger-owning table prevents
  concurrent execution of the trigger functions while their definitions and constraints are
  replaced;
- replace `ck_workflow_dispatches_type` with an exact `IN` check for v1/v2;
- replace `enqueue_provisioning_workflow_dispatch()` so only future inserts use v2;
- do not update existing rows;
- implement fail-closed downgrade checks that reject on
  `EXISTS (SELECT 1 FROM workflow_dispatches WHERE workflow_type =
  'fstack.provision_crusher.v2')` without filtering by state or `dispatched_at`, and also
  reject on any ingress row; restore the v1 trigger/check only when both checks and all
  existing history guards are safe. Do not infer safety from delivery state.

Run:

```bash
uv run pytest tests/test_postgres_schema.py tests/test_provisioning_api_postgres.py -q
```

Expected: GREEN.

Task 2 assumes the documented maintenance-mode gate but is limited to executable database
cutover invariants. PostgreSQL tests cannot prove that external API, dispatcher, worker, or
Temporal poller processes have exited. Executable process-exit/poller-shutdown and restart
proof belongs only to the maintenance drill/canary in Tasks 18 and 21 and the final checklist.

**Commit boundary:** `feat: dispatch new provisioning operations to workflow v2`

### Task 3: Define canonical route and replayable ingress preparation

**Objective:** Prepare/replay the exact runtime and ingress submissions, start the operation,
and write the v2 started audit at one PostgreSQL boundary.

**Files:**
- Create: `src/fstack/ingress_submission.py`
- Modify: `src/fstack/runtime_submission.py`
- Modify: `migrations/versions/20261013_0004_local_caddy_ingress.py`
- Create: `tests/test_ingress_submission.py`
- Modify: `tests/test_temporal_runtime_submission_postgres.py`
- Modify: `tests/test_postgres_schema.py`

**RED**

Add unit tests for:

- exact deterministic route ID and container dial;
- exact route dictionary with no extra keys/options;
- stable submission key and route fingerprint;
- canonical fingerprint changes for host/deployment drift;
- returned public dataclass excludes route JSON, host, names and fingerprints.

Add PostgreSQL tests for:

- one call creates one runtime plus one ingress submission atomically;
- the same transaction changes the operation `pending -> running` and inserts exactly one
  `provisioning.started` audit containing exactly `runtime_submission_id` and
  `ingress_submission_id`;
- replay returns the same three UUIDs, preserves the first `started_at`, and replays the
  exact existing started audit without inserting another;
- drifted configuration cannot create a second row;
- a durable exact-bundle drift/conflict is distinguishable from infrastructure failure so
  the workflow can finalize only the former;
- failure injected after each statement rolls back both submissions, the pending-to-running
  transition and the started audit;
- one operation cannot bind a cross-organization ingress row.

Add migration tests proving the replacement audit constraint/history guard accepts only:

- released v1 requested `{}`, started `{runtime_submission_id, provider: "local_docker"}`,
  succeeded `{runtime_submission_id}`, and failed
  `{runtime_submission_id, error_code}`;
- v2 requested `{}`, started/succeeded
  `{runtime_submission_id, ingress_submission_id}`, and failed
  `{runtime_submission_id, ingress_submission_id, error_code}`.

Reject every mixed or superset shape, noncanonical UUID, wrong organization/operation
binding, provider in v2, ingress ID in v1, and extra key. Preserve all existing released v1
rows during migration.

Run the new focused tests. Expected: RED because the module/API is absent.

**GREEN**

Implement:

```python
@dataclass(frozen=True)
class PreparedProvisioningSubmissions:
    operation_id: UUID
    runtime_submission_id: UUID
    ingress_submission_id: UUID
```

Expose `prepare_operation_bundle(operation_id)` only. Internally reuse
`runtime_submission._prepare(connection, operation_id)` and insert/replay the ingress row
under the same `engine.begin()` transaction. In that transaction, lock the operation,
validate/replay both exact identities, perform `pending -> running`, and insert or replay the
exact v2 started audit. On replay require the operation, both rows and audit payload to agree
exactly. Never call or modify the released v1 `mark_running` path, and never return or log
sealed values.

In migration `0004`, replace the audit payload constraint and history guard with the closed
v1/v2 union above. The guard must validate every referenced UUID against the same
organization and operation. It must preserve exact released v1 provider-bearing started
audits while forbidding provider fields in every v2 audit.

Run focused tests and the existing runtime-submission PostgreSQL tests. Expected: GREEN.

**Commit boundary:** `feat: prepare deterministic Caddy ingress submissions`

### Task 4: Add bounded HTTP response parsing

**Objective:** Build one strict HTTP/1.x framing primitive before socket/provider behavior.

**Files:**
- Create: `src/fstack/http_framing.py`
- Create: `tests/test_http_framing.py`

**RED**

Table-drive tests for:

- valid content-length, chunked and close-delimited responses;
- HTTP/1.0 and HTTP/1.1 status parsing;
- duplicate equal content-length accepted, unequal rejected;
- transfer-encoding plus content-length rejected;
- unsupported/multiple transfer codings rejected;
- malformed status/header lines, folded headers and non-ASCII header names rejected;
- premature EOF, oversized headers/body/trailers and undeclared trailing bytes rejected;
- one absolute monotonic deadline across connect, complete send, headers, every body
  read/chunk/trailer, EOF and trailing-byte validation, with no timeout reset;
- thrown error is one sanitized internal code with no captured body/provider text.

Use `socket.socketpair()` for framing plus send/receive timing without a network or Caddy
dependency. Use an injected fake connector for the slow-connect case and an injected bounded
sender for blocked/slow-send behavior. Tasks 5 and 9 repeat the absolute-deadline contract on
the real Unix-admin and TCP-probe paths.

Run: `uv run pytest tests/test_http_framing.py -q`

Expected: RED, then implement the minimal parser and rerun to GREEN.

**Commit boundary:** `feat: add bounded HTTP framing for local providers`

### Task 5: Validate installation configuration and confine the Caddy admin socket

**Objective:** Prove every admin request revalidates one permissioned Unix socket through a
trusted directory descriptor.

**Files:**
- Create: `src/fstack/caddy_admin.py`
- Create: `tests/test_caddy_admin.py`

**RED**

Add Linux-only tests for:

- missing environment configuration, including `FSTACK_CADDY_CONTAINER_NAME`;
- noncanonical UID/GID and probe config rejection;
- noncanonical/empty Caddy container name rejection and proof it remains fixed installation
  configuration rather than request or Temporal data;
- trusted root must be absolute, directory, root/current-EUID owned and not group/other
  writable;
- fixed socket entry only;
- symlink root leaf, symlink socket entry, regular file, wrong UID/GID and overly broad mode
  fail closed;
- socket mode is accepted only when exactly `0220`; every other mode is rejected, and tests
  verify the worker has the configured supplemental group;
- directory traversal cannot be injected;
- socket entry replacement between pre-connect and post-connect evidence fails;
- root/socket are reopened and revalidated for every request;
- environments without `O_NOFOLLOW`, `O_DIRECTORY`, dir-fd stat/open support or
  `/proc/self/fd` fail closed;
- errors contain only bounded codes and discard path/provider context.
- slow connect, blocked/slow partial send, slow headers and slow body/chunks all consume the
  same absolute admin-request deadline and cannot reset it.

Run: `uv run pytest tests/test_caddy_admin.py -q`

Expected: RED.

**GREEN**

Implement private descriptor helpers modeled on the proven no-symlink patterns in
`src/fstack/runtime_application.py`, but do not share secret-path error codes or expose
paths. Connect through `/proc/self/fd/<root-fd>/caddy-admin.sock`, then compare
pre/post `lstat(..., dir_fd=root_fd, follow_symlinks=False)` evidence before sending bytes.

Run focused tests; expected: GREEN.

**Commit boundary:** `feat: confine Caddy admin socket access`

### Task 6: Implement whole-array Caddy inspection and conflict classification

**Objective:** Classify the dedicated server's entire routes array before adoption or
creation, without mutation.

**Files:**
- Modify: `src/fstack/caddy_admin.py`
- Modify: `tests/test_caddy_admin.py`

**RED**

Add fake Unix-server tests proving:

- separate exhaustive tables iterate every status `100` through `599` for direct routes GET
  in pre-mutation and post-mutation contexts, parent servers GET in both contexts, and ID GET
  before mutation, plus malformed/out-of-range status lines;
- pre-mutation direct routes GET `200` requires an exact JSON array and exactly one valid Etag
  before whole-array classification;
- pre-mutation direct routes GET `400` or `404` discards its body and triggers the bounded
  parent servers GET; `401`/`403` are permanent `caddy_admin_forbidden`; every `300`-`399` and
  every other `400`-`499`, including `405`, `412`, `415`, and `422`, are permanent
  `caddy_admin_incompatible`; every `100`-`199`, `201`-`299`, and `500`-`599`, plus
  transport/timeout/framing failures and malformed/out-of-range status lines, are retryable
  `caddy_unavailable`;
- post-mutation direct routes GET uses the same permanent forbidden/incompatible mappings, but
  every `100`-`199`, `201`-`299`, `500`-`599`, transport/timeout/framing failure and
  malformed/out-of-range status line is retryable `caddy_mutation_ambiguous`; `400`/`404`
  invokes parent fallback in post-mutation context;
- post-mutation direct routes GET `200` must parse and prove exactly one exact desired route
  plus a safe whole array; exact conflicts keep their permanent codes, while malformed JSON,
  a non-array, or any otherwise unresolved/invalid confirmation is retryable
  `caddy_mutation_ambiguous`;
- parent servers GET `200` requires an object; absent `fstack`, non-object `fstack`, absent
  `routes`, or non-array `routes` is permanent `caddy_prerequisite_missing`; an embedded
  routes array uses the same complete whole-array classifier; an exact safe route may be
  adopted and conflicts keep their exact codes, but a safe absent route is
  `caddy_admin_incompatible` before mutation because parent-only evidence supplies no direct
  routes `Etag` and cannot authorize POST;
- parent servers GET `400`/`404` is permanent `caddy_prerequisite_missing`; `401`/`403` is
  permanent `caddy_admin_forbidden`; every `3xx` and other `4xx` is permanent
  `caddy_admin_incompatible`; `1xx`, non-`200` `2xx`, `5xx`, transport/timeout/framing is
  retryable `caddy_unavailable` before mutation and retryable `caddy_mutation_ambiguous`
  during post-mutation readback/resolution;
- in post-mutation parent fallback, hierarchy absence remains permanent
  `caddy_prerequisite_missing`, exact conflicts retain their permanent codes, an exact safe
  route confirms success, and any parent evidence that cannot resolve the mutation, including
  a present safe array without the route, is retryable `caddy_mutation_ambiguous`;
- ID GET before mutation maps `200` to exact desired route or permanent
  `caddy_route_id_conflict`, `404` to absent, `401`/`403` to permanent
  `caddy_admin_forbidden`, every `300`-`399` and other `400`-`499` to permanent
  `caddy_admin_incompatible`, and every `100`-`199`, `201`-`299`, `500`-`599`,
  transport/timeout/framing failure to retryable `caddy_unavailable`;
- pre-mutation ID GET is read-only and precedes the routes-array GET; an exact `200` remains
  provisional until the array proves one exact occurrence and whole-array safety, while an
  exact global ID absent from the owned array is `caddy_route_id_conflict`;
- exact expected ID is adopted only when it occurs exactly once, equals the exact desired
  route, and the rest of the array is safe;
- exact adoption succeeds with an unrelated route only when that route is itself the exact
  frozen Fstack-owned shape for another deterministic deployment ID, exact host and matching
  deterministic Crusher upstream;
- same expected ID with changed host, matcher shape, handler, upstream or any extra key is
  permanent `caddy_route_id_conflict`;
- duplicate expected IDs are permanent `caddy_route_id_conflict`;
- exact expected ID plus a different ID claiming the same host is
  `caddy_host_conflict`;
- same host under a different route is `caddy_host_conflict`;
- a wildcard host that matches the desired host is `caddy_host_conflict`;
- a catch-all/no-host route is `caddy_routes_ambiguous`;
- before mutation, duplicate JSON keys, non-object routes or any other unclassifiable
  matcher/handler shape is `caddy_routes_ambiguous`; an invalid post-mutation `200`
  confirmation is instead retryable `caddy_mutation_ambiguous` unless it establishes an exact
  permanent whole-array conflict;
- missing server/routes is `caddy_prerequisite_missing`;
- raw JSON/error markers never escape exceptions/captured logs.
- no status classification retains a response body.

Run focused tests; expected: RED then GREEN.

Implement exact structural equality; do not normalize, fill defaults or accept superset
objects. Scan and classify the complete array before either adopting or posting. Permanent
conflicts leave every route untouched.

**Commit boundary:** `feat: classify existing Caddy routes safely`

### Task 7: Implement one-request atomic route mutation and ambiguity recovery

**Objective:** Append only the exact route, with optimistic concurrency and deterministic
post-ambiguity inspection.

**Files:**
- Modify: `src/fstack/caddy_admin.py`
- Modify: `tests/test_caddy_admin.py`

**RED**

Test:

- exact POST path, canonical JSON bytes, content length and `If-Match` Etag;
- no PUT/PATCH/DELETE or whole-config writes are possible from the public class;
- an exhaustive table iterates every POST status `100` through `599` plus malformed and
  out-of-range status lines;
- `200` is the only definite mutation acceptance and records route submission only after a
  post-mutation direct routes GET proves the exact route occurs once and the whole array is
  safe;
- a separate exhaustive direct routes GET table covers every `100`-`599` response plus
  malformed/out-of-range status lines in post-mutation context for confirmation after POST
  `200`, readback after POST `400`/`404`, and confirmation after exact ID-based ambiguity
  resolution;
- after POST `200` or exact ID-based ambiguity resolution, unavailable, malformed, invalid or
  otherwise unresolved direct routes confirmation, including a present safe array without the
  desired route, is retryable `caddy_mutation_ambiguous`;
- `412` maps to retryable `caddy_config_changed`; the activity performs no mutation retry and
  workflow retry restarts from pre-mutation ID inspection followed by full routes-array
  inspection;
- `400` and `404` discard their bodies and trigger direct-routes readback with bounded parent
  fallback, including concurrent prerequisite disappearance; missing prerequisite maps to
  `caddy_prerequisite_missing`, an exact safe route is success, and conflicts use the
  whole-array rules; a direct routes `200` that does not establish the exact unique route, or
  parent evidence that cannot resolve the mutation, maps to retryable
  `caddy_mutation_ambiguous`;
- `422` maps to permanent `caddy_route_rejected`;
- `401` and `403` map to permanent `caddy_admin_forbidden`;
- `405`, `415`, unsupported `3xx`, and every other bounded `4xx` map to permanent
  `caddy_admin_incompatible`;
- every `100`-`199`, `201`-`299`, `500`-`599`, timeout, EOF, malformed/framing response and
  transport failure triggers `GET /id/<id>` ambiguity resolution;
- an exhaustive table iterates every status `100` through `599` for ID GET after ambiguous
  POST: exact `200` is provisional, different `200` is permanent
  `caddy_route_id_conflict`, `401`/`403` are permanent `caddy_admin_forbidden`, every `3xx`
  and other `4xx` is permanent `caddy_admin_incompatible`, and `404` or every
  retryable/unavailable category becomes retryable `caddy_mutation_ambiguous`;
- matching ID post-inspection resolves success only after complete routes-array confirmation
  still proves the ID unique/exact and the array safe;
- mismatching post-inspection is permanent ID conflict;
- absent/unavailable post-inspection is retryable `caddy_mutation_ambiguous`;
- retry restarts from pre-mutation ID inspection and full routes-array inspection, then adopts
  rather than posting twice;
- concurrent prerequisite disappearance is classified by deterministic direct/parent
  readback for either `400` or `404`;
- no classification retains a response body.

Implement `CaddyAdmin.reconcile(route) -> None` with no externally visible provider data.

Run focused tests; expected: GREEN.

**Commit boundary:** `feat: reconcile one atomic Caddy route mutation`

### Task 8: Persist ingress submitted state without exposing route identity

**Objective:** Record adoption/mutation success as immutable provider acceptance.

**Files:**
- Create: `src/fstack/ingress_state.py`
- Modify: `src/fstack/caddy_admin.py`
- Create: `tests/test_ingress_state_postgres.py`

**RED**

PostgreSQL tests must prove:

- `mark_submitted(ingress_submission_id, operation_id)` performs
  `prepared -> submitted` and replays;
- mismatched operation/submission rejects with no mutation;
- submitted timestamp cannot change;
- terminal rows cannot be resubmitted;
- no socket, listener, host, route body, fingerprint or provider response is written as
  terminal evidence;
- infrastructure exceptions are sanitized.

**GREEN**

Implement state functions that load and lock exact operation/deployment agreement. The Caddy activity calls `mark_submitted` only after adoption or a mutation whose mandatory
fresh whole-array confirmation proves one exact route and a safe array. It does not store an
external Caddy body or error.

Run focused tests; expected: GREEN.

**Commit boundary:** `feat: persist local Caddy route submission state`

### Task 9: Implement production topology preflight and exact routed-listener probe

**Objective:** Prove the one supported Dockerized-Caddy topology before mutation, then verify
listener-to-Crusher reachability with an exact Host header and health body.

**Files:**
- Create: `src/fstack/caddy_topology.py`
- Create: `src/fstack/ingress_probe.py`
- Create: `tests/test_caddy_topology.py`
- Create: `tests/test_ingress_probe.py`

**RED**

Use exact Docker inspect/network-inspect fixtures and a fake TCP listener to test topology
preflight:

- it accepts only the configured `FSTACK_CADDY_CONTAINER_NAME`, deterministic
  `fstack-crusher-<deployment hex>` API container, and exact `FSTACK_DOCKER_NETWORK`;
- it loads the container name, network, probe address and immutable deployment/runtime
  identity internally from installation configuration/PostgreSQL, never request data;
- Caddy and Crusher must both be running and attached to that exact network;
- Caddy's exact IP on that network must equal the IP in
  `FSTACK_CADDY_PROBE_ADDRESS`;
- Docker network inspection must expose the deterministic Crusher name or alias on the exact
  Crusher endpoint, providing observable DNS-routing evidence;
- a bounded TCP connect to the configured Caddy listener must complete;
- one absolute 40-second monotonic preflight deadline spans all work; each of the three
  read-only Docker inspections has a 10-second cap and the TCP connect has a 5-second cap,
  with no phase resetting the absolute deadline;
- a valid not-found result for the exact network, missing/mismatched network membership,
  probe-IP mismatch, absent deterministic Crusher alias/endpoint, a missing/non-running
  Crusher container, or unsupported host-process/remote/implicit topology is permanent
  `caddy_topology_invalid`, scope `ingress`;
- unavailable/timed-out/malformed Docker inspection, a missing/non-running Caddy container,
  or unavailable listener is retryable `caddy_unavailable`;
- every failure returns no provider details, performs no Caddy admin request, and a workflow
  harness proves `reconcile` is never called after preflight failure;
- success returns `None` and exposes no container, network, endpoint, address, alias, inspect
  payload or provider text.

Use local TCP fake servers to test:

- canonical IPv4 and bracketed IPv6 probe-address validation;
- hostname, unspecified IP, URL, path, credentials, zone ID and invalid port rejection;
- exact request bytes and exact canonical Host header loaded from PostgreSQL desired state;
- status exactly 200;
- body exactly `{"status":"ok","service":"crusher"}` with no duplicate/extra keys;
- content-length, chunked and close-delimited response handling through `http_framing`;
- 5-second connect and 10-second total deadlines;
- slow connect, blocked/slow partial send, slow headers and slow body/chunks all consume one
  absolute TCP-probe deadline through EOF/trailing-byte validation with no reset;
- 16 KiB header and 1,024-byte body limits;
- no response/exception marker in raised error or logs.

All routed-readiness failures map to retryable `ingress_readiness_unavailable`; invalid
installation configuration maps to permanent `invalid_caddy_probe_configuration`.

Implement `preflight_local_caddy_topology(ids: ProvisioningSubmissionIds) -> None` and
`probe_ingress(ids: ProvisioningSubmissionIds) -> None`; both load desired/runtime and
installation facts internally rather than from Temporal payload fields. Use read-only bounded
Docker calls following the existing runtime Docker adapter's sanitization patterns, but do
not mutate or supervise either container. Declare a preflight timeout budget and assert the
one-minute activity Start-to-Close timeout exceeds it. Preflight must finish before
`CaddyAdmin.reconcile` can be constructed/called.

Run:

```bash
uv run pytest tests/test_caddy_topology.py tests/test_ingress_probe.py -q
```

Expected: GREEN after minimal implementation.

**Commit boundary:** `feat: preflight and probe local Caddy topology`

### Task 10: Add atomic joint success finalization

**Objective:** Confirm runtime, ingress, operation and audit in one transaction only after the
listener probe succeeds.

**Files:**
- Modify: `src/fstack/runtime_state.py`
- Modify: `src/fstack/ingress_state.py`
- Modify: `tests/test_ingress_state_postgres.py`
- Modify: `tests/test_temporal_runtime_submission_postgres.py`

**RED**

Add tests for one `finalize_provisioning_success(...)` transaction:

- requires matching operation/runtime/ingress IDs;
- requires runtime and ingress both `submitted`, operation `running`;
- changes both submissions to `confirmed` and operation to `succeeded` at one database
  boundary;
- inserts exactly one `provisioning.succeeded` audit with exactly
  `runtime_submission_id` and `ingress_submission_id`;
- replays terminal success without a second audit;
- any injected statement failure rolls back every state and the audit;
- the v2 success audit rejects extra keys, route/container IDs, provider fields and
  fingerprints, while the released v1 success audit remains accepted unchanged.

**GREEN**

Replace v2 success use of the old runtime-only finalizer; keep the old function unchanged for
v1 compatibility. Use the closed audit constraint/history guard introduced in Task 3; add
focused success-shape tests here rather than broadening it. The exact released v1 success
shape remains `{runtime_submission_id}` and the exact v2 success shape is
`{runtime_submission_id, ingress_submission_id}`.

Run focused PostgreSQL tests; expected: GREEN.

**Commit boundary:** `feat: atomically finalize runtime and Caddy ingress success`

### Task 11: Add atomic scoped failure finalization

**Objective:** Resolve submissions by operation ID and terminalize the applicable durable
records for permanent v2 failures.

**Files:**
- Modify: `src/fstack/ingress_state.py`
- Modify: `tests/test_ingress_state_postgres.py`

**RED**

Test exact behavior:

- the activity payload contains exactly canonical `operation_id`, scope and sanitized
  `error_code`, with no runtime or ingress submission ID;
- scope `runtime`: runtime becomes failed, prepared ingress becomes failed, operation becomes
  failed, one audit is appended;
- scope `ingress`: ingress becomes failed, operation becomes failed, already-submitted runtime
  remains submitted with unchanged external ID and submitted timestamp;
- a permanent prepare conflict after an exact bundle was already committed resolves and
  locks both submissions by operation ID, fails both and the operation consistently, and
  replays without a second audit;
- if no exact durable bundle exists, only one half exists, either row disagrees with the
  operation/organization/deployment, or PostgreSQL lookup/locking fails, the finalizer makes
  no terminal mutation and surfaces retryable infrastructure/state-resolution failure;
- retryable errors never call this function and leave durable state nonterminal;
- repeated same failure replays; different terminal code/scope conflicts;
- failure audit contains exactly both submission UUIDs and `error_code`;
- invalid/unsanitized error becomes bounded fallback and never stores provider text;
- transaction rollback is all-or-nothing.

Implement one locked transaction beginning from `operation_id`: lock the operation, select
and lock runtime plus ingress rows by operation ID, validate their exact relationship, then
apply scope rules. Never trust submission IDs from Temporal. Permit permanent prepare
terminalization only for an already-created exact bundle; classify absence/partial bundle or
database failure before durable preparation as retryable. Use the closed audit guard from
Task 3 and do not invent compensation or remove Docker/Caddy resources.

Run focused tests; expected: GREEN.

**Commit boundary:** `feat: finalize scoped provisioning failures atomically`

### Task 12: Add sanitized v2 worker activities

**Objective:** Keep every new v2 Temporal activity boundary free of sensitive/provider
context while preserving exact released v1 boundaries.

**Files:**
- Modify: `src/fstack/temporal_workflow.py`
- Modify: `src/fstack/temporal_worker.py`
- Modify: `tests/test_temporal_workflow.py`
- Modify: `tests/test_worker_contract.py`

**RED**

Add activity contract tests for all seven new activity names/payloads. Assert:

- canonical UUID parsing rejects alternate/noncanonical strings;
- returned payloads contain only the declared UUID/state fields;
- runtime v2 activity reuses existing Docker reconciliation but does not return container
  identity/name/fingerprint;
- topology activity is named exactly `fstack.preflight_local_caddy_topology.v1`, accepts the
  UUID-only `ProvisioningSubmissionIds`, loads all installation/runtime details internally,
  returns no details, maps permanent mismatch to `caddy_topology_invalid`, and maps
  Docker/listener unavailability to retryable `caddy_unavailable`;
- Caddy activity loads route desired state internally and does not return route/host data;
- probe activity loads host internally;
- known permanent codes become nonretryable `ApplicationError` with only the sanitized code;
- retryable provider/infrastructure failures are generic and contain no exception context;
- pre-durable prepare infrastructure failure stays retryable and does not invoke failure
  finalization;
- the failure activity accepts only `operation_id`, scope and `error_code`, then resolves
  submissions from PostgreSQL;
- finalizers map state conflicts to sanitized nonretryable errors;
- captured logs, exception chains and Temporal histories do not contain injected container,
  network, endpoint, address, alias, Docker-inspection or provider marker values.

**GREEN**

Register v2 activity functions while retaining every v1 function and payload conversion
unchanged for pending dispatches and replay. Use small mapping sets for known state
conflicts, following the existing worker's sanitizing pattern. Do not apply v2 field
prohibitions retroactively to the released v1 `RuntimeSubmission` history shape.

Run:

```bash
uv run pytest tests/test_temporal_workflow.py tests/test_worker_contract.py -q
```

Expected: GREEN.

**Commit boundary:** `feat: add sanitized local Caddy Temporal activities`

### Task 13: Add the deterministic v2 workflow

**Objective:** Orchestrate runtime readiness, topology preflight, Caddy reconciliation,
listener probe and joint finalization in the required order.

**Files:**
- Modify: `src/fstack/temporal_workflow.py`
- Modify: `tests/test_temporal_workflow.py`

**RED**

Use `WorkflowEnvironment.start_time_skipping()` and fake named activities to prove:

- exact activity order and payload equality;
- prepare is the first activity and its committed effect is already operation `running` plus
  one exact v2 started audit before runtime apply is scheduled;
- exact Start-to-Close timeouts and retry policies;
- runtime retryable failure does not schedule ingress/finalization;
- topology preflight runs after runtime apply/readiness and before reconcile, using the exact
  `ProvisioningSubmissionIds` payload and configured timeout/retry policy;
- any retryable or permanent preflight failure proves reconcile is never called;
- retryable `caddy_unavailable` remains workflow failure-in-progress, while permanent
  `caddy_topology_invalid` schedules failure scope `ingress` once;
- reconcile retryable failure does not schedule probe/finalization;
- permanent runtime failure schedules failure scope `runtime` once;
- durable prepare conflict schedules one failure with only operation ID, `runtime` scope and
  sanitized code, while pre-durable infrastructure failure schedules no finalizer;
- permanent topology/reconcile/probe failure schedules scope `ingress` once;
- success returns only operation ID and `confirmed`;
- no v2 history payload contains marker route/host/path/fingerprint/container/network/
  endpoint/address/alias/inspection/provider data.

Implement `@workflow.defn(name="fstack.provision_crusher.v2")` without changing the v1 class.

Run focused workflow tests; expected: GREEN.

**Commit boundary:** `feat: orchestrate Crusher publication with workflow v2`

### Task 14: Dispatch by persisted workflow type and register v1 plus v2

**Objective:** Start the workflow implementation named by the immutable outbox row and keep
v1 replay compatibility.

**Files:**
- Modify: `src/fstack/workflow_dispatch.py`
- Modify: `src/fstack/temporal_client.py`
- Modify: `src/fstack/temporal_worker.py`
- Modify: `tests/test_temporal_dispatch.py`
- Modify: `tests/test_temporal_dispatch_postgres.py`
- Modify: `tests/test_worker_contract.py`

**RED**

Test:

- pending selection returns `(operation_id, workflow_type)`;
- v1 rows start `ProvisionCrusherWorkflow` with v1 input;
- v2 rows start `ProvisionCrusherV2Workflow` with v2 input;
- unknown types fail closed and remain pending without logging payload/provider text;
- duplicate Workflow ID uses the existing execution under current conflict/reuse policy;
- worker registration contains both workflow classes, every released v1 activity, and all
  seven new activities including `fstack.preflight_local_caddy_topology.v1`;
- transient start failures preserve the exact pending row.

**GREEN**

Change the client helper to accept the persisted workflow type and branch only over the two
exact constants. Keep Workflow ID conflict/reuse behavior unchanged.

Run dispatch and worker tests; expected: GREEN.

**Commit boundary:** `feat: dispatch and register provisioning workflow v2`

### Task 15: Add explicit v1 replay compatibility evidence

**Objective:** Prove new worker code replays released v1 history.

**Files:**
- Modify: `tests/test_temporal_workflow.py`
- Create: `tests/fixtures/temporal/provision-crusher-v1-history.json`

**RED**

Generate a v1 history using the exact released pre-v2 activity sequence and payload/result
shapes, then freeze it as a fixture with no secrets. The fixture is expected to retain the
released `provider`, `generation`, `submission_key`, `spec_fingerprint`,
`api_container_name`, and `migration_container_name` fields; do not redact, rename or
retrofit them to the v2 contract. Add a test using Temporal's `Replayer` with both workflow
classes registered. The fixture must replay through `ProvisionCrusherWorkflow` without
nondeterminism while v2 tests execute independently.

Run:

```bash
uv run pytest tests/test_temporal_workflow.py -q
```

Expected: RED until the fixture/replayer setup is correct, then GREEN.

Review the fixture text to ensure it contains only the exact released v1 fields and no secret
refs/values, host filesystem paths, Docker container IDs, raw provider bodies or provider
exception text. Separately assert those grandfathered v1 values do not appear in API
responses or captured logs/telemetry.

**Commit boundary:** `test: prove provisioning workflow v1 replay compatibility`

### Task 16: Characterize and preserve the HTTP API contract

**Objective:** Lock the public API decision: unchanged POST and existing operation GET only.

**Files:**
- Modify: `tests/test_api.py`
- Modify: `tests/test_provisioning_api_postgres.py`
- Modify: `src/fstack/api.py` only if GET no-store handling needs generalized cleanup
- Modify: `src/fstack/provisioning.py` only for documentation/type comments, not request fields

**CHARACTERIZATION / CONTRACT PRESERVATION**

Add or retain tests that characterize the already-released behavior before implementation
changes. These assertions may already be GREEN; their purpose is to prevent regression, not
to pretend existing behavior begins RED. Prove that:

- OpenAPI POST request/response fields are byte-for-byte equivalent to the current contract;
- request fingerprint for the existing body remains unchanged;
- no `/v1/deployments/{id}` route exists;
- operation `succeeded` is returned only after joint finalization in PostgreSQL integration;
- every operation GET response/error, including invalid UUID and unauthorized, has
  `Cache-Control: no-store` and authenticates before lookup;
- API response serialization contains none of the forbidden ingress/runtime details, v2
  audits use only exact v2 shapes, and existing v1 audits remain limited to their exact
  released shapes.

No production API change should be needed beyond a small shared GET-auth/no-store helper if
the characterization exposes a real gap.

Run API tests; expected: GREEN before and after the slice unless a genuine pre-existing
contract gap is first captured explicitly.

**Commit boundary:** `test: freeze public API through local Caddy ingress`

### Task 17: Document installation prerequisites

**Objective:** Make the runtime configuration and ownership boundary operable without
claiming Fstack manages Caddy.

**Files:**
- Modify: `README.md`
- Modify: `.env.example`
- Modify: `docs/architecture/temporal-provisioning.md`

Document:

- all five new environment variables with placeholders only, including canonical fixed
  `FSTACK_CADDY_CONTAINER_NAME` as installation configuration rather than request data;
- fixed socket entry `caddy-admin.sock`;
- exact socket mode `0220`, rejection of every other mode, the worker's required configured
  supplemental group membership, and a trusted root that is never group/other writable;
- required Caddy server `fstack`, existing routes array and installation-owned listener;
- the first supported compatibility baseline is Caddy v2.11.7 at the exact image digest
  certified by the release canary; installations must use that certified version/digest until
  another version/digest is re-certified, must not infer support from tag/major alone, and
  must not copy a guessed digest from documentation;
- permissioned Unix admin socket, never unauthenticated TCP;
- Caddy must join/reach `FSTACK_DOCKER_NETWORK`, resolve deterministic Crusher container
  names, and reach port `8000`; the worker must reach the configured listener IP/port;
- the admin socket is shared separately on a confined volume, while the Caddy data plane and
  worker listener probe use the installation Docker network rather than worker loopback;
- after internal runtime readiness, bounded topology inspection proves the configured Caddy
  container and deterministic Crusher API container are running on the exact
  `FSTACK_DOCKER_NETWORK`, Caddy's network IP equals the probe IP, the Crusher endpoint
  exposes its deterministic name/alias, and the listener accepts TCP before reconcile;
- unsupported/mismatched topology or missing/non-running Crusher is permanent
  `caddy_topology_invalid`; unavailable Docker inspection, missing/non-running Caddy, or an
  unavailable listener is retryable `caddy_unavailable`;
- v2 order and joint success meaning;
- v1 registration/replay compatibility;
- routed HTTP readiness is not DNS/TLS/Internet readiness;
- exclusions from the frozen contract.

Run:

```bash
uv run python tools/scan_tracked_secrets.py
uv run ruff format --check .
git diff --check
```

Expected: all pass.

**Commit boundary:** `docs: describe installation-managed local Caddy ingress`

### Task 18: Add migration roundtrip and PostgreSQL gates

**Objective:** Verify the successor migration against live PostgreSQL and preserve released
history.

**Files:**
- Modify tests only if a discovered migration defect requires a new RED case before repair.

Run against an isolated database ending in `_test`:

```bash
uv run alembic upgrade head
uv run alembic downgrade 20261006_0003
uv run alembic upgrade head
uv run alembic downgrade base
uv run alembic upgrade head
uv run pytest -m postgres -q
```

Verify with SQLAlchemy inspection/tests:

- head is `20261013_0004`;
- exact columns/constraints/triggers exist;
- old migration hashes match;
- v1 rows remain v1 across upgrade;
- new rows are v2;
- downgrade rejects both pending and dispatched v2 rows, without consulting delivery state;
- upgrade locks only pre-existing trigger-owning/dependency tables before DDL, downgrade adds
  `ingress_submissions`, locks use fail-fast `NOWAIT` where feasible, and no function is
  described or treated as a lockable table;
- no leases, attempts, polling claims or scheduler fields exist;
- terminal runtime/ingress/audit records are immutable;
- downgrade fails closed with real ingress/v2 history.

Also execute the maintenance-mode upgrade drill: stop/quiesce API, dispatchers and workers;
verify process exit/no old poller; apply `0004`; start the v2-capable worker/dispatcher;
verify v1/v2 registration and polling; enable API only afterward. Insert a canary v2 command
only after that gate and prove old code had no opportunity to consume its row.

**Commit boundary:** only if a test-first repair is needed; otherwise no commit.

### Task 19: Run full static, unit, replay and secret gates

**Objective:** Produce complete repository evidence before the live canary.

Run exactly:

```bash
uv lock --check
git diff --check
uv run python tools/scan_tracked_secrets.py
uv run ruff check .
uv run ruff format --check .
uv run mypy
uv run pytest -q
```

Also search tracked source/test/docs output for forbidden strings introduced into v2/new
durable boundaries, API responses, logs and telemetry by the slice. Do not treat exact fields
inside the frozen released-v1 replay fixture as violations; instead verify that fixture
matches the released shape and that the fields do not escape history into other boundaries.
Review Temporal history fixtures and audit assertions manually. Any failure requires a new
focused RED test before production repair.

Do not edit CI credentials or claim CI is broken based on redacted output. Preserve
redaction. Record the already-passing canonical evidence—run `38330810746` on merge SHA
`2169276635c545b1bbc53fb2d523833cbc888899`, with 497 PostgreSQL tests passed—and separately
prove at runtime that a non-redacted credential works without writing it to source, docs or
captured evidence.

**Commit boundary:** `test: close local Caddy ingress quality gates` only if test artifacts or
repairs changed files.

### Task 20: Implement, test and review the isolated canary harness

**Objective:** Land a reviewed harness as source code before it is trusted to execute a live
PostgreSQL + Temporal + Docker + Caddy canary.

**Files:**
- Create: `tools/run_local_caddy_ingress_canary.py`
- Create: `tests/test_local_caddy_ingress_canary_contract.py`

Do not change CI configuration or credentials for this finding.

**RED**

Contract-test the harness command construction and cleanup registry without launching
services. Assert:

- every resource is Fstack-prefixed and cleanup is registered before create;
- it requires a clean tree for `--source-commit`, or an exact immutable source archive;
- it refuses non-test/existing production resource names;
- Caddy, Crusher, API and worker use one isolated Docker network;
- worker configuration includes the exact isolated Caddy container name;
- Caddy receives one fixed isolated IP and worker configuration uses
  `<fixed-caddy-ip>:<listener-port>`, never worker loopback;
- Caddy-to-Crusher DNS/port reachability and worker-to-Caddy-listener reachability are checked
  before provisioning;
- the success fixture proves preflight precedes reconcile, while permanent wrong-network,
  probe-IP, and missing deterministic-alias fixtures return `caddy_topology_invalid`, never
  call reconcile, and preserve byte-identical Caddy config;
- the admin socket is shared separately through a confined volume, uses exact mode `0220`,
  and the worker has the configured supplemental group;
- exact immutable digests for Caddy, PostgreSQL, Temporal and Crusher are required harness
  inputs; the Caddy digest must identify v2.11.7, and mutable tags and omitted/guessed digests
  are rejected;
- evidence output must be outside the source tree and no live execution may modify tracked
  files;
- every real-Caddy route-conflict fixture is syntactically valid Caddy configuration and
  cleanup is verified on success and failure; duplicate expected-ID conflict stays in the
  fake Unix admin/unit protocol suite and is not loaded or labeled valid here because real
  Caddy rejects duplicate IDs while loading/indexing configuration;
- the maintenance-upgrade harness path encodes the Task 21 sequence without claiming Task 20
  has proved live process or poller exit.

**GREEN**

Implement the harness and its contract tests only. Run focused tests, the full static/unit
gates, and security review of command construction, resource isolation, secret handling and
cleanup. Do not run live services in this task.

**Commit boundary:** `test: add isolated local Caddy ingress canary harness`

The commit must include the tested harness and be clean before Task 21 starts.

### Task 21: Execute the committed isolated canary

**Objective:** Prove the exact reviewed source works end-to-end without touching production
services or changing tracked source.

**Files:** none. Evidence is written only to an explicitly configured directory outside the
repository.

Preflight must:

1. require `git status --porcelain` to be empty;
2. select the exact harness commit or an immutable archive produced from it;
3. resolve Caddy v2.11.7 plus the selected PostgreSQL, Temporal and Crusher acceptance images
   to exact immutable digests, record those digests as harness inputs, reject mutable tags,
   and fail if the Caddy digest does not identify v2.11.7;
4. create an external evidence directory and prove it is outside the repository;
5. verify no production-named resource collides with the canary namespace.

From that exact source and digest set:

1. create unique `fstack-caddy-canary-<nonce>` names for PostgreSQL database/container,
   Temporal namespace/task queue/container, one Docker network, Crusher containers, Caddy
   container/config/volume/socket directory and Fstack worker/API containers;
2. assign Caddy a fixed isolated IP on that network; attach Crusher, API and worker to the
   same isolated network; configure the worker probe as
   `<fixed-caddy-ip>:<listener-port>` rather than `127.0.0.1` or worker loopback;
3. install Caddy server `fstack`, routes `[]`, the listener, and an admin socket of exact mode
   `0220` on a separate confined volume shared only with a worker that has the configured
   supplemental group;
4. set `FSTACK_CADDY_CONTAINER_NAME` to the exact isolated Caddy container, prove the bounded
   topology preflight sees both running containers on the exact network, exact Caddy probe IP,
   deterministic Crusher alias/endpoint, and a successful listener TCP connect before
   reconcile;
5. execute the maintenance-mode upgrade drill: stop/quiesce API, dispatchers and workers;
   prove process exit/no old poller; apply `20261013_0004`; start the v2-capable worker and
   dispatcher; prove v1/v2 registration and polling; only then enable API traffic and insert
   a v2 command, proving no v2 row was exposed to old code;
6. submit the unchanged POST body with a unique canonical test host and wait through the
   authenticated operation GET until `succeeded`;
7. inspect isolated Caddy and assert the exact one-route object; probe the fixed Caddy IP with
   the exact Host header and exact Crusher JSON;
8. replay the command and assert no second route/submission/audit;
9. pre-seed exact matching-route adoption together with an unrelated safe deterministic
   Fstack route and prove adoption without mutation;
10. run independent valid isolated real-Caddy configurations for:
    - expected ID with different exact content -> `caddy_route_id_conflict`;
    - exact expected ID plus same-host different ID -> `caddy_host_conflict`, where Caddy
      accepts that configuration as valid;
    - same host under another ID -> `caddy_host_conflict`;
    - wildcard host matching the desired host -> `caddy_host_conflict`;
    - catch-all/no-host route -> `caddy_routes_ambiguous`;
    - another unclassifiable but valid Caddy route -> `caddy_routes_ambiguous`;
    - missing required routes prerequisite -> `caddy_prerequisite_missing`;
    Duplicate expected IDs remain covered by fake Unix admin/unit protocol tests only; do not
    ask real Caddy to load that invalid base configuration.
11. after each permanent conflict, require byte-identical Caddy config with no append,
    rewrite or repair;
12. exercise exhaustive `100`-`599` classification for direct routes GET separately in
    pre-mutation and post-mutation contexts, parent servers GET in both contexts, ID GET before
    mutation, ID GET after ambiguous POST, and POST, plus malformed/out-of-range status lines,
    every parent `200` object shape, concurrent prerequisite disappearance, and direct
    post-mutation readback after POST `200`, POST `400`/`404`, and exact ID-based ambiguity
    resolution; prove unresolved/invalid post-mutation confirmation is
    `caddy_mutation_ambiguous` and retain no bodies;
13. run a permanent topology mismatch using a wrong Caddy network/IP or missing deterministic
    Crusher alias, prove `caddy_topology_invalid`, prove reconcile was never called, and
    compare the complete Caddy config byte-for-byte before and after;
14. write command versions, exact source SHA/archive digest, all four image digests, migration
    head, test outputs and sanitized state evidence only to the external evidence directory;
15. in `finally`, remove every canary resource and verify by exact-name queries that none
    remain; then prove `git status --porcelain` is still empty.

The harness must never use default production database names, Temporal namespace/task queue,
Docker network, Caddy socket/config or existing containers. It must not print secrets,
provider bodies, socket paths, container IDs or route JSON.

Run only after the harness commit is reviewed and the immutable digests are resolved:

```bash
test -z "$(git status --porcelain)"
uv run python tools/run_local_caddy_ingress_canary.py \
  --source-commit "$(git rev-parse HEAD)" \
  --caddy-image-digest "$FSTACK_CANARY_CADDY_IMAGE_DIGEST" \
  --postgres-image-digest "$FSTACK_CANARY_POSTGRES_IMAGE_DIGEST" \
  --temporal-image-digest "$FSTACK_CANARY_TEMPORAL_IMAGE_DIGEST" \
  --crusher-image-digest "$FSTACK_CANARY_CRUSHER_IMAGE_DIGEST" \
  --evidence-dir "$FSTACK_CANARY_EXTERNAL_EVIDENCE_DIR"
test -z "$(git status --porcelain)"
```

Expected: exit 0, short sanitized summary, complete cleanup, unchanged clean tree. No commit
is made from live execution.

### Task 22: Review, PR and merge evidence

**Objective:** Deliver reviewable proof without weakening any acceptance criterion.

**Files:** none unless review finds a defect; every repair starts with RED.

Before opening the PR:

```bash
git status --short
git diff --stat main...HEAD
git diff --check main...HEAD
git log --oneline --decorate main..HEAD
```

PR evidence must include:

- exact source commit/archive used by the canary;
- Caddy version exactly v2.11.7 and the exact immutable Caddy, PostgreSQL, Temporal and
  Crusher image digests supplied to the canary;
- migration head and roundtrip commands;
- PostgreSQL gate result;
- Temporal v1 replay result and v2 workflow test result;
- unit/integration/full-suite totals;
- Ruff, format, mypy, lock and secret-scan results;
- live canary result and cleanup verification;
- proof the harness was committed/reviewed before execution and the tree was clean before and
  after, with evidence stored outside tracked source;
- explicit statement that production PostgreSQL, Temporal, Docker and Caddy were untouched;
- explicit statement that DNS, TLS, route mutation beyond one append, update/delete,
  rollback/compensation and generic adapters remain excluded.

Request a security-focused review of Unix-socket confinement, HTTP framing, Caddy conflict
classification, durable sanitization and migration downgrade behavior. Request a separate
workflow/replay review. Merge only after required CI and both reviews pass; record the merge
commit and post-merge CI URL in the delivery report.

---

## Final verification checklist

- [ ] Only migration `20261013_0004` changes schema; released migrations are byte-identical.
- [ ] New operations dispatch v2; old v1 rows/history replay unchanged.
- [ ] Downgrade rejects any v2 dispatch row, including pending/undelivered, and any ingress
  row.
- [ ] Upgrade locks only pre-existing dependency/trigger-owning tables before DDL; downgrade
  adds `ingress_submissions`; `NOWAIT` is fail-closed where feasible and functions are not
  treated as tables.
- [ ] Maintenance-mode sequencing stops API/dispatchers/workers, proves no old poller,
  applies `0004`, starts and verifies v2-capable polling, then enables API; no v2 row is
  exposed to old code.
- [ ] Task 2 proves only database cutover invariants and assumes the maintenance gate; live
  process-exit/poller-shutdown proof exists only in Tasks 18 and 21.
- [ ] PostgreSQL has one immutable ingress submission and no retry/scheduler authority.
- [ ] V2 preparation atomically prepares/replays both submissions, marks the operation
  running and inserts/replays the exact two-ID started audit; v1 prepare/mark-running remains
  unchanged.
- [ ] Runtime is internally ready before Caddy work.
- [ ] Exactly five required Caddy worker environment variables are validated, including the
  canonical fixed installation `FSTACK_CADDY_CONTAINER_NAME`; none is request data.
- [ ] Before any Caddy admin mutation, bounded preflight proves the configured Caddy container
  and deterministic Crusher API container are running on the exact `FSTACK_DOCKER_NETWORK`,
  Caddy's network IP equals the probe IP, the Crusher endpoint exposes the deterministic
  name/alias, and the listener accepts TCP.
- [ ] Topology mismatch is permanent `caddy_topology_invalid`; unavailable/malformed Docker
  inspection, non-running Caddy, and listener-connect failure are retryable
  `caddy_unavailable`; reconcile is never called after preflight failure.
- [ ] The entire routes array is safely classified before adoption or creation; an expected
  ID is adopted only when unique and exact, with no exact/wildcard host competitor,
  catch-all or ambiguous route; safely unrelated routes are exact deterministic Fstack
  shapes.
- [ ] Direct routes GET `400`/`404` performs bounded parent servers inspection without reading
  an error body; parent object/status classification distinguishes a missing prerequisite in
  both pre-mutation and post-mutation contexts, and parent-only evidence never authorizes
  mutation without a direct routes `Etag`.
- [ ] Duplicate expected IDs and changed exact-ID content are `caddy_route_id_conflict`;
  exact/wildcard host competitors are `caddy_host_conflict`; catch-all/unclassifiable routes
  are `caddy_routes_ambiguous`; all permanent conflicts are untouched.
- [ ] Mutation is one POST with `If-Match`; no shared-config replacement exists.
- [ ] Direct routes GET has separate exhaustive `100`-`599` tables plus malformed/out-of-range
  status-line tests in pre-mutation and post-mutation contexts; ID GET before mutation, ID GET
  after ambiguous POST, and POST have the same exhaustive coverage; parent servers GET has
  exhaustive status coverage in both contexts plus every `200` object shape; every
  classification matches the frozen closed mapping and never retains a body.
- [ ] POST `200`, POST `400`/`404` readback, and ambiguous exact-ID readback use the
  post-mutation direct routes mapping and require exact unique-route plus safe whole-array
  confirmation; unresolved/invalid confirmation is `caddy_mutation_ambiguous`, while parent
  proof of a missing prerequisite remains `caddy_prerequisite_missing`; `412` restarts from
  inspection without an in-activity mutation retry.
- [ ] Admin socket access is permissioned, descriptor-relative, no-symlink and revalidated
  before every request; socket mode is exactly `0220`, every other mode is rejected, the
  worker has the configured group, and the trusted root is not group/other writable.
- [ ] Admin and probe HTTP each use one absolute monotonic deadline across connect, complete
  send, headers, body/chunks/trailers, EOF and trailing-byte validation, with slow-phase
  tests and no reset.
- [ ] Caddy and workloads share one exact required Docker data-plane network; exact Docker
  fixtures prove Caddy IP/probe agreement and deterministic Crusher alias/endpoint evidence,
  while the admin socket uses a separate confined volume.
- [ ] Listener probe uses the fixed Caddy IP rather than worker loopback, exact Host and exact
  bounded Crusher health response.
- [ ] Final success atomically confirms runtime, ingress, operation and audit.
- [ ] V2 failure payload has only operation ID, scope and sanitized code; the finalizer
  resolves submissions by operation ID, and pre-durable preparation failures remain
  retryable.
- [ ] POST and request fingerprint remain stable; no deployment read endpoint was added.
- [ ] No forbidden provider/secret/path/config/fingerprint detail crosses v2/new or public
  boundaries; exact released v1 history and audit shapes remain replayable without broader
  exposure.
- [ ] Canary harness is tested/reviewed/committed before execution; live execution uses that
  exact clean commit/archive, writes evidence outside tracked source, and leaves the tree
  clean.
- [ ] Canary inputs record exact immutable Caddy, PostgreSQL, Temporal and Crusher image
  digests resolved before execution; no guessed digest is hard-coded.
- [ ] The certified installation baseline is Caddy v2.11.7 at the exact canary-recorded
  digest; no other version/digest is called compatible until it passes re-certification, and
  support is never inferred from a tag or major version.
- [ ] Canary proves all frozen Caddy adoption/conflict/status cases return exact sanitized
  results and preserve byte-identical isolated configs for permanent conflicts.
- [ ] Real Caddy canaries never load or call duplicate expected IDs valid; that conflict stays
  in fake Unix admin/unit protocol tests while real valid-config cases cover changed same ID,
  exact ID plus same-host different ID where valid, same host/different ID, wildcard,
  catch-all, unclassifiable valid routes, and missing prerequisite.
- [ ] Canary proves topology-preflight success and a permanent topology mismatch that never
  invokes reconcile and leaves the complete Caddy config byte-identical.
- [ ] Migration, PostgreSQL, replay, static, secret, full-test and isolated live-canary gates
  pass.
- [ ] Canary resources are completely removed and production services were untouched.
