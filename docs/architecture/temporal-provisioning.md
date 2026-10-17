# Temporal provisioning architecture

Status: active implementation contract

## Decision

Temporal is the sole durable workflow-execution system for Fstack provisioning.
PostgreSQL remains the business system of record, while the selected runtime
provider remains responsible for workload reconciliation.

```text
Fstack API -> PostgreSQL operation
           -> transactional dispatch intent
dispatcher -> Temporal workflow -> runtime provider
                                           -> PostgreSQL result
```

Fstack does not maintain a second general-purpose scheduler beside Temporal.
There are no PostgreSQL worker leases, polling claims, heartbeats, fencing tokens
or workflow-attempt rows.

## Durable identities

Every provisioning operation has exactly one Temporal Workflow ID:

```text
provisioning/<operation UUID>
```

The workflow is started with duplicate reuse rejected and an active duplicate
resolved to the existing execution. Activity retries must therefore reuse the
same operation UUID and the same deterministic runtime-submission identity.

## Ownership boundaries

Temporal owns:

- workflow progress and replay;
- activity attempts, retries, timers and cancellation;
- crash recovery and long-running waits;
- orchestration across bindings, runtime submission and finalization.

PostgreSQL owns:

- organizations, businesses, environments and deployments;
- the public provisioning operation and its sanitized status;
- immutable desired state and bindings;
- the transactional workflow-dispatch intent and its delivered marker;
- a minimal idempotent runtime-submission record;
- product audit events and terminal business outcomes.

The runtime provider owns:

- container or allocation lifecycle;
- placement and restart behavior;
- health and readiness;
- provider-native deployment reconciliation.

## Local Docker runtime vertical slice

`ProvisionCrusherWorkflow` orchestrates prepare, local-Docker application/readiness,
and terminal PostgreSQL finalization. Preparation only seals or replays the exact
runtime submission. The apply activity atomically marks the operation running, stages
the approved local-file database bundle, reconciles deterministic migration and API
containers, records the API container identity immediately after creation/adoption,
then requires migration success, application-role PostgreSQL connectivity, Docker
health, and the exact Crusher `/health` response. A separate activity atomically
confirms the submission, succeeds the operation, and appends the product audit event.
Permanent provider/domain failures are finalized once through the corresponding atomic
failure activity; retryable failures remain Temporal activity failures and preserve any
submitted provider identity for reconciliation.

The synchronous apply activity has a 30-minute Start-to-Close timeout. Its Docker boundary
centralizes a 1,710-second declared worst-case call budget (30-second ordinary calls and
600-second pull/wait calls), and the workflow contract requires the activity timeout to
remain strictly greater than that budget so Temporal cannot retry while a bounded call path
is still executing.

Docker reconciliation validates the requested immutable image digest before mutation
and compares existing deterministic-name resources against the complete sealed spec and
exact Fstack labels. Matching resources are replay-safe. Unmanaged or mismatched
resources fail closed and are never deleted or replaced. Provider output and exception
text are discarded in favor of bounded sanitized codes. Known domain conflicts are
nonretryable; unexpected database, state, and provider failures cross every activity
boundary as the generic retryable `activity_infrastructure_failure` error.

The sealed comparison includes namespace/cgroup mode, memory/OOM/PID limits, CPU controls,
block-I/O and device rules, runtime/shared-memory/ulimit settings, and the exact masked and
read-only path lists. Docker 29 `null` is equivalent to an explicit empty list only for the
documented nullable list fields and to sealed `false` only for `OomKillDisable`; missing
non-equivalent fields, wrong scalar types, and extra nested resource fields fail inspection.

The local secret provider is intentionally narrow: database references must use
`secret://local-file/<segments>`, resolved beneath `FSTACK_LOCAL_SECRET_ROOT` without
following symlinks. Exact owner/application JSON is copied privately to
`FSTACK_SECRET_STAGE_ROOT/<deployment hex>/` and mounted read-only. Secret values never
enter PostgreSQL, Temporal payloads, Docker labels/commands/environment overrides, logs,
or public API responses. Object-storage references are not resolved in this slice.

The API transaction creates the operation and one exact workflow-dispatch intent together.
The Temporal worker continuously drains pending intents, starts deterministic workflow
`provisioning/<operation UUID>`, and marks the intent dispatched only after Temporal accepts
the start. A crash after Temporal acceptance but before the PostgreSQL marker causes a safe
duplicate start against the same Workflow ID; an active duplicate resolves to the existing
execution. No PostgreSQL lease, attempt or retry scheduler is introduced.

## Migration lineage

`20261004_0002` is released history and remains byte-identical. It contains the former
PostgreSQL lifecycle fields, so installations that already applied it cannot receive the
Temporal authority model by rewriting that revision. Successor revision `20261006_0003`
performs the cutover and creates both `runtime_submissions` and `workflow_dispatches`.

The cutover takes fail-fast exclusive locks before inspecting or changing authority. It
accepts only pristine pending operations with requested-only audit history; advanced legacy
attempt history aborts the migration transaction instead of being silently reinterpreted as
a Temporal execution. Existing pristine operations receive deterministic pending dispatch
intents during the same migration. Downgrade likewise fails closed once Temporal dispatch or
runtime-submission history exists.

The runtime record stores provider, generation, deterministic submission key, sealed
specification fingerprint, deterministic resource names, provider external identity,
sanitized terminal failure and lifecycle timestamps. It does not own retries or
workflow progress. Runtime application, readiness, and terminal PostgreSQL finalization
are now Temporal activities; PostgreSQL still contains no attempts, leases, heartbeats,
retry scheduler, or polling claims.

## Installation-managed Caddy ingress

Fstack manages only its deterministic route inside an installation-owned Caddy
configuration. It does not install, upgrade, restart, or otherwise own Caddy. The supported
production topology is one explicitly configured Docker container with:

- Caddy v2.11.7 at the exact immutable image digest certified by the release canary;
- an existing `apps.http.servers.fstack` object and `routes` array;
- exactly one final installation-owned `fstack-unclaimed-host` terminal static-response route
  whose strictly typed JSON returns status `404` and body `not found\n`;
- at least one installation-owned listener reachable at the configured probe address;
- attachment to the exact `FSTACK_DOCKER_NETWORK` used by Crusher;
- a separately shared, confined Unix admin-socket volume.

Compatibility is limited to the canary-certified version/digest pair until another pair is
re-certified. A mutable tag, matching major version, or nominally compatible admin API is
not evidence of support. The digest is recorded from the canary; documentation must not
invent one.

The worker requires these fixed installation values, not request fields:

- `FSTACK_CADDY_ADMIN_ROOT`: normalized absolute trusted socket root;
- `FSTACK_CADDY_ADMIN_UID`: expected numeric socket owner;
- `FSTACK_CADDY_ADMIN_GID`: expected numeric socket group and a required worker supplemental
  group;
- `FSTACK_CADDY_PROBE_ADDRESS`: canonical IP literal and listener port on the installation
  Docker network;
- `FSTACK_CADDY_CONTAINER_NAME`: canonical fixed Caddy container name.

The only socket entry is `caddy-admin.sock`. Before every admin request, Fstack opens the
trusted root without following symlinks, requires it to be owned by root or the worker EUID,
and rejects it if group/other writable. It accepts only a Unix socket with the configured
owner/group and mode exactly `0220`; every other mode is invalid. The worker connects
descriptor-relative and revalidates the socket after connection. This boundary requires
Linux directory-descriptor and inotify primitives plus `/proc/self/fd` and fails closed when
they are unavailable. An unauthenticated TCP admin endpoint is never supported.

Caddy must resolve deterministic Crusher API names and reach port `8000`. The worker reaches
the configured listener over `FSTACK_DOCKER_NETWORK`, not through worker loopback; the admin
socket remains a separate control-plane mount. After runtime readiness and before route
reconciliation, bounded topology inspection proves that the configured Caddy container and
deterministic Crusher API container are running on the exact network, Caddy's network IP
matches the probe IP, the Crusher endpoint advertises its deterministic name or alias, and
the listener accepts TCP. Structurally valid but unsupported topology, network/IP/alias
mismatch, or a missing or non-running Crusher container is permanent
`caddy_topology_invalid`. Unavailable or malformed Docker/provider inspection,
missing/non-running Caddy, or an unavailable listener is retryable `caddy_unavailable`.

The v2 workflow order is:

1. atomically prepare or replay the exact runtime and ingress submission identities,
   transition the operation from `pending` to `running`, and insert or replay the exact v2
   `provisioning.started` audit;
2. apply and prove the runtime internally ready;
3. preflight the exact Dockerized-Caddy topology and listener without mutating Caddy;
4. reconcile or adopt the exact route;
5. probe the installation-managed listener with the exact Host header;
6. atomically finalize runtime submission, ingress submission, operation, and audit.

Public `succeeded` therefore means both runtime and ingress are durably complete. The
released v1 workflow remains registered and its frozen histories remain replayable; old rows
continue to select v1 from their persisted workflow type.

Routed HTTP readiness is deliberately narrower than external availability. It does not prove
or manage DNS, DNS ownership, ACME, TLS, HTTPS, certificates, load balancers, firewalls, or
Internet reachability. The slice also excludes Caddy installation/startup/supervision or
listener creation; creation/replacement of the `fstack` server or routes array; generic
ingress/provider adapter frameworks; route update, delete, reorder, domain change, rollback,
or compensation; container supervision or replacement; object-storage resolution; and
portal/UI, billing, or Remesher support. Host-process Caddy, Kubernetes, Podman, remote Caddy,
implicit Caddy discovery, arbitrary server selection, and per-request Caddy configuration
are unsupported.

## Test boundary

The public repository includes:

- workflow determinism and execution tests using Temporal's test environment;
- PostgreSQL-backed outbox creation, replay and transient-dispatch recovery tests;
- activity mapping and retry-idempotency tests;
- PostgreSQL integration proving an exact operation becomes one runtime submission;
- provider contract tests that require no production credentials.

Managed-fleet, scale, chaos and production-provider certification remain separate
from the public unit and contract suite.
