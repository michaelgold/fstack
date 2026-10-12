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

## Initial vertical slice

`ProvisionCrusherWorkflow` currently invokes one idempotent activity that prepares
or replays the exact PostgreSQL-backed Crusher runtime submission. The activity is
safe to execute again after an ambiguous completion because it addresses the exact
operation and returns the existing deterministic submission instead of creating a
second runtime submission.

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
workflow progress. The next slice moves runtime application, readiness and terminal
PostgreSQL finalization into Temporal activities.

## Test boundary

The public repository includes:

- workflow determinism and execution tests using Temporal's test environment;
- PostgreSQL-backed outbox creation, replay and transient-dispatch recovery tests;
- activity mapping and retry-idempotency tests;
- PostgreSQL integration proving an exact operation becomes one runtime submission;
- provider contract tests that require no production credentials.

Managed-fleet, scale, chaos and production-provider certification remain separate
from the public unit and contract suite.
