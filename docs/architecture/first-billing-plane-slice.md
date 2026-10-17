# First billing-plane slice

Status: frozen acceptance contract for the first managed billing implementation.

## Product outcome

A managed Fstack installation supports one provider-neutral Crusher plan, one active subscription per organization, one deployment quota, one immutable usage fact, and deterministic Lago reconciliation.

The first plan is:

```text
plan_key: crusher.single.v1
capability: crusher.deployment.create
quota_dimension: crusher.deployment.active
quota_limit: 1
usage_metric: crusher.deployment.activated
usage_unit: deployment
usage_quantity: 1
```

An organization with a current active entitlement may provision one Crusher deployment. An inactive, stale or unavailable entitlement blocks new provisioning without affecting an already accepted operation or an existing workload. Private subscription contradiction prevents fresh revisions and therefore fails closed when the last authoritative revision expires. Joint runtime-and-ingress success emits exactly one deployment-activation usage fact.

This deployment-activation fact is the first executable billing event because Fstack already owns its exact durable success boundary. Per-email metering requires a separate Crusher effect-ledger integration and is explicitly deferred rather than inferred from logs or aggregate counters.

## Ownership boundary

Public Fstack owns only provider-neutral contracts and enforcement:

- installation billing mode;
- entitlement subjects and immutable entitlement revisions;
- provisioning authorization and quota consumption;
- immutable usage facts;
- authenticated installation-local entitlement and usage interfaces;
- stable provisioning denial codes.

Private `fstack-cloud` owns commercial policy and provider integration:

- prices, currency, billing period and tax policy;
- Lago customer, plan, subscription, metric, event and invoice identifiers;
- the exact supported Lago API version and credentials;
- subscription synchronization and commercial state interpretation;
- usage export payloads, attempts, receipts and reconciliation;
- payment-provider and future portal behavior.

No `lago_*`, price, currency, invoice, wallet, payment-method or provider-response field may enter public Fstack tables, public API models, Temporal payloads, audit payloads or logs.

Temporal remains the sole retry and orchestration authority. PostgreSQL stores business facts, exact idempotent submission evidence and audits; it does not gain a second lease, attempt or polling workflow engine.

## Installation modes

Fstack has one exact environment setting:

```text
FSTACK_ENTITLEMENT_MODE=disabled|required
```

- Unset or empty defaults to `disabled`, preserving every existing and self-hosted installation.
- Any nonempty value other than the exact lowercase strings `disabled` and `required` is a
  startup error; the API and worker do not start under an ambiguous value.
- `disabled` preserves self-hosted operation and the released provisioning contract without billing infrastructure.
- `required` is the managed-cloud mode. Every genuinely new provisioning operation requires a current entitlement and available quota.
- The mode is installation configuration, not caller input, and cannot vary per request.
- Changing the mode does not rewrite prior operations or retrofit authorizations onto existing deployments.
- `required` startup additionally requires nonempty `FSTACK_BILLING_RECONCILE_TOKEN` and
  `FSTACK_BILLING_EXPORT_TOKEN`. Each is 32-200 visible ASCII bytes, they must differ from each
  other and from `FSTACK_OPERATOR_TOKEN`, and their values are never logged. Billing endpoints
  return `404` in `disabled` mode even when billing-token variables are present.

## Provider-neutral identities

All identifiers are UUIDs unless an existing released identifier is named.

- `entitlement_subject_id`: stable Fstack identity for one canonical organization slug.
- `entitlement_revision_id`: immutable revision identity.
- `entitlement_generation`: positive, monotonically increasing integer within one subject.
- `authorization_id`: immutable binding between one operation and one entitlement revision.
- `operation_id`: released provisioning operation UUID.
- `deployment_id`: released desired deployment UUID.
- `usage_fact_id`: pre-generated immutable Fstack usage identity stored on the required-mode
  authorization and used by any later success fact and by the private adapter as Lago's
  transaction identity.

Provider IDs are never reused as these public identities.

## Entitlement contract

### Subject

The first subject type is exactly `organization_slug`. The subject key is the released canonical lowercase organization slug. A stable subject row exists to provide one PostgreSQL serialization lock and does not contain commercial state. PostgreSQL rejects deletion and any update to the subject ID, type, key or creation timestamp; the slug-to-subject binding is permanent.

### Immutable revision

A revision contains exactly the provider-neutral decision fields:

- `entitlement_revision_id`;
- `entitlement_subject_id`;
- `generation`;
- `plan_key`, initially exactly `crusher.single.v1`;
- `state`: `active` or `inactive`;
- `capability`, initially exactly `crusher.deployment.create`;
- `quota_dimension`, initially exactly `crusher.deployment.active`;
- `quota_limit`, initially exactly `1`;
- `observed_at`;
- `valid_until`;
- `source_revision_id`, an opaque UUID generated by `fstack-cloud` for idempotent synchronization;
- `recorded_at`;
- a canonical lowercase hexadecimal SHA-256 payload digest.

Revisions are append-only. Updates and deletes are rejected by PostgreSQL. `(entitlement_subject_id, generation)` and `source_revision_id` are unique. A repeated identical source revision adopts the existing row. Reuse with different canonical content is a conflict.

### Canonical revision encoding

The digest domain separator is `fstack.entitlement-revision.v1`. The following block is the
exact field inventory, not serialization order; the serializer sorts these keys
lexicographically:

```text
discriminator
entitlement_revision_id
entitlement_subject_id
generation
plan_key
state
capability
quota_dimension
quota_limit
observed_at
valid_until
source_revision_id
```

UUIDs use lowercase canonical hyphenated text. Integers use JSON numbers. Timestamps are UTC
RFC 3339 strings with exactly six fractional digits and a terminal `Z`, for example
`2026-10-17T15:06:02.123456Z`. JSON is UTF-8, keys are sorted lexicographically, separators
are exactly `,` and `:`, non-ASCII text is not escaped, and no insignificant whitespace or
trailing newline is present. The digest is lowercase hexadecimal SHA-256 of those exact bytes.
`recorded_at` and the digest field itself are excluded.

`valid_until` must be later than `observed_at` and is bounded by installation policy. The first managed deployment uses a maximum validity window of five minutes. Fstack does not infer activity beyond `valid_until` and does not contact Lago on the provisioning request path.

### Entitlement synchronization interface

The exact installation-local endpoint is:

```text
PUT /internal/billing/entitlement-revisions
Authorization: Bearer <FSTACK_BILLING_RECONCILE_TOKEN>
Content-Type: application/json
```

The request body contains exactly the canonical revision fields listed above except
`discriminator` and `entitlement_subject_id`, which Fstack supplies, plus the exact canonical
`organization_slug` used to locate or create the stable subject. Fstack alone allocates a new
subject UUID. The caller never proposes or selects it. Unknown fields,
repeated JSON keys, noncanonical UUID/timestamp text, invalid enum values, a non-increasing
new generation, and a validity window over five minutes are rejected.

Subject creation uses the released canonical organization slug and PostgreSQL unique
arbitration: generate a candidate subject UUID, execute `INSERT ... ON CONFLICT (subject_type,
subject_key) DO NOTHING RETURNING id`, then select the committed exact subject row `FOR UPDATE`.
A concurrent first synchronization either wins that insert or waits and adopts the winner's
subject UUID. A slug can never be rebound to another subject, and because callers cannot submit
subject IDs there is no slug/ID disagreement path.

Responses are:

- `201` with `{ "entitlement_subject_id": "<uuid>", "entitlement_revision_id": "<uuid>", "replayed": false }` after a new row commits;
- `200` with both same IDs and `replayed: true` for byte-identical `source_revision_id` replay;
- `400 invalid_entitlement_revision` for malformed or noncanonical input;
- `401 authentication_required` before database access when the dedicated reconcile token is
  absent or wrong; the general operator token and export token are never accepted here;
- `409 entitlement_revision_conflict` for same source ID or same subject generation with different canonical bytes, or a new generation not greater than current;
- `503 entitlement_persistence_unavailable` for sanitized unknown persistence failures.

A conflict leaves every previously committed subject and revision byte-for-byte unchanged and
has no durable conflict marker. The private controller records provider divergence privately and
stops issuing fresh revisions until reconciliation is authoritative.

### Current decision

Under the subject serialization lock, the current revision is the highest generation whose canonical row is unambiguous. Provisioning is permitted only when all are true:

- installation mode is `required`;
- the subject exists for the exact requested organization slug;
- the current revision is `active`;
- plan, capability and quota dimension equal the frozen values above;
- the database clock is not later than `valid_until`;
- quota consumption is below `1`.

These current-decision rules apply only when `entitlement_mode=required`. In `disabled`
mode the released self-hosted admission path remains authoritative and no entitlement
subject, revision or authorization is required.

Missing, stale or unreadable required-mode state is unavailable, never implicitly active.
Conflicting same-generation or same-source synchronization requests are rejected by the
synchronization request itself and do not mutate or poison the previously committed current
revision. Private subscription ambiguity or reconciliation divergence prevents the controller
from issuing a fresh revision; the last revision remains usable only until its existing
`valid_until`.

## Provisioning admission and replay

The released `POST /v1/provisioning/operations` request and fingerprint remain unchanged. Billing fields are not accepted from callers.

The service opens one PostgreSQL transaction and preserves the released operation-claim arbitration:

1. Validate authentication, canonical input and the exact idempotency key.
2. Attempt the released conflict-safe provisioning-operation claim.
3. If the claim loses and the committed fingerprint matches, return the existing operation immediately as an idempotent replay. Replay does not require a current entitlement, consume another authorization or contact `fstack-cloud` or Lago.
4. If the claim wins and `entitlement_mode=disabled`, continue with the released inserts and persist `authorization_mode=not_required`.
5. If the claim wins and `entitlement_mode=required`, lock the exact entitlement subject row, resolve the current immutable revision and count quota consumption under the same lock.
6. Insert one immutable authorization binding the generated `authorization_id`, pre-generated
   `usage_fact_id`, exact revision, operation, deployment, capability, quota dimension, units
   `1`, request fingerprint and decision timestamp.
7. Insert the released organization, business, environment, workload, deployment, bindings, `provisioning.requested` audit and exact v2 Temporal dispatch intent.
8. Commit once.

Any denial or failure before commit rolls back the claim and every resource, authorization, audit and dispatch row. No Docker, Caddy, Temporal or provider call occurs.

Every committed required-mode authorization consumes its unit permanently in this slice,
including when its operation later fails. The released API has no organization replacement or
retry-with-new-operation path, and this contract does not invent one implicitly. An identical
idempotent replay returns the original operation. Capacity release, operation replacement and
deprovisioning are later lifecycle slices.

Authorization rows are append-only evidence. PostgreSQL rejects every authorization update and
delete, including changes to subject, revision, operation, deployment, capability, quota,
request fingerprint and frozen `usage_fact_id`. Quota reads and finalizer replay rely only on
these protected rows.

Concurrent requests for the same subject serialize on the stable subject row. At most one authorization whose operation consumes quota can commit. A pre-check without this database lock is not accepted.

### Stable errors

Managed provisioning maps decisions to the existing stable error envelope:

- `402 subscription_inactive`: a current authoritative revision is `inactive`;
- `409 entitlement_quota_exceeded`: the one active-deployment unit is consumed;
- `503 entitlement_state_unavailable`: the subject or revision is missing, stale, contradictory or unreadable.

Errors contain no provider ID, revision payload, request fingerprint, submitted domain, secret reference, database detail or commercial policy.

Once authorization commits, the released Temporal workflow proceeds without another billing check. Later expiry, cancellation or provider outage cannot strand the accepted workflow.

## Immutable usage fact

The first event discriminator is:

```text
fstack.usage-fact.v1
```

The first metric is exactly `crusher.deployment.activated`. One fact contains:

- `usage_fact_id`;
- `entitlement_subject_id`;
- `entitlement_revision_id`;
- `authorization_id`;
- `organization_id`;
- `operation_id`;
- `deployment_id`;
- `metric`;
- `unit = deployment`;
- `quantity = 1`;
- `source_event = provisioning.succeeded`;
- `occurred_at`, equal to the joint runtime-and-ingress confirmation time;
- `recorded_at`;
- `schema_version = 1`;
- canonical payload digest.

The usage digest domain separator is `fstack.usage-fact.v1`. The following block is the exact
field inventory, not serialization order; the serializer sorts these keys lexicographically:

```text
discriminator
usage_fact_id
entitlement_subject_id
entitlement_revision_id
authorization_id
organization_id
operation_id
deployment_id
metric
unit
quantity
source_event
occurred_at
schema_version
```

UUID, timestamp, integer and JSON encodings are exactly the canonical encodings defined for
entitlement revisions. `recorded_at` and the digest field are excluded. The finalizer uses the
`usage_fact_id` frozen on the authorization at provisioning admission, so replay compares one
stable identity and payload rather than generating a new identity or reconstructing identity
from mutable rows.

Only a required-mode operation with an exact authorization emits this billing usage fact.
Disabled-mode operations preserve released finalization and emit no billing fact.

The v2 joint-success finalizer samples one `clock_timestamp()` boundary before its terminal
writes and reuses that exact value for runtime `confirmed_at`, ingress `confirmed_at`, operation
`completed_at` and `updated_at`, and usage `occurred_at`. The fact is inserted in the same
PostgreSQL transaction that performs those writes and inserts the `provisioning.succeeded`
audit event.

Exactly one fact may exist for `(operation_id, metric)` and `(deployment_id, metric)`. A finalizer replay must verify exact agreement and adopt the existing fact. Any missing, partial or mismatched bundle is a sanitized retryable infrastructure failure. A failed operation emits no activation fact.

Usage facts are append-only. PostgreSQL rejects updates and deletes. Corrections, reversals and deprovisioning facts are deferred; no actor may rewrite invoice evidence.

Logs, metrics, Temporal histories, container state and aggregate counters are never billing evidence.

## Installation-local usage interface

The exact endpoint is:

```text
GET /internal/billing/usage-facts?limit=<1-100>&cursor=<optional opaque cursor>
Authorization: Bearer <FSTACK_BILLING_EXPORT_TOKEN>
```

`limit` defaults to `100`. Repeated parameters, unknown parameters, padding-bearing or
noncanonical cursors, and out-of-range limits return `400 invalid_usage_cursor` before database
access. The public `api.fstack.com` ingress must intercept `/internal/billing/*` and return `404`;
only direct installation-local access is supported.

The cursor is unpadded base64url of canonical UTF-8 JSON with exactly:

```json
{"recorded_at":"2026-10-17T15:06:02.123456Z","usage_fact_id":"00000000-0000-0000-0000-000000000000"}
```

The timestamp and UUID use the canonical forms above. Ordering is ascending
`(recorded_at, usage_fact_id)`, and the supplied cursor is exclusive. The endpoint selects at
most `limit + 1` rows in one PostgreSQL read transaction, returns at most `limit`, and emits a
`next_cursor` only when another row was observed. A fact that was uncommitted during an earlier
page can later sort behind an advanced cursor; therefore each reconciliation run must perform a
bounded-period rescan and deduplicate immutable `usage_fact_id` values rather than treating one
forward cursor pass as complete billing evidence.

A `200` response is exactly:

```json
{
  "facts": [
    {
      "usage_fact_id": "<uuid>",
      "entitlement_subject_id": "<uuid>",
      "entitlement_revision_id": "<uuid>",
      "authorization_id": "<uuid>",
      "organization_id": "<uuid>",
      "operation_id": "<uuid>",
      "deployment_id": "<uuid>",
      "metric": "crusher.deployment.activated",
      "unit": "deployment",
      "quantity": 1,
      "source_event": "provisioning.succeeded",
      "occurred_at": "<canonical timestamp>",
      "recorded_at": "<canonical timestamp>",
      "schema_version": 1,
      "payload_digest": "<64 lowercase hex>"
    }
  ],
  "next_cursor": "<opaque cursor or null>"
}
```

A missing or wrong dedicated export token returns `401 authentication_required` before database
access; the general operator token and reconcile token are never accepted here. Unknown read
failures return `503 usage_facts_unavailable` without a cursor or partial page. No response
contains secret references, provider configuration or commercial data.

A cursor is transport progress, not billing truth. The private reconciler rescans each bounded
billing period from its first cursor and compares exact immutable fact identities; Fstack never
marks a fact billed or mutates it after export.

## Private Lago mapping

For this slice, `fstack-cloud` maps:

```text
plan_key = crusher.single.v1
metric = crusher.deployment.activated
transaction_id = usage_fact_id
quantity = 1
occurred_at = usage_fact.occurred_at
```

The private controller resolves its Lago-only subscription and metric identifiers, constructs one
provider request, and stores the exact UTF-8 request bytes, SHA-256 digest, content type and
certified adapter version before external I/O. Those bytes are immutable after the first attempt;
all retries transmit the stored bytes rather than regenerating JSON. Public Fstack does not define
or store this provider payload.

Temporal workflow ID is deterministic:

```text
billing-usage/<usage_fact_id>
```

Provider submission uses a durable pre-I/O uncertainty fence. In one private-database
transaction, the workflow appends an immutable export-attempt row and changes the fact's export
projection from `pending` to `reconciliation_required` before opening the HTTP request or writing
its first byte. The row records the attempt UUID, usage fact, stored-request digest, Temporal run
identity and start time, but no retry lease or scheduling authority. Every workflow re-entry that
observes `reconciliation_required` performs transaction-ID lookup before any submission.

Provider outcomes are classified at the request-byte boundary:

- a proven failure before any HTTP request byte is written may append that outcome and transition
  the projection back to `pending`, permitting a later resend of the stored bytes;
- an exact certified success appends its receipt and transitions to `accepted` atomically;
- a certified permanent rejection appends its receipt and transitions to `permanent_rejection`;
- timeout, connection loss after any request byte, provider `5xx`, malformed success, duplicate
  disagreement, or acceptance followed by local persistence uncertainty leaves the precommitted
  `reconciliation_required` fence in place.

In `reconciliation_required`, the same deterministic Temporal workflow performs only an
authoritative Lago lookup by `transaction_id`. An exact matching event transitions to
`accepted`. An authoritative not-found result transitions back to export-pending and permits one
resubmission of the exact stored bytes and same transaction ID. Lookup unavailability stays
`reconciliation_required` and retries only the lookup. A conflicting event becomes `divergent`
and is never resubmitted automatically. This behavior relies on the certified Lago API's exact
transaction-ID lookup and duplicate semantics; managed billing cannot be enabled until both are
proven by canary.

Raw provider bodies, credentials and exception text do not enter public state, Temporal payloads, customer-visible errors or logs.

## Subscription reconciliation

The private controller maintains exactly one expected Lago subscription for one private commercial account mapped to the exact Fstack entitlement subject and plan.

A deterministic reconciliation workflow:

1. reads the exact Lago customer, plan and subscription state through the certified API version;
2. rejects duplicate, contradictory or unmappable subscriptions;
3. writes a new complete provider-neutral entitlement revision to Fstack only after a successful authoritative observation;
4. exports pending immutable usage facts;
5. compares local facts and Lago events by transaction ID, subscription, metric, original occurrence time and quantity;
6. records private receipts and reconciliation items.

Reconciliation states are:

```text
pending | in_sync | lagging | provider_unavailable | divergent
```

`in_sync` requires exact agreement for every fact in the bounded period and no unexpected event for this metric and subscription. `lagging` contains only known retryable exports. Missing accepted events, conflicting payloads, unexpected events, permanent rejection or attribution disagreement is `divergent`.

Only `in_sync` may permit later automated invoice approval. Invoice approval itself is excluded from this slice.

Webhooks may accelerate reconciliation later but never replace authoritative polling and exact comparison.

## Outage and cancellation behavior

- Lago unavailable: the last entitlement revision remains durable; new provisioning is allowed only until its bounded `valid_until`, then fails with `503`. Existing workloads continue. Usage facts continue to persist and exports queue privately.
- `fstack-cloud` unavailable: new provisioning fails after the current revision expires. Existing workloads and already accepted provisioning workflows continue.
- Usage exporter unavailable: immutable facts accumulate; running workloads and accepted workflows continue.
- Reconciliation divergent: private Cloud writes no fresh active revision, so new provisioning fails closed after the last revision expires. Existing workloads continue.
- Subscription inactive: a fresh inactive revision blocks future provisioning with `402`. Existing workloads continue.
- Outage after operation commit: the exact authorized Temporal workflow continues without reauthorization.
- Fstack database unavailable: no new operation commits. Already-running provider workloads continue independently.

This slice never stops, suspends, reroutes or deletes an existing workload because of billing state.

## Acceptance mapping

1. Unset, empty and exact `disabled` mode preserve released provisioning, replay and finalization behavior and emit no billing usage fact; invalid nonempty mode prevents API and worker startup.
2. `required` mode refuses startup when either dedicated billing token is missing, malformed, equal to the other token, or equal to the general operator token.
3. `required` mode with no current revision creates no operation, authorization, audit or dispatch and returns `503`.
4. An authoritative inactive revision creates no durable provisioning state and returns `402`.
5. An active revision is usable when the database clock equals `valid_until` and unavailable on the first instant after it; application-host time is never authoritative.
6. One current active revision authorizes one operation, freezes `authorization_id` and `usage_fact_id`, and binds the exact revision atomically.
7. Two concurrent new requests under one subject cannot both commit against the one-unit quota.
8. A committed authorization remains quota-consuming after operation failure or success; the public API exposes no replacement path in this slice.
9. An identical operation replay succeeds after entitlement expiry without another authorization or external call.
10. No admission path calls Lago, `fstack-cloud`, Docker, Caddy or Temporal before the PostgreSQL operation/authorization/dispatch transaction commits.
11. Concurrent first entitlement synchronization for one slug commits one Fstack-allocated subject ID and both callers resolve that exact ID.
12. Byte-identical entitlement source replay returns the existing subject and revision; same source or generation with different canonical bytes returns `409` and leaves prior state unchanged.
13. Entitlement synchronization accepts only the dedicated reconcile token before database access and sanitizes malformed, conflict and persistence failures.
14. An outage after operation commit does not stop or reauthorize the exact v2 workflow.
15. Required-mode joint runtime-and-ingress success reuses one database boundary timestamp and commits exactly one activation fact with the success audit.
16. Finalizer replay verifies and adopts the exact frozen usage identity; it creates no second fact. Failed and disabled-mode provisioning create none.
17. PostgreSQL rejects subject deletion or identity rebinding and rejects every authorization,
entitlement-revision and usage-fact update/delete attempt; failed mutations leave subject
mapping, quota consumption and frozen usage identity unchanged.
18. Public schemas, responses, logs, audit payloads, Temporal payloads and traceback locals contain no Lago or commercial values.
19. Usage export accepts only the dedicated export token before database access; the operator and reconcile tokens fail, and public ingress returns `404` for `/internal/billing/*`.
20. Pagination over a quiescent committed fact set has no gaps or duplicates at limits `1`, an intermediate value and `100`; cursors are exclusive and noncanonical cursors fail before database access.
21. A fact that commits behind an already advanced cursor is found by the required bounded-period rescan and is deduplicated by `usage_fact_id`; transport cursors are never treated as reconciliation truth.
22. Every provider attempt commits an immutable attempt and `reconciliation_required` fence before opening the request; termination immediately after that commit, after the first request byte, or after provider acceptance but before receipt persistence always re-enters through transaction-ID lookup rather than blind resend.
23. A proven pre-write failure may return to pending; exact lookup match becomes accepted, authoritative absence permits one same-identity resend, lookup unavailability retries only lookup, and conflicting lookup becomes divergent.
24. Subscription ambiguity and reconciliation divergence issue no fresh active revision, so new provisioning fails closed after prior validity expires while existing workloads remain healthy.
25. Temporal remains the sole retry authority: public migrations add no billing attempt, lease, heartbeat or poller tables; private attempt rows are immutable evidence only, and deterministic usage-export workflow IDs own provider retry orchestration.
26. Real PostgreSQL concurrency, migration upgrade/downgrade rules, real Temporal replay, Lago timeout/duplicate/lookup/crash canaries and production deployment readback are required before enabling `required` mode.

## Migration and release rules

- Released migrations `20261013_0004` and `20261015_0005` remain byte-for-byte unchanged.
- A successor public migration adds the provider-neutral subject, immutable revision, authorization and usage-fact contracts.
- Existing operations are marked `authorization_mode=not_required`; they are never assigned synthetic paid entitlements or usage facts.
- Downgrade is permitted only before any entitlement subject, entitlement revision,
  required-mode authorization or usage fact exists. After any billing contract row exists,
  downgrade fails closed rather than deleting provider-neutral history or billing evidence.
- `fstack-cloud` uses a separate private database and migrations for commercial accounts, Lago mappings, payloads, receipts and reconciliation. It never shares or writes Lago's database.
- Managed enforcement remains disabled until exact merged revisions, certified Lago behavior, synchronization, export, reconciliation and outage canaries all pass in production.

## Explicit exclusions

- checkout, payment-method collection, Stripe integration or customer portal;
- prices, taxes, invoices, credits, coupons, refunds, dunning or email notifications;
- trials, upgrades, downgrades, proration, overages or multiple plans;
- more than one active deployment, multiple subscription selection or account merging;
- per-email, storage, CPU, bandwidth or delivery-outcome metering;
- automatic invoice approval or post-invoice correction;
- stopping or deprovisioning existing workloads after cancellation;
- retroactive authorization or usage backfill for existing deployments;
- generalized billing-provider plugins;
- webhook-only correctness;
- portal or tenant self-service APIs.
