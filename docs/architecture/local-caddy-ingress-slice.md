# Local Caddy ingress slice

Status: frozen acceptance contract for the next bounded implementation.

## Product outcome

A newly requested Crusher deployment is not successful merely because its local Docker
runtime is internally healthy. Fstack also reconciles one exact HTTP route into an
installation-managed Caddy instance and proves that the configured Caddy listener reaches
the exact Crusher `/health` response when addressed with the deployment's canonical
`public_domain` Host header.

Success means **routed listener readiness inside the installation**. It does not mean that
DNS resolves, the listener is reachable from the public Internet, TLS is configured, or a
certificate exists.

## Ownership boundary

This slice supports one production topology only: the installation-managed Caddy process is
an explicitly configured Docker container. Host-process Caddy, Kubernetes, Podman, remote
Caddy, and implicit container discovery are unsupported in this slice.

The first implementation targets the Caddy v2.11.7 admin semantics frozen below. Release
acceptance certifies one exact v2.11.7 image digest. Installation compatibility is limited to
that certified version and digest until a later version/digest is run through and passes the
same release canary; compatibility must never be inferred from a mutable tag, major version,
or nominal API similarity. This contract does not guess or hard-code the digest.

The installation owns:

- the Caddy process and service supervision;
- the permissioned Caddy admin Unix socket, its parent directory and access policy;
- Caddy persistence and resume behavior;
- the native Caddy base configuration;
- all listener addresses and network/firewall exposure;
- the existing HTTP server named exactly `fstack` and its existing `routes` array.

Fstack owns only one route object per Fstack deployment inside that preconfigured routes
array. The installation owns one exact terminal `fstack-unclaimed-host` static-response
route returning HTTP `404`; it must exist exactly once as the final array element. Fstack
does not create, replace or repair the `fstack` server, its listeners, its routes array,
Caddy storage, the installation-owned fallback, or the Caddy process.

The installation-owned fallback is exactly this JSON object, with these JSON value types
(`404` is an integer and `true` is a boolean; numeric aliases such as `404.0` or `1` are not
accepted):

```json
{
  "@id": "fstack-unclaimed-host",
  "handle": [
    {
      "handler": "static_response",
      "status_code": 404,
      "body": "not found\n"
    }
  ],
  "terminal": true
}
```

The preconfigured prerequisites are:

```text
apps.http.servers.fstack
apps.http.servers.fstack.routes
```

The server must already have at least one installation-selected HTTP listener that can be
used by the worker's configured local probe address. The configured
`FSTACK_CADDY_CONTAINER_NAME` must identify the one running installation-managed Caddy
container. That container must be attached to the exact `FSTACK_DOCKER_NETWORK`, and its
address on that network must equal the IP literal in `FSTACK_CADDY_PROBE_ADDRESS`. The
deterministic `fstack-crusher-<deployment hex>` API container must be running on that same
network, and Docker network inspection must expose that exact deterministic name or alias on
its endpoint so Caddy DNS routing is observable rather than assumed. The worker must also
complete a bounded TCP connect to the configured Caddy listener before any Caddy admin
mutation. The admin socket is shared separately through a confined installation-owned
volume; it is not a substitute for the data-plane network. Fstack verifies these topology
prerequisites after internal runtime readiness and before ingress reconciliation. A
structurally valid but missing, mismatched, or unsupported topology is a permanent
installation conflict; unavailable or malformed provider inspection is retryable. Fstack
leaves Caddy unchanged in either case.

## Security boundary for the admin endpoint

The Caddy admin endpoint is a permissioned Unix-domain socket beneath a trusted
installation-owned root. An unauthenticated TCP admin endpoint is not supported.

The worker receives installation configuration, not deployment input, for:

- an absolute trusted admin-socket root;
- the one fixed relative socket entry `caddy-admin.sock`;
- the expected numeric socket owner UID and GID;
- one local HTTP listener probe address;
- the canonical fixed installation-managed Caddy container name.

Before **every** admin request, Fstack:

1. opens the trusted root as a directory descriptor with no symlink following;
2. rejects a root that is not a directory, is not owned by root or the worker EUID, or is
   group/other writable;
3. resolves the fixed socket entry descriptor-relative with no symlink following;
4. rejects anything other than a Unix socket with the configured UID/GID and mode exactly
   `0220`; every other permission mode is invalid, and the worker must have the configured
   supplemental group membership needed to connect;
5. connects through the held root descriptor, never by joining and reopening a pathname;
6. re-stats the descriptor-relative entry after connect and rejects replacement if device,
   inode, type, owner, group or mode differs from the pre-connect evidence.

The trusted root remains confined and may not be writable by group or other. The worker
fails closed when the required Linux descriptor primitives or `/proc/self/fd` confinement
are unavailable. The trusted root, socket path, ownership evidence and provider
exception text never enter API responses, audits, v2 Temporal payloads/errors/history, logs
or telemetry. Released v1 history compatibility is governed by the narrow exception in
**Temporal authority and compatibility**; it does not permit any new path or provider text.

## Exact route identity and JSON

For deployment UUID `D`, let `hex(D)` be its 32 lowercase hexadecimal digits without
hyphens.

```text
route ID:       fstack-crusher-ingress-<hex(D)>
upstream dial:  fstack-crusher-<hex(D)>:8000
host:           deployments.public_domain
```

The route ID is deterministic and globally unique within the Caddy configuration. The
route object is exactly:

```json
{
  "@id": "fstack-crusher-ingress-<deployment hex>",
  "match": [
    {
      "host": ["<canonical public_domain>"]
    }
  ],
  "handle": [
    {
      "handler": "reverse_proxy",
      "upstreams": [
        {
          "dial": "fstack-crusher-<deployment hex>:8000"
        }
      ]
    }
  ]
}
```

No other route keys, matchers, handlers, upstream options, transport settings, header
changes, load-balancing options, health checks, TLS settings or terminal flags are present.
The route matches exactly one canonical host. The upstream is exactly the deterministic
Crusher API container name on port `8000` on the existing installation-managed Docker
network.

The route JSON has a versioned canonical fingerprint in PostgreSQL, but the JSON and
fingerprint never enter public responses, audits, v2 Temporal payloads/errors/history, logs
or telemetry.

## Caddy admin protocol

Fstack speaks bounded HTTP/1.1 directly over the verified Unix socket. Every request uses:

```text
Host: localhost
Connection: close
```

Mutations additionally use:

```text
Content-Type: application/json
Content-Length: <exact byte length>
```

The implementation accepts only HTTP/1.0 or HTTP/1.1 responses with a syntactically valid
status line and headers. Duplicate unequal `Content-Length`, simultaneous
`Content-Length` and `Transfer-Encoding`, unsupported transfer codings, malformed chunking,
oversized headers or bodies, premature EOF and trailing bytes beyond the declared frame are
provider failures. Admin response headers are capped at 64 KiB and bodies at 64 KiB. One
admin request has a 5-second connect cap inside one absolute 15-second monotonic deadline.
That single deadline begins before connect and spans connect, the complete request send,
status and headers, every body chunk and trailer, EOF, and trailing-byte validation. No
phase, retry, partial write or successful read resets it.

Caddy's native JSON admin API is used only at these scopes:

```text
GET  /config/apps/http/servers/fstack/routes
GET  /config/apps/http/servers
GET  /id/<route-id>
PUT /config/apps/http/servers/fstack/routes/0
```

The servers GET is bounded, read-only parent-prerequisite inspection used only after the
direct routes-array GET returns `400` or `404`. The direct routes-array GET must return an
array and an `Etag`; only that direct response can authorize mutation. Fstack never calls
`/load`, never writes the whole server or routes array, and never performs GET-modify-write
replacement of shared configuration. The only mutation is one PUT that inserts the exact
route object at index zero, before the required terminal fallback. It carries `If-Match`
with the direct routes-array Etag so a concurrent
overlapping change fails rather than being overwritten. Each mutation is one Caddy ACID
request.

Provider response bodies and error bodies are never copied into exceptions, logs,
telemetry, PostgreSQL failure text, audits, API responses or v2 Temporal history.

### Exhaustive status classification

Status classification is a closed function over every integer status from `100` through
`599`. A malformed or out-of-range status line is a framing/protocol failure. Bodies are
discarded after bounded framing and are never retained for classification.

| Response | Direct routes GET before mutation | Direct routes GET after mutation | ID GET before mutation | ID GET after ambiguous PUT |
|---|---|---|---|---|
| `200` | Parse an exact JSON array plus exactly one valid `Etag`, then classify the whole array. | Parse an exact JSON array and require exactly one exact desired route plus a safe whole array. Exact whole-array conflicts retain their permanent conflict codes. A safe array without the route, malformed JSON, a non-array, or any other unresolved/invalid confirmation is retryable `caddy_mutation_ambiguous`, whether this GET follows PUT `200`, PUT `400`/`404`, or ID-based ambiguity resolution. | Exact desired route, otherwise permanent `caddy_route_id_conflict`. | Exact desired route is provisional only; a fresh post-mutation direct routes GET must prove one exact occurrence and a safe whole array. Different content is permanent `caddy_route_id_conflict`. |
| `400`, `404` | Discard the body and issue the bounded parent servers GET below in pre-mutation context. | Discard the body and issue the bounded parent servers GET below in post-mutation context. | `400` is permanent `caddy_admin_incompatible`; `404` means route absent. | `400` is permanent `caddy_admin_incompatible`; `404` is retryable `caddy_mutation_ambiguous`. |
| `401`, `403` | Permanent `caddy_admin_forbidden`. | Permanent `caddy_admin_forbidden`. | Permanent `caddy_admin_forbidden`. | Permanent `caddy_admin_forbidden`. |
| `300`-`399`, every other `400`-`499` | Permanent `caddy_admin_incompatible`; this includes `405`, `412`, `415`, and `422`. | Permanent `caddy_admin_incompatible`. | Permanent `caddy_admin_incompatible`. | Permanent `caddy_admin_incompatible`. |
| `100`-`199`, `201`-`299`, `500`-`599`, transport, timeout, framing | Retryable `caddy_unavailable`. | Retryable `caddy_mutation_ambiguous`, because the mutation outcome remains unresolved. | Retryable `caddy_unavailable`. | Retryable `caddy_mutation_ambiguous`, because the mutation outcome remains unresolved. |

Parent servers GET classification is also closed and depends on request context:

| Parent servers GET response | Before mutation | During post-mutation readback/resolution |
|---|---|---|
| `200` | Require an object. Absence of `fstack`, a non-object `fstack`, absence of `routes`, or a non-array `routes` is permanent `caddy_prerequisite_missing`. Otherwise classify the embedded routes by the same complete whole-array rules. An exact safe desired route may be adopted; conflicts keep their exact permanent codes. If the desired route is absent and the array is safe, return permanent `caddy_admin_incompatible`, because parent-only evidence supplies no direct routes `Etag` and cannot authorize PUT. | Apply the same object/prerequisite and whole-array rules. An exact safe desired route confirms success. Missing hierarchy is permanent `caddy_prerequisite_missing`, and conflicts retain their exact permanent codes. Any parent evidence that cannot resolve the mutation, including a present safe array without the desired route, is retryable `caddy_mutation_ambiguous`; parent-only evidence cannot establish rejection. |
| `400`, `404` | Permanent `caddy_prerequisite_missing`: the fixed hierarchy is absent. | Permanent `caddy_prerequisite_missing`. |
| `401`, `403` | Permanent `caddy_admin_forbidden`. | Permanent `caddy_admin_forbidden`. |
| `300`-`399`, every other `400`-`499` | Permanent `caddy_admin_incompatible`. | Permanent `caddy_admin_incompatible`. |
| `100`-`199`, non-`200` `2xx`, `500`-`599`, transport, timeout, framing | Retryable `caddy_unavailable`. | Retryable `caddy_mutation_ambiguous`. |

No error body is inspected to distinguish a missing path. Caddy v2.11.7 may return either
`400` or `404` when a configuration path cannot be traversed, so only the bounded parent
object decides whether the fixed `fstack.routes` prerequisite exists.

PUT classification is separately exhaustive:

| PUT response | Result |
|---|---|
| `200` | Definite mutation acceptance, but do not mark submitted until a fresh routes-array GET proves the exact route occurs once and the whole array is safe. An unavailable confirmation or a present safe array without the desired route is retryable `caddy_mutation_ambiguous`. |
| `412` | Retryable `caddy_config_changed`; do not retry the mutation inside the activity. The workflow retry restarts from routes-array inspection. |
| `400`, `404` | Discard the body and perform post-mutation direct-routes readback with parent fallback. A direct `200` exact route in a safe whole array is success; all exact conflicts use their whole-array codes. A direct `200` that does not establish the exact unique route, or parent evidence that cannot resolve the mutation, is retryable `caddy_mutation_ambiguous`. Parent proof of a missing prerequisite is permanent `caddy_prerequisite_missing`. |
| `422` | Permanent `caddy_route_rejected`. |
| `401`, `403` | Permanent `caddy_admin_forbidden`. |
| `405`, `415`, `300`-`399`, every other `400`-`499` | Permanent `caddy_admin_incompatible`. |
| `100`-`199`, `201`-`299`, `500`-`599`, transport, timeout, framing | Ambiguous. Resolve with ID GET and then mandatory fresh whole-array confirmation; any unresolved outcome is retryable `caddy_mutation_ambiguous`. |

Malformed or out-of-range status lines map to retryable `caddy_unavailable` for pre-mutation
direct routes GET and pre-mutation ID GET; to retryable `caddy_mutation_ambiguous` for
post-mutation direct routes GET and ID GET after an ambiguous PUT; and trigger
ambiguous-PUT resolution when received from PUT. Tests iterate every status `100` through
`599` for direct routes GET in both request contexts, both ID-GET contexts, and PUT, plus
malformed/out-of-range status lines. Parent GET tests iterate every status `100` through
`599` in pre-mutation and post-mutation request contexts and exercise every `200` object
shape. Tests also prove concurrent prerequisite disappearance and post-success confirmation.

## Production topology preflight

After the Crusher runtime is internally ready and before `reconcile`, activity
`fstack.preflight_local_caddy_topology.v1` receives only `ProvisioningSubmissionIds`. It
loads installation configuration and immutable deployment/runtime identity internally and
performs bounded Docker Engine calls to inspect:

1. the configured `FSTACK_CADDY_CONTAINER_NAME` container;
2. the deterministic `fstack-crusher-<deployment hex>` API container;
3. the exact `FSTACK_DOCKER_NETWORK` network and both endpoint records;
4. a bounded TCP connection to the listener in `FSTACK_CADDY_PROBE_ADDRESS`.

Preflight has one absolute 40-second monotonic deadline. Each of the three read-only Docker
inspection calls is additionally capped at 10 seconds, and the listener TCP connect is capped
at 5 seconds; no successful phase resets or extends the absolute deadline.

The Caddy container and Crusher API container must both be running. Each must have an
endpoint on the exact configured network. Caddy's exact network IP must equal the probe
address IP, and the Crusher endpoint must publish its deterministic container name or alias.
A valid Docker not-found result for the exact network, missing or mismatched network
membership, a missing or non-running Crusher container, probe-IP mismatch, absent
deterministic Crusher alias/endpoint, or any unsupported topology is permanent
`caddy_topology_invalid` with scope `ingress`. Docker/provider inspection that is unavailable,
times out, or returns malformed data is retryable `caddy_unavailable`. A missing or
non-running Caddy container or failed/timed-out listener TCP connect is also retryable
`caddy_unavailable`. No failure calls the Caddy admin API, and success returns no provider or
topology details.

## Reconciliation algorithm

The runtime is applied and proven internally ready, and the production topology preflight
succeeds, before ingress reconciliation starts.
Before either adoption or creation, Fstack inspects and safely classifies the dedicated
`fstack` server's **entire** routes array. A safely classifiable unrelated route is exactly
the frozen Fstack-owned route shape in this document for another deterministic deployment
ID, canonical exact host, and matching deterministic Crusher upstream. No unmanaged,
partially matching, extended, catch-all or otherwise unknown route shape is assumed safe.

Ingress reconciliation then follows this exact decision order:

1. `GET /id/<route-id>` before mutation.
   - `200` must be the exact desired route; different content is permanent
     `caddy_route_id_conflict`. Even an exact result is not adoption until step 2 confirms
     that the route occurs exactly once in the dedicated routes array and the whole array is
     safe.
   - `404` means absent at this instant and continues to step 2.
   - Every other result uses the closed pre-mutation ID-GET classification above.
2. Pre-mutation `GET /config/apps/http/servers/fstack/routes`.
   - `200` requires an exact array and exactly one valid `Etag`; missing/non-array routes or a
     missing/invalid `Etag` is permanent `caddy_prerequisite_missing`.
   - `400` or `404` discards the body and performs
     `GET /config/apps/http/servers`. Parent `400`/`404`, or parent `200` without an object
     `fstack.routes` array, is permanent `caddy_prerequisite_missing`. A parent `200` with the
     embedded array is classified by all rules below. It may prove exact adoption or a
     conflict, but when the desired route is absent and the array is safe it is permanent
     `caddy_admin_incompatible`: mutation requires an `Etag` from a successful direct routes
     GET and parent-only evidence can never authorize PUT.
   - Parent status and request-context failures use the closed parent mapping above; no error
     body is inspected.
   - Scan every element and count every occurrence of the expected `@id`.
   - More than one occurrence of the expected ID, or one occurrence whose object is not
     exactly the desired route: permanent `caddy_route_id_conflict`.
   - Any other route that claims the exact desired host or has a wildcard host matcher that
     matches it: permanent `caddy_host_conflict`.
   - After route-ID and host-conflict checks, require exactly one structurally exact and
     type-exact installation-owned `fstack-unclaimed-host` fallback in the final array
     position. A missing, modified, duplicated, or non-final fallback is permanent
     `caddy_routes_ambiguous`.
   - Excluding that exact final fallback, any catch-all/no-host route, or any route not
     exactly classifiable as the frozen Fstack-owned shape for another deterministic
     deployment, is permanent `caddy_routes_ambiguous` because ordering or matcher semantics
     could shadow the desired route.
   - Exactly one expected ID that equals the exact desired route, with no competing host,
     wildcard, catch-all or ambiguous route, and with the exact fallback last: adopt without
     mutation. Safely classified unrelated Fstack routes may coexist before the fallback.
   - If step 1 returned exact `200` but this array contains no expected ID, permanent
     `caddy_route_id_conflict`: the global ID is outside the owned routes array.
   - If step 1 returned `404`, no expected ID, only safely classified unrelated Fstack routes,
     and the exact fallback is last: continue. A route concurrently appearing in this array
     is classified/adopted by the preceding rules rather than inserted again.
3. `PUT /config/apps/http/servers/fstack/routes/0` with the exact route and the observed
   `If-Match` Etag.
   - `200`: definite mutation acceptance; perform a post-mutation direct routes GET and
     continue only when its `200` body proves the expected ID is unique/exact and the whole
     array is safe. Exact conflicts retain their permanent codes. A `400`/`404` uses the
     post-mutation parent fallback; every retryable/unavailable category, malformed or invalid
     `200` confirmation, a safe array without the desired route, parent evidence that cannot
     resolve the mutation, or any otherwise unresolved confirmation is retryable
     `caddy_mutation_ambiguous`. Parent proof that the hierarchy is missing remains permanent
     `caddy_prerequisite_missing`.
   - `412 Precondition Failed`: retryable `caddy_config_changed`; Temporal retries from
     step 1, including both read-only inspections.
   - `400` or `404`: discard the body, then perform a post-mutation direct routes GET with the
     same bounded parent fallback. A direct `200` exact desired route in a safe whole array is
     success. Exact conflicts retain their permanent whole-array codes. Parent proof that the
     hierarchy is missing is permanent `caddy_prerequisite_missing`. Only a direct `200` that
     does not establish the exact unique route, parent evidence that cannot resolve the
     mutation, or other unresolved non-conflict evidence is retryable
     `caddy_mutation_ambiguous`.
   - `422`: permanent `caddy_route_rejected`.
   - `401` or `403`: permanent `caddy_admin_forbidden`.
   - `405`, `415`, any unsupported `3xx`, or any other bounded `4xx`: permanent
     `caddy_admin_incompatible`.
   - Every `100`-`199`, `201`-`299`, `5xx`, timeout, disconnect, malformed/framing failure or
     other transport result is ambiguous; do not insert again until resolving it by
     `GET /id/<route-id>`.
4. Ambiguous PUT resolution by `GET /id/<route-id>`:
   - Exact route now exists: perform the post-mutation direct routes GET and succeed only if a
     valid `200` proves the expected ID is unique/exact and the whole array remains safe.
   - Same ID has different content, or duplicate expected IDs are observed during that
     confirmation: permanent `caddy_route_id_conflict`.
   - ID is absent, is in a retryable/unavailable category, or the required fresh direct routes
     confirmation is unavailable, invalid or otherwise unresolved: retryable
     `caddy_mutation_ambiguous`. A direct `400`/`404` uses post-mutation parent fallback;
     missing prerequisite remains permanent and parent evidence that cannot resolve the
     mutation remains ambiguous.

Concurrent disappearance of the server or routes prerequisite is classified through the
same deterministic direct-read/parent-read fallback for either `400` or `404`, never by
retaining or interpreting a response body.

A matching pre-existing route is adoption, not an error. Fstack never updates, deletes,
reorders or replaces a route in this slice. Permanent conflicts remain untouched.

## Routed listener probe

After the route is adopted or created, Fstack opens a bounded plain-HTTP connection to the
installation-configured local probe address. The address is installation configuration,
not request data; it is a canonical IP literal plus port and may not contain a hostname,
URL components, credentials or an unspecified address.

The request is exactly:

```text
GET /health HTTP/1.1<CRLF>
Host: <canonical public_domain><CRLF>
Accept: application/json<CRLF>
Connection: close<CRLF>
<CRLF>
```

where `<CRLF>` is the two-byte sequence carriage return plus line feed.

The probe has a 5-second connect cap inside one absolute 10-second monotonic deadline, a
16 KiB header cap and a 1,024-byte body cap. The one deadline begins before connect and
spans connect, the complete request send, headers, body chunks and trailers, EOF, and
trailing-byte validation without reset. Success requires all of:

- HTTP status exactly `200`;
- a single, valid HTTP response frame;
- body length no greater than 1,024 bytes;
- UTF-8 JSON with no duplicate object keys;
- exact value `{"status":"ok","service":"crusher"}` and no extra keys.

Connection failures, non-200 responses, malformed framing and any other non-exact response
are retryable `ingress_readiness_unavailable`. Response bodies and exception text are
discarded.

## PostgreSQL model

Successor migration `20261013_0004` adds one `ingress_submissions` row per provisioning
operation. Historical released revisions remain byte-identical.

An ingress submission stores only immutable desired/provider identity and terminal evidence:

- UUID identity and exact organization/operation/deployment agreement;
- provider exactly `local_caddy` and generation exactly `1`;
- deterministic submission key;
- route fingerprint and deterministic route ID;
- state and sanitized failure code;
- prepared, submitted, confirmed and failed timestamps.

It stores no socket path, listener address, route body, Caddy response/error body, attempts,
leases, claims, heartbeats, timers, next-run time or retry counters.

States are:

```text
prepared -> submitted -> confirmed
prepared -> failed
submitted -> failed
```

`submitted` means that an exact matching Caddy route has been adopted or a single atomic
mutation has been resolved as successful. `confirmed` means the routed listener probe
succeeded and final PostgreSQL success committed. Terminal rows are immutable. Provider
identity and submitted timestamp are immutable once submitted.

For v2, preparation is also the operation start boundary. One PostgreSQL transaction locks
the operation, prepares or exactly replays both the runtime and ingress submissions,
changes the operation from `pending` to `running`, and inserts or exactly replays one
`provisioning.started` audit whose payload is exactly:

```json
{
  "runtime_submission_id": "<canonical UUID>",
  "ingress_submission_id": "<canonical UUID>"
}
```

On replay, all three identities and the existing audit must agree exactly; no second audit
is inserted. Any statement failure rolls back submission preparation, the operation
transition and the audit together. The released v1 prepare and `mark_running` boundaries,
including their released audit payload, remain unchanged.

The existing runtime submission remains `submitted` after internal runtime readiness while
ingress is reconciled. Final success is one PostgreSQL transaction that:

- changes the runtime submission from `submitted` to `confirmed`;
- changes the ingress submission from `submitted` to `confirmed`;
- changes the operation from `running` to `succeeded`;
- appends exactly one `provisioning.succeeded` audit event referring to both submission UUIDs.

A permanent runtime failure atomically fails the runtime submission, the still-prepared
ingress submission, the operation and the terminal audit. A permanent ingress failure
atomically fails the ingress submission and operation while preserving an already-submitted
runtime submission and its immutable provider identity. The v2 failure finalizer receives
only canonical `operation_id`, scope and sanitized `error_code`; it locks the operation and
resolves both submission rows by operation ID inside the transaction instead of trusting
Temporal-supplied submission IDs.

A permanent prepare conflict is nonretryable only when PostgreSQL can resolve an
already-created exact v2 runtime/ingress bundle for that operation. The finalizer locks that
bundle by operation ID, fails both still-failable submissions, fails the operation and
inserts or exactly replays the v2 failure audit. A conflict in only one half of the expected
bundle, mismatched operation/organization/deployment identity, or any database failure
before an exact bundle is durably prepared is an infrastructure/state-resolution failure:
the activity remains retryable and no terminal state or audit is written. Retryable failures
never write a failure code or terminal audit and remain Temporal activity failures.

Migration `0004` replaces the audit payload constraint and history guard with a closed union:

- exact released v1 shapes only: requested `{}`, started
  `{runtime_submission_id, provider: "local_docker"}`, succeeded
  `{runtime_submission_id}`, and failed `{runtime_submission_id, error_code}`;
- exact v2 shapes only: requested `{}`, started/succeeded
  `{runtime_submission_id, ingress_submission_id}`, and failed
  `{runtime_submission_id, ingress_submission_id, error_code}`.

The guard validates canonical UUIDs and exact organization/operation agreement against the
referenced rows. No extra keys are allowed. New v2 audits contain no provider IDs, container
IDs, route IDs, paths, hosts, fingerprints, route configuration or provider text. Released
v1 audit rows remain valid but may not be broadened.

Before upgrade DDL, `0004` takes fail-fast `ACCESS EXCLUSIVE ... NOWAIT` locks, where
PostgreSQL permits, on only the pre-existing dependency and trigger-owning tables:
`organizations`, `deployments`, `provisioning_operations`, `runtime_submissions`,
`workflow_dispatches`, and `audit_events`. The newly created `ingress_submissions` table is
transactionally protected by its DDL and is not in the pre-DDL upgrade lock set. Downgrade
locks the same set plus `ingress_submissions`. Functions are not described as lockable
tables; locking the trigger-owning tables prevents concurrent trigger/function execution
while trigger functions and constraints are replaced. Failure to acquire required locks is
fail-closed rather than a partially concurrent migration.

## Temporal authority and compatibility

Temporal remains the sole retry and workflow-progress authority. PostgreSQL gains no
scheduler or attempt authority.

Migration `0004` admits both workflow types but continues assigning new operations to the
released v1 workflow while the v2 workflow and version-aware dispatcher are not yet present.
This keeps independently merged schema/activity work deployable without sending a v2 audit
shape through the v1 workflow. Migration `20261015_0005` is the Task 14 cutover: under
`ACCESS EXCLUSIVE MODE NOWAIT` locks on `provisioning_operations` and
`workflow_dispatches`, it replaces only the enqueue function so operations inserted after
that migration use workflow type:

```text
fstack.provision_crusher.v2
```

Existing dispatch rows are not updated. Downgrading `0005` takes the same locks, restores v1
only for future inserts, and leaves already-persisted v2 rows unchanged. Both directions retain
the deterministic Workflow ID:

```text
provisioning/<operation UUID>
```

The cutover worker registers both `fstack.provision_crusher.v1` and
`fstack.provision_crusher.v2`. Existing v1 dispatch rows and histories continue to start,
replay and complete with their released workflow/activity names and payloads. The cutover
migration does not rewrite existing dispatch history.

The Task 14 workflow-type cutover is a maintenance-mode operation. Stop or quiesce the API,
every dispatcher, and every worker, then verify process exit and that no old dispatcher/poller
remains. Apply the cutover migration, start the v2-capable worker and dispatcher, verify both
workflow versions are registered and polling, and only then enable API traffic. This gate
prevents an old dispatcher from consuming a newly inserted v2 row.

The v2 workflow payloads contain only canonical UUID strings, bounded state names and
sanitized error codes. They contain no secret references or values, socket/listener paths,
container IDs or names, route IDs or JSON, provider identities, fingerprints, host paths or
provider exception text. `ProvisioningFailureV2` contains exactly `operation_id`, scope and
`error_code`; submission IDs are resolved from PostgreSQL by the failure finalizer.

This prohibition applies to v2 and all new Temporal boundaries. It does not rewrite or
sanitize released v1 histories: the exact released v1 activity/result payloads remain
registered and replayable, including `provider`, `generation`, `submission_key`,
`spec_fingerprint`, `api_container_name` and `migration_container_name` where those fields
already exist. That is a byte-compatible replay exception only. Those values must not enter
API responses, logs or telemetry, and v1 audits remain limited to the exact released shapes
listed above. No new v1 field, activity shape or history producer is permitted.

The v2 order is:

1. atomically prepare/replay the exact runtime and ingress submission identities, transition
   the operation `pending -> running`, and insert/replay the exact v2
   `provisioning.started` audit;
2. apply and prove the runtime internally ready;
3. preflight the exact Dockerized-Caddy production topology and listener without touching the
   Caddy admin API;
4. reconcile/adopt the exact Caddy route;
5. probe the installation-managed Caddy listener with the exact Host header;
6. atomically finalize runtime submission, ingress submission, operation and audit.

Permanent domain/provider failures invoke one idempotent terminal finalizer using only
operation ID, scope and sanitized code. Retryable failures, including infrastructure
failures before durable bundle preparation, remain activity failures under Temporal retry
policy and preserve any submitted runtime or ingress provider identity.

## API contract

`POST /v1/provisioning/operations` is unchanged. `public_domain` already carries the exact
immutable routing intent, so no request field is added and the canonical v1 request
fingerprint remains unchanged.

The existing authenticated operation status endpoint is sufficient for this slice: its
`succeeded` state now means runtime plus routed listener readiness. No deployment read
endpoint is added. If a later product slice needs independent publication state, it must add
a separately reviewed sanitized endpoint rather than changing this POST response.

All GET authentication continues to happen before lookup and every GET response, including
401, 404 and 422, uses `Cache-Control: no-store`.

## Sanitization boundary

For v2 and every new boundary, the following never appear in API responses, audits,
Temporal errors/history, logs or telemetry:

- raw Caddy request or response bodies;
- Caddy error text or provider exception text;
- admin socket roots or socket paths;
- local listener/probe addresses;
- Docker container IDs or deterministic container names;
- Docker network names, endpoint addresses, aliases or inspection payloads;
- Caddy route IDs or route JSON;
- desired-state or route fingerprints;
- secret references or values;
- host filesystem paths.

Only bounded stable error codes cross new durable/public boundaries. The sole compatibility
exception is exact released v1 Temporal payload/history and exact released v1 audit shapes,
as defined above; it is not an exemption for API responses, logs, telemetry or newly emitted
payload fields.

## Explicit exclusions

This slice does not include:

- DNS provider APIs, DNS mutation or DNS ownership verification;
- ACME, TLS, HTTPS verification or certificate lifecycle;
- global Internet reachability assertions;
- Caddy process installation, startup, supervision or listener creation;
- creation or replacement of the `fstack` Caddy server or routes array;
- generic ingress/provider adapter frameworks;
- route update, delete, reorder, domain change, rollback or compensation;
- container supervision or replacement;
- object-storage resolution;
- portal/UI, billing or Remesher support.

## Real acceptance canary

Release acceptance uses an isolated environment containing PostgreSQL, Temporal, Docker and
Caddy. The reviewed canary harness is implemented, tested and committed first. Live
execution then requires a clean tree and runs that exact commit or immutable source archive;
evidence is written outside tracked source so execution cannot dirty or change the reviewed
artifact. Before execution, the harness resolves and records exact immutable image digests
for Caddy, PostgreSQL, Temporal and Crusher as explicit harness inputs; this contract does
not guess or hard-code those digests. It must:

- build from one exact source commit or immutable source archive;
- use only Fstack-prefixed networks, containers, volumes, database/schema names, Temporal
  namespace/task queue and Caddy resources;
- place Caddy, Crusher, API and worker containers on one isolated Docker network, assign
  Caddy one fixed isolated IP, configure the worker probe as
  `<fixed-caddy-ip>:<listener-port>` rather than worker loopback, and verify Caddy-to-Crusher
  name resolution/reachability plus worker-to-listener reachability before provisioning;
- configure the worker's fixed Caddy container name and prove the production topology
  preflight succeeds before reconcile; then run a permanent probe-IP/network/alias mismatch
  case that returns `caddy_topology_invalid`, never invokes reconcile, and leaves the complete
  Caddy config byte-identical;
- run Caddy v2.11.7 from the exact immutable digest supplied to the canary, record that digest
  as compatibility evidence, and make no compatibility claim for any tag/version/digest that
  has not passed this canary;
- preconfigure Caddy server `fstack`, its routes array, an HTTP listener and a permissioned
  admin Unix socket of exact mode `0220` on a separately confined shared volume, with the
  worker in the configured socket group;
- provision one Crusher deployment through the public POST command;
- prove the v2 workflow, exact route adoption/reconciliation and exact Host-header health
  response through the Caddy listener;
- prove exact adoption with an unrelated safe Fstack route, matching-route replay, and all
  permanent route conflicts
  (`caddy_route_id_conflict`, `caddy_host_conflict`, `caddy_routes_ambiguous`, and
  `caddy_prerequisite_missing`) return the exact sanitized terminal code and leave the
  isolated Caddy config byte-identical;
- use a separate valid isolated Caddy base config for each real-Caddy conflict: exact same ID
  with different content, exact ID plus same-host different ID where Caddy accepts that valid
  configuration, same host under a different ID, a wildcard host matching the desired host,
  a catch-all/no-host route, another deliberately unclassifiable but valid Caddy route, and a
  config whose `fstack` server exists without the required `routes` value. Duplicate expected
  IDs remain mandatory fake-admin/unit protocol coverage, but are not called a valid base
  config and are never loaded into real Caddy because Caddy rejects duplicate IDs while
  loading/indexing configuration;
- exercise exhaustive `100`-`599` status classification for direct routes GET in both
  pre-mutation and post-mutation contexts, parent servers GET in both contexts, both ID-GET
  contexts, and PUT, including
  every parent `200` object shape, malformed/out-of-range status lines, concurrent
  prerequisite disappearance, and post-mutation confirmation without retaining bodies;
- prove the maintenance-mode upgrade gate and that no v2 dispatch row is ever exposed to an
  old dispatcher or worker;
- capture migration, PostgreSQL state, Temporal replay, secret-scan, static-check and test
  evidence from that exact source;
- remove every canary-created process, container, network, volume, file, socket, database,
  namespace and temporary artifact on both success and failure.

Production PostgreSQL, Temporal, Docker, Caddy, networks, listeners, routes and data are not
touched by acceptance.
