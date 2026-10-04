# First Crusher provisioning slice

Status: frozen acceptance contract for the first implementation.

## Product outcome

An authenticated installation operator can request one new isolated branded business with Crusher as its first workload. Fstack records one durable desired state and one resumable provisioning operation. Retrying the same command is safe and returns the same operation.

This slice establishes the control-plane transaction and adapter boundary. It does not yet create cloud infrastructure, run a portal, bill a customer or route telemetry to a hosted backend.

## Authority boundary

`POST /v1/provisioning/operations` is an installation-operator endpoint, not tenant self-service. Authentication must yield a trusted principal with the installation-wide `business:provision` capability. Caller-supplied slugs never establish authority.

The first slice creates a **new** organization and its first business. It does not add businesses to existing organizations. A request whose organization slug already exists without the same idempotent operation returns `409 organization_exists` and never reveals that organization's resource IDs. Only the authenticated installation operator may replay or inspect operations created through this endpoint.

## Command

Required header:

- `Idempotency-Key`: 1-200 visible ASCII bytes (`0x21`-`0x7e`). It is stored and compared byte-for-byte without trimming or case folding.

Required body:

- `organization_slug`
- `organization_name`
- `business_slug`
- `business_name`
- `environment_slug` (initially exactly `production`)
- `public_domain`
- `crusher_image`
- `database_secret_ref`
- `object_storage_secret_ref`

Secret fields are references only; raw credentials are rejected.

## Canonical forms

### Slugs and names

All slugs are lowercase ASCII and match:

```text
[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?
```

Organization slugs are installation-global. Business slugs are unique within an organization. Environment slugs are unique within a business. The initial Crusher workload uses the reserved slug and kind `crusher` and is unique within its environment.

Names are UTF-8 text, trimmed once at both ends, 1-160 Unicode scalar values after trimming, and otherwise preserve case and internal whitespace. Empty or control-containing names are rejected.

### Public domain

`public_domain` is a lowercase ASCII hostname in canonical A-label form, without a scheme, port, path, query, fragment or trailing dot. It is 1-253 bytes, contains at least two labels, and every label matches the slug grammar above. The domain is reserved by an installation-global PostgreSQL unique constraint.

This slice reserves routing intent but does not verify DNS ownership or change DNS.

### Crusher image

`crusher_image` must be an immutable digest reference:

```text
<lowercase-registry-and-repository>@sha256:<64 lowercase hexadecimal characters>
```

Tags, uppercase repository text, missing registries, alternate digest algorithms and mutable aliases are rejected.

### Secret references

The only accepted form in this slice is:

```text
secret://<provider>/<identifier-segment>[/<identifier-segment>...]
```

The provider and every path segment use the slug grammar, the total reference is at most 500 ASCII bytes, and userinfo, ports, percent encoding, query strings, fragments, empty segments and dot segments are rejected. This is a broker-neutral reference syntax; retrieval is deferred.

References are persisted only in binding rows through parameterized SQL. Raw submitted values and stored references are never included in API errors, audit payloads, application logs or telemetry attributes.

## Canonical request fingerprint

The version discriminator is `fstack.crusher-provisioning-request.v1`.

The canonical payload includes exactly the discriminator and all body fields after the normalization rules above. It excludes the authentication principal and `Idempotency-Key`. Fstack serializes a JSON object with lexicographically sorted keys, UTF-8 encoding, no insignificant whitespace and no ASCII escaping, then stores the lowercase hexadecimal SHA-256 digest.

The fingerprint is persisted on the provisioning operation. Replay decisions never reconstruct identity from mutable resource rows.

## PostgreSQL scopes and tenant agreement

Every domain row has a UUID primary key. Tenant-owned rows also carry `organization_id` directly. Parent tables expose composite uniqueness on `(organization_id, id)`, and children use composite foreign keys so PostgreSQL proves tenant agreement:

- business → organization;
- environment → business;
- workload → environment;
- deployment → workload;
- database/storage binding → deployment;
- provisioning operation → organization, business, environment, workload and deployment;
- audit event → provisioning operation.

A plain foreign-key chain without these agreement constraints is not accepted.

Database uniqueness implements the declared scopes:

- organization: `(slug)`;
- business: `(organization_id, slug)`;
- environment: `(organization_id, business_id, slug)`;
- workload: `(organization_id, environment_id, slug)` and initially `(organization_id, environment_id, kind)`;
- public domain: `(public_domain)` installation-global;
- idempotency claim: `(organization_slug, idempotency_key)` with C/POSIX bytewise comparison;
- requested audit event: `(operation_id, event_type)`.

## Atomic persistence and idempotency algorithm

The application generates all public UUIDs before SQL and opens one PostgreSQL transaction.

1. Validate authentication and canonical input before persistence.
2. Insert the `provisioning_operations` row first as the idempotency claim, with generated resource IDs, organization slug, exact key, request fingerprint and state `pending`. Its resource foreign keys are `DEFERRABLE INITIALLY DEFERRED`. The claim statement is exactly conflict-safe in shape: `INSERT ... ON CONFLICT (organization_slug, idempotency_key) DO NOTHING RETURNING id`.
3. The unique constraint on `(organization_slug, idempotency_key)` arbitrates concurrent requests. No application-only pre-check is authoritative. PostgreSQL waits on an uncommitted conflicting tuple before deciding whether the `INSERT` returns the new ID or no row.
4. If `RETURNING` yields the generated operation ID, this transaction won. Insert the organization, business, production environment, Crusher workload, desired deployment, database binding, storage binding and one `provisioning.requested` audit event using the pre-generated IDs.
5. Commit once; deferred tenant-agreement foreign keys and all uniqueness constraints must pass.
6. If `RETURNING` yields no row, the competing claim committed. Without leaving or aborting the transaction, `SELECT` the operation by the exact organization slug and idempotency key after the arbitration statement:
   - equal fingerprint: return its stored IDs as a replay without inserts or a new audit event;
   - different fingerprint: return `409 idempotency_conflict` without exposing secret references.
7. If the competing transaction rolls back, PostgreSQL allows the conflict-safe `INSERT` to proceed; `RETURNING` yields this request's generated ID and it follows the winner path.

A command using a different idempotency key against an existing organization slug returns `409 organization_exists`. Existing businesses, environments or workloads are never silently adopted by this slice. Any failure before commit leaves no operation, organization, business, workload, binding or audit row.

## API response

A new command returns `202` with:

- stable public operation ID;
- state `pending`;
- organization, business, environment, workload and deployment IDs;
- `replayed: false`.

An identical replay returns `200` with the same IDs and `replayed: true`.

Errors use a stable envelope with `code` and `message`. Existence and idempotency errors do not include resource IDs, request fingerprints, secret references or submitted secret-shaped values.

## Operation lifecycle

This slice persists only the safe pre-effect transition:

```text
pending
```

A subsequent worker slice will claim `pending`, create immutable attempts, call the Crusher adapter and record external-effect results. The API request never invokes Docker, cloud APIs, billing systems, DNS providers or secret managers.

## Explicit exclusions

- actual Crusher container or cloud provisioning;
- adding a business to an existing organization;
- secret-value retrieval;
- billing, quotas or entitlements;
- portal/UI work;
- generic plugin discovery;
- Remesher support;
- Collector or Grafana deployment;
- worker claims, leases, retries, attempts or compensation.

Those are follow-up slices. This contract deliberately proves the durable command boundary before external side effects are permitted.
