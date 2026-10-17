# Provisioned-host certificate authorization slice

## Goal

A shared public Caddy instance may obtain or renew an on-demand certificate for an exact
hostname only when current Fstack durable state says that hostname belongs to a successfully
provisioned deployment. Wildcard DNS resolution alone never authorizes certificate issuance.

## Ownership boundary

- Fstack owns the durable allow/deny decision for exact provisioned hostnames.
- Host Caddy owns ACME, certificate storage, renewal and the public TLS listener.
- `fstack-cloud` owns the installation-specific host-Caddy wiring on `billing.crusher.sh`.
- This slice does not mutate DNS and does not make Cloudflare an authority for tenant
  ownership.

## Authorization endpoint

The Fstack API exposes an unauthenticated installation-local endpoint:

```text
GET /internal/caddy/ask?domain=<hostname>
```

The endpoint is intended only for direct loopback access by host Caddy. The public
`api.fstack.com` site must intercept this path and return `404` without proxying it.

The decision is deliberately bodyless:

- `200` when `domain` is the exact canonical `deployments.public_domain` for a persisted v2
  workflow dispatch whose operation is `succeeded` and whose exact runtime and ingress
  submissions are both `confirmed`;
- `403` for every well-formed but unauthorized hostname, including legacy v1 successes,
  pending, running, failed, unknown and previously submitted-only deployments;
- `400` for missing, repeated, non-ASCII, noncanonical, wildcard, trailing-dot, single-label,
  oversized or otherwise malformed values;
- `503` when the durable decision cannot be read.

Every response carries `Cache-Control: no-store`. Bodies, database/provider errors, submitted
values and authorization candidates are never logged or returned. The endpoint performs no
mutation and does not require the installation operator token.

Canonical hostname validation uses the existing Fstack public-domain contract: lowercase
ASCII A-label form, 1-253 bytes, at least two labels, each label matching the released slug
label grammar, with no scheme, port, path, query, fragment, wildcard or trailing dot.

## Host-Caddy wiring

The host Caddy global options configure:

```caddyfile
on_demand_tls {
    ask http://127.0.0.1:8300/internal/caddy/ask
}
```

The existing wildcard site retains `tls { on_demand }`. The exact API site rejects the
internal path before its reverse proxy. The managed Fstack public-ingress block is installed
atomically after backup, validated with the exact host Caddy binary, reloaded, and read back.
Rollback restores the previous bytes and reloads them.

## Acceptance

1. A succeeded retained deployment such as `smoke.fstack.com` receives `200` from the direct
   loopback authorization endpoint.
2. Pending, running, failed, unknown and malformed names never receive a successful decision.
3. The same internal path through `https://api.fstack.com` returns `404`.
4. A provisioned hostname retains valid public HTTPS and workload health.
5. A fresh random wildcard hostname is denied by the authorization endpoint and cannot
   complete public TLS. The internal workload listener still returns its terminal `404` for
   an unclaimed Host header and no workload route is created. Without an already-issued or
   wildcard certificate, a public HTTPS client cannot reach that HTTP fallback because the
   certificate decision intentionally fails first.
6. Existing host Caddy sites, Fstack routes and sibling workloads remain healthy.

## Explicit exclusions

- DNS mutation or DNS ownership challenges;
- Cloudflare API automation or proxy-mode changes;
- certificate inventory as Fstack durable state;
- custom customer domains outside the existing canonical hostname contract;
- route update, domain change, deprovisioning or certificate revocation;
- billing, quotas, portal work or generalized ingress-provider adapters.
