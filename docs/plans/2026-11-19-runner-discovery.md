# Owner-enrolled runner discovery Implementation Plan

> **For Hermes:** Use subagent-driven-development for subsequent session dispatch; this candidate requires independent review before commit.

**Goal:** An owner enrolls a user-managed machine, its outbound runner reports bounded availability, and an existing account-authorized agent reads fresh/stale capacity and an honest local vendor-charge estimate.

**Architecture:** Extend installation-local IdentityAuthority, not operator authentication. Reuse existing owner sessions and account:read grants. PostgreSQL owns immutable enrollment, monotonic heartbeat generations, expiry and irreversible revocation; no cloud dependency.

**Tech Stack:** Existing FastAPI, PostgreSQL/Alembic, SQLAlchemy and Python standard-library runner client.

## Inspection and scope decision

Inspected public main 8509428, private main 16db247, runtime main 2965cde. Preserve the existing public usage_facts and shipped private Lago contracts; their presence does not decide the accounting backend for new prepaid compute. Current usage is deployment activation, not resource lifecycle seconds. Existing agent grants allow only account:read. The shipped runtime Tailcat supervisor accepts an externally generated server private key. New server-side generation and execution-scoped agent grants are necessary before session dispatch; a read-only grant must not acquire execution authority.

This bounded prerequisite is NOT the requested full coordinator-to-native-MCP session demo. It deliberately returns no assignment and has no assignment endpoint, session state, capability address, or server key. No payment, deployment, asset transfer, or cloud provisioning is enabled. The existing hybrid execution plan remains the broader roadmap.

## Acceptance / exact files

1. Add tests/test_runner_discovery_postgres.py: through packaged create_app, explicit owner enrollment, authenticated outbound report, existing agent discovery; deny operator and cross-account access. Run RED first.
2. Add migration 20261119_0008 and src/fstack/runner_discovery.py: immutable account/owner/image/machine class, irreversible revoke, bounded expiry, integer monotonic report sequence. Duplicate exact reports accepted; changed replay and stale sequence rejected. Database trigger rejects illegal direct updates.
3. Extract existing agent read authority into IdentityAuthority without broadening permissions; mount additive routes in api.py and privacy middleware.
4. Add src/fstack/runner_client.py: bounded outbound HTTPS report with explicit timeout; no inbound listener or Docker socket.
5. Exercise real PostgreSQL, all affected identity tests, lint/type checks, migration down/up. Record exact outcomes, not planned passing results.

## Estimate contract and honest limitations

A local user-managed machine has zero proposed vendor compute charge, not zero electricity/hardware cost. Return USD decimal strings and resource-second units, rate observation time, quote ID and expiry, freshness, unknown job duration/confidence, queue observation and explicit non-guaranteed capacity. No provider prices, settled cost, customer wallet, rate multiplier or commercial rating enter Core. No authority for spending is conferred by a quote. Managed customer pricing and the provisional configurable 2x proposal remain private follow-up.

## Clarified managed-commercial follow-up (not enabled)

Prepaid credit packs fund actual metered usage at an accepted immutable quote/rate, not a fixed job price. Credit denomination, pack amounts and USD conversion are unspecified: do not assume any. Keep wallets, balance/reserved/spendable projections, configured credit-to-currency conversion versions and provider USD costs private. **Accounting backend decision OPEN:** private Stripe plus a narrow credit ledger is a proposal, and Lago is optional/deferred, not mandated for this workflow. The user has not approved a billing architecture change; preserve existing integrations and hooks, and do not implement, replace or remove billing under this slice. Reserve the approved maximum before dispatch, meter exact integer resource durations, settle actual usage and release unused reservations. PostgreSQL-backed idempotent reserve/settle/refund and concurrent-overspend tests are required before enablement. Reserve cleanup headroom and stop on exhausted budgets; provider liability may continue until deletion is confirmed. No payment, credits issuance or charges are authorized.

Dynamic provider/machine offers are per independent business, not a cross-business marketplace. Each authentic observed offer needs capabilities, availability observation/expiry, a customer credit rate separate from internal provider USD cost, benchmark scope and uncertain startup/queue/execution/transfer estimates. First implement one working local/RunPod target before other providers. Freeze the accepted rate/conversion per allocation; switching providers requires a new quote and consent unless a prior explicit policy covers it. Preserve upload consent and hard backend caps. Product checkpoint compatibility and transfer costs prohibit pretending this is transparent live Blender migration. Do not present fixture entries as live offers.

## Next vertical, not delivered here

Explicit owner approval of session-execution delegation; immutable workload/workspace/resource/TTL admission; conditional assignment leases and retry generations; runner-local Tailcat server generation; protected transient address handoff; actual MCP version call; stop/expiry and deletion receipts; allocation-through-deletion neutral UsageFacts with any commercial export adapter deferred to the open private accounting decision. No capability address may enter plaintext database/logs, no private identity leaves its originating runner/agent, and no MCP calls proxy through coordinator. Real native-MCP evidence remains a delivery gate, not a claim made by this prerequisite.
