# Explicit local runner sessions — review candidate

This extends [runner discovery](runner-discovery.md) with one short, owner-authorized native session. It is not managed compute, automatic placement, a billing system or hostile-tenant isolation. Account-free direct bpy.dev remains independent of this optional coordinator.

## Authorization and lifecycle

1. The agent creates its Tailcat client identity locally and shares **only its public key** with the human owner. It also generates a fresh session capability `fs_x_` plus `secrets.token_urlsafe(32)`; do not reuse it or log it.
2. Using the existing current-owner browser session, Origin and CSRF header, POST `/v1/auth/accounts/<account>/sessions` with `session_id`, `runner_id`, `workspace_id` (UUIDs), `client_public_key`, `session_token`, `ttl_seconds` (5–300), `memory_mib` (512–4096), and `cpu_millis` (100–2000). The owner must be the runner's enrolling owner. The runner's previously approved immutable image is copied into the session. This approval is specifically for arbitrary code execution inside that user-managed workload, not payment or asset upload permission.
3. `account:read` cannot create/execute/cancel sessions. The session capability permits only GET `/v1/sessions/<session>` and POST `/v1/sessions/<session>/stop`. It is bound to that session, account, current owner and absolute expiry. The owner sees the capability while approving and can use it to cancel. Revoked membership or runner enrollment denies further access; the runner stops on failed polling, with local TTL as the outage fallback.
4. The enrolled runner POSTs `/v1/runners/<runner>/assignment`. PostgreSQL atomically changes requested→leased, generation 0→1, returning `launch=true` **once**. Repeat polls return the same assignment with `launch=false`. A lost initial reply does not authorize relaunch: the next runner reconciles/removes the deterministic owned container and acknowledges stopped. A deliberate retry is a new owner-authorized session after cleanup, not a reset generation.
5. The local supervisor generates its own server private identity in a private temporary directory, starts Tailcat with exactly the approved agent public key, and hands the capability address to the poller over a private pipe. The poller POSTs `/ready`; the coordinator keeps the address in volatile process memory only and supplies it solely after session authorization. The runner republishes while active to recover coordinator memory loss. Use **one API process**; multi-worker/HA address storage is unsupported.
6. MCP travels directly through Tailcat (or its relay), never through the coordinator. Tailcat executes a locally configured product command in the immutable Docker image when the allowed agent connects. Workload networking is disabled; no host socket or bind-mounted host workspace is supplied. Workspace UUID identifies a disposable in-memory workspace, not access to user files. Memory, CPU, process-count and scratch-space limits are fixed by admission/local code.
7. Cancellation/expiry requests stopping. The supervisor independently enforces absolute TTL and parent-pipe EOF, kills its listener/process group, removes the exact owned container and verifies absence before a stopped receipt. Poller and supervisor retain the same exclusive local runner lock through cleanup. Empty lock files intentionally remain so concurrent processes cannot lock replacement inodes. Private identity directories are removed. If deletion or its acknowledgement fails, durable state remains unresolved; it must not be reported as settled usage or successful cleanup.

The current state machine has one active session per runner, one immutable execution generation, no automatic retry or reassignment, and no distributed retry scheduler. The coordinator stores lifecycle timestamps, **not canonical resource UsageFacts or invoices**. Lifecycle metering/export is still a follow-up. Session admission replay is immutable but returns the original receipt, not renewed TTL or a new token.

## Runner API (local trusted configuration)

Call `fstack.local_session_runner.run_assignment(origin, runner_id, runner_token, image=..., command=[absolute_executable, ...], tailcat=absolute_tailcat_binary)` from an owner-controlled runner process. Keep credentials in private files/in-memory, not argv or logs. `command` is trusted local product configuration; the coordinator cannot supply it. The call pulls and completes one assignment (or reports idle); this increment does not install a machine-wide daemon.

The supervisor module's stdin/stdout protocol is private IPC, not a human CLI. Do not invoke it to print addresses into a terminal/log. Pin a verified Tailcat binary; the acceptance copies v0.7.0 from the pinned runtime image rather than downloading arbitrary software. This local backend targets Linux with an already authorized Docker daemon; never change daemon permissions or groups to make it work.

Runner control HTTP uses a dedicated subprocess with a hard total exchange deadline, the existing bounded `http_framing` reader, no redirects/proxies, and verified TLS outside explicit loopback tests. Slow-header/body tests verify socket shutdown. Per-session database transactions have 2-second lock and 3-second statement limits. Container deletion has bounded cleanup attempts; these are not a guarantee against a stalled/unavailable Docker daemon or hostile code escaping container isolation.

## Actual native acceptance

Use an owned disposable PostgreSQL database ending `_test`; repository fixtures reset it. Pull the exact published runtime before the opt-in test:

```sh
docker pull ghcr.io/bpy-dev/bpy-runtime@sha256:325ed79d5b34613d981786ec392d2b440bc88a05e5bfc26f89085ba32c6671b2
FSTACK_NATIVE_SESSION=1 FSTACK_REQUIRE_POSTGRES=1 \
FSTACK_TEST_DATABASE_URL=postgresql://USER:replace-me@HOST/local_session_test \
uv run pytest tests/test_runner_session_native.py -q
```

The product-specific test bootstraps an empty `.blend` entirely inside the disposable workload and calls the actual enhanced MCP `execute_blender_code_for_cli` tool. It verifies version 5.3.0, wrong-client denial with the correct address, separate runner/agent private identities, no key/address in ordinary PostgreSQL rows or Docker container configuration, and stop/expiry deletion before test teardown. It does not upload a user asset, render, spend money, certify remote NAT/forced relay, exercise production HTTPS, or prove full hosted capability parity. Native tests are opt-in locally, not a new mandatory hosted image-pull gate; portable HTTP/PG tests remain under ordinary CI discovery.

## Commercial boundary

The newer accepted direction supersedes the prior discovery plan's open accounting note: reusable **private Fstack Cloud** credit accounting, initial Stripe payments, and a replaceable billing adapter, with Lago optional later. Preserve existing Lago integration/hooks. Each business keeps separate identities, accounts, wallets and data while reusing source. No credit conversion, pack denomination, charge, reservation engine or payment is implemented here. See the [next-slice plan](plans/2026-11-19-local-runner-session.md) for the bounded private reservation contract proposal.
