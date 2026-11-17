# bpy.dev Local-First and Hybrid Execution Implementation Plan

> **For Hermes:** Use the subagent-driven-development skill to implement this plan task-by-task after separate implementation authorization. This revision is documentation only: no deployment, cloud spending, billing enablement, credentials, or merge is authorized.

**Goal:** Let an existing coding agent inspect and edit a local Blender scene, preview it, checkpoint and fork it, and obtain a valid capped render plus an editable `.blend`; then add explicitly authorized cloud execution of that checkpoint.

**Architecture:** Reuse the bpy.dev enhanced Blender MCP runtime for the first complete local workflow. Public Fstack supplies provider-neutral installation, identity and durable operation contracts only where needed; private Fstack Cloud owns managed procurement and commercial operations. Prove local execution before adding one cloud target, and prove that target before distributing independent frames.

**Tech stack:** Existing Blender/bpy and enhanced MCP tooling; existing Fstack Python/FastAPI, PostgreSQL and durable provisioning/Temporal contracts where the audited integration requires them. Do not add a universal scheduler, queue, framework, or new core/cloud repository.

## Status and accepted direction

Planning only. No runtime behavior is implemented or certified by this document.

- First useful deliverable: the existing-agent local inspect/edit/preview/checkpoint/fork/capped-render loop, with no cloud credentials, vendor endpoints, or subscription.
- Second deliverable: one explicit cloud execution from an immutable checkpoint, with separate upload consent and a user-approved dollar spend cap. RunPod remains the proposed first GPU target, not an authorization to procure it.
- The RTX 5090 plus one cloud GPU independent-frame demonstration is a later extension, **not a first-release gate**. Verify actual hardware/backend availability rather than assuming a machine label proves compatibility.
- CLI/MCP first; use the existing client surface, not a new chat application or browser Blender editor. A minimal preview/files/status/cost view can follow demonstrated need.
- Keep customer-operated core, runners and self-hosted product adapters open source and usable without vendor login or billing. Monetize provided compute and managed operation; optional team/compliance features are separate decisions. Basic local security remains core.
- bpy.dev, Remesher and Crusher remain separate branded businesses with separate customer identity, credentials, billing, storage and data boundaries.
- Patch is an interface reference, not a requirement for full compatibility.

This bounded Fstack integration increment does not redefine the whole bpy.dev release. The existing whole-project plan still requires a Hermes plugin exposing the **full enhanced MCP capability surface**, including explicit live-session/UI prerequisites and local/hosted parity evidence. A successful headless loop is one useful increment, not proof of full plugin delivery or parity; do not silently hide unsupported tools.

## Verified repository ownership and existing authority

Authenticated `gh repo list` for both `michaelgold` and `bpy-dev`, existing local `PROJECT.md`/README documents, and exact remote content reads establish the following ownership on 2026-11-17:

| Repository | Verified role and revision |
| --- | --- |
| [`bpy-dev/bpy-dev.github.io`](https://github.com/bpy-dev/bpy-dev.github.io/blob/89a66803cbe2c6ee19665e130b74fe376af708ee/PROJECT.md) | Public website and authoritative whole-project `PROJECT.md`; head `89a66803cbe2c6ee19665e130b74fe376af708ee`. Not the execution runtime. |
| [`bpy-dev/blender-mcp`](https://github.com/bpy-dev/blender-mcp/tree/e8ac4088d4a3e0469d3eff9f9bb2dd717e899219) | Public enhanced MCP execution runtime; head `e8ac4088d4a3e0469d3eff9f9bb2dd717e899219`. Owns Blender inspection, execution, preview and saved-file behavior. |
| [`michaelgold/buildbpy`](https://github.com/michaelgold/buildbpy) | Standalone bpy build/packaging tooling, not the product service. The whole-project plan pins its inspected README at `8113295240b0d95781fa4c043b6cfecba9a6eba7`; this is a historical source reference, not a fresh runtime certification. |
| [`michaelgold/fstack`](https://github.com/michaelgold/fstack) | Public platform core; inspected main `0083f101a68204d78e74e0bf6da1a3b83b92dba6`. |
| `michaelgold/fstack-cloud` | Separate private managed-service repository; inspected main `16db24748fbb426a72b70859ba3d45cf15686dbb`. Its merged documentation PR #41 records full bpy.dev integration as planned, not delivered. |

**Discovery result:** the product is split across the verified website/project-plan and enhanced MCP repositories; there is no need to invent a `bpy.dev` service repository. `buildbpy` was a researched packaging candidate, not the execution owner. Plugin packaging/distribution and the exact implementation file map remain open design work; this is not an unresolved repository-ownership blocker. Freeze that file map against the actual runtime tree before implementation, rather than inventing module names here.

Read the current public `README.md`, `docs/STATUS.md`, `src/fstack/identity.py`, `provisioning.py`, `runtime_state.py`, `runtime_submission.py`, `runtime_application.py`, `docker_cli.py`, and existing Temporal dispatch contracts. Reuse existing seams; inspect `docs/architecture/first-crusher-slice.md`, `local-crusher-execution-slice.md`, `temporal-provisioning.md`, and `first-billing-plane-slice.md` before proposing extensions.

Read private `docs/STATUS.md` and `docs/architecture/first-lago-billing-slice.md` at the current private revision. Its billing recovery/delivery gates remain open where recorded; a merged integration plan does not establish collection, eventual usage delivery, deployment or billing enablement. Preserve historical evidence and the stock OSS Lago/native Stripe direction.

The public README's managed-fleet boundary and second-workload language need a scoped reconciliation before implementing any public coordination extension. Customer-operated explicit coordination may belong in core; vendor capacity management and hosted commercial operations remain private. This plan does not move the entire fleet layer into public core or make a runner registry a prerequisite for the first local scene edit.

## Product/platform boundary

### Public Fstack

Installation/release identity, desired/observed deployment state, scoped human/agent authorization, durable lifecycle operations, local audit, and provider-neutral usage facts remain core. Add trusted runner registration, explicit placement or reservation only when the concrete workflow needs a missing contract. Customer-operated operation must not phone home or import private billing/provider identities.

### Product-owned semantics

- **bpy.dev:** Blender version/backend selection, sessions and saved files, inspection/edit/preview, checkpoint/fork, artifact packaging, render validation, and later independent-frame decomposition/assembly.
- **Remesher:** model/pipeline selection, stage dependencies, VRAM, mesh/rig/export validation.
- **Crusher:** migration/readiness/drain hooks, mail transport, sender policy, suppression and delivery/recovery semantics. Email deliveries are not generic GPU jobs.

Keep Blender, mesh and recipient logic out of generic placement. Remesher/Crusher implementation is exclusively in the separate [small adapter follow-up plan](2026-11-17-product-adapter-follow-up.md); neither adapter is a dependency of local or checkpoint-cloud delivery.

### Private Fstack Cloud

Vendor capacity procurement, managed credentials, hosted quotas/rating/billing and commercial operations remain private. Preserve **unmodified stock OSS Lago and native Stripe collection through supported interfaces**; extend around Lago, not inside its core, and do not introduce a second billing engine. Separate branded realms and ledgers. Public security and usage contracts remain usable without these services.

## First acceptance: one local existing-agent loop

Acceptance must run through the actual agent-facing MCP/CLI interface, not just internal helpers:

1. Start from a small non-sensitive scene on a trusted local runner; inspect objects, current scene and runtime capabilities.
2. Make a visible edit and obtain a preview; inspect again to verify the requested edit, not merely command success.
3. Save an immutable checkpoint with input/dependency hashes and runtime provenance. Reopen the saved `.blend` in a fresh process to prove the edit persisted.
4. Fork the checkpoint into a distinct editable output path; edit the fork and prove the original checkpoint bytes remain unchanged.
5. Submit a bounded local render: explicit frame count, resolution, samples, wall-clock deadline and cancellation. Reject invalid/unsupported limits before work; enforce limits at execution, not only in the client.
6. Decode the output, check dimensions and expected scene content, and return a downloadable valid render and editable `.blend` with matching provenance. A nonempty file or success status alone is insufficient.
7. Repeat with cloud credentials absent and vendor endpoints blocked, capturing the negative network control. Local-only must never fall back to paid work.
8. Exercise script failure, timeout/cancellation, changed-input retry and unauthorized artifact access. Preserve checkpoint/source files, return a stable operation handle, and verify child cleanup rather than relying on test teardown.

A CPU backend is sufficient for this first acceptance. Local GPU execution is an optional next capability using the same saved scene/client contract; unsupported GPU hardware must yield a useful error, never implicit cloud fallback. No cloud registration, pricing integration or distributed scheduling is required to prove this loop.

## Second acceptance: explicitly authorized checkpoint cloud execution

Only after local acceptance and separate execution authorization:

1. Select one immutable local checkpoint and its complete dependency manifest. Show exactly what would leave the machine; require explicit upload consent scoped to that checkpoint/assets and destination.
2. Authenticate the account and delegated agent, verify current membership/scopes and entitlement, and obtain an explicit dollar spend cap and termination deadline. Upload consent, admission authority and spending approval are separate requirements.
3. Reserve a conservative allowance before provisioning one cloud target. Pin compatible Blender/backend/dependencies and immutable image/artifact identity. An altered checkpoint requires renewed matching authorization.
4. Execute one bounded render, fetch and validate outputs, and preserve the editable checkpoint plus runtime/output provenance.
5. Exercise cancellation and ambiguous launch/response recovery. Read back actual resource termination; account for storage, transfer, stopping latency and outstanding provider charges separately.
6. Reconcile accepted output separately from attempt usage and final cost; verify cloud denial leaves the local loop usable.

Provider fixtures establish contract behavior, not live acceptance. A real-provider smoke needs its own explicit test budget/upload permission; illustrative prices below grant neither. Do not claim a spend cap prevents all delayed provider charges during an outage: stop new admission with conservative headroom, bound teardown attempts and expose unresolved liability/resource handles for reconciliation.

## Delivery sequence: finish each useful increment

### Slice 0 — narrow existing-runtime audit

**Read:** the pinned product `PROJECT.md`, runtime `readme.md`, `readme_tools.rst`, `readme_bpy_backend.rst`, `SECURITY.md`, and the public/private contracts above.

1. Inventory the actual registered capability surface and live-session versus saved-file prerequisites. Existing `_for_cli` tools use fresh subprocesses and require explicit saves to distinct paths; standalone bpy is not an interactive UI or OS sandbox.
2. Map the first local acceptance steps to existing tools and only the missing behavior. Record supported version/platform pins; historical macOS evidence does not certify this Linux/RTX runtime.
3. Freeze exact existing implementation/test paths and commands in the owning runtime repository before writing code. Record plugin packaging ownership without creating a new repository.
4. Review the local request/result/error/limit contract and acceptance fixture. Propose Fstack changes only if an actual missing boundary prevents this loop.

**Exit:** an actionable file map and first behavioral test, not a new platform/framework inventory. Repository discovery is completed above; runtime compatibility and implementation mapping are still open. Stop if these cannot be established; do not put Blender execution into Fstack to evade ownership.

### Slice 1 — deliver the local loop

**Owner/files:** `bpy-dev/blender-mcp` runtime and its audited tests; update its existing runtime/tool/security guides where behavior changes. Exact code paths are a Slice 0 gate, not speculative Fstack additions.

For each next observable behavior: write one failing agent-facing acceptance test, run it and confirm the expected missing behavior, implement the smallest change, then run the focused and affected tests and refactor. Start with inspect/edit/preview and persistence, then checkpoint/fork and capped render in the same complete local workflow. Include necessary ownership, valid-state, timeout and source-preservation failures in this slice, not as later polish.

**Exit:** every first-acceptance step above passes with real Blender, decodable output and reopened editable `.blend`, plus cloud-off controls. Deliver this useful increment before provider integration. Do not label it full enhanced MCP/plugin parity.

### Slice 2 — deliver one authorized cloud checkpoint

**Owner/files:** product checkpoint packaging/client behavior in the audited runtime; provider-neutral admission/operation seam in public core only if missing; vendor credentials, spend reservation and managed billing composition in private cloud. Read and extend existing provisioning/identity contracts rather than creating parallel authorities.

1. Drive consent/authorization/limit failures test-first, including cross-account access and changed-input retry rejection, before any external side effect.
2. Add only the provider launch/lookup/cancel/teardown contract needed for one render; test lost responses and failed termination with controlled fixtures and real durable storage.
3. Exercise the complete second acceptance under a separately approved budget, then verify teardown/cost readback. Keep customer-supplied provider credentials usable without our managed service where that mode is offered.
4. Publish the exact evidence and unresolved charges or cleanup failures; fixtures and source review do not certify live operation.

**Exit:** authorized checkpoint-to-valid-output execution with bounded admission and honest cleanup/cost evidence. No hybrid frame scheduler or other product adapter is required.

### Later extension — independent-frame hybrid rendering

After Slices 1 and 2 are delivered, optionally verify local GPU support and split a saved scene into deterministic independent-frame tasks. Explicit placement may assign frames to a customer RTX 5090 and one cloud GPU. Test disconnected workers, interrupted attempts, one accepted output per frame, cancellation and exhausted budgets. bpy.dev owns decomposition/assembly; only a demonstrated shared need can justify a public dispatch abstraction.

This is **not a first-release gate**. No shared GPU memory, transparent multi-machine arbitrary single-frame rendering, or distributed sequential simulations are promised. Pin compatible runtimes/settings/dependencies; use representative visual tolerances rather than promise byte-identical images across GPU architectures.

### Client presentation, not another engine

Use the existing MCP/client interface in Slice 1, not after infrastructure work. If HTTP/SDK/CLI wrappers are needed later, keep one canonical execution contract and thin mappings: inspect, edit, preview, checkpoint, fork, render, status, download, cancel. Add a minimal web view only when useful. A narrow Patch adapter requires a concrete client need and explicit compatibility tests, not resemblance alone.

## Valid-state, idempotency and security invariants

- A session/checkpoint belongs to an authorized owner; immutable manifests bind inputs, dependencies, sizes, hashes and runtime. Interactive mutation has one writer; forks use distinct outputs.
- Model legal transitions with narrow typed operations and transactional conditional updates, not arbitrary enum assignment. Reject malformed input before credentials, uploads or provider actions.
- Reuse existing durable operation identity. Same request and identity returns the existing operation; changed inputs with that identity fail. A timed-out caller retrieves/reconciles rather than launches another billable job.
- Keep logical success separate from execution attempts and usage. In later fan-out, deterministic task identity plus attempt generation/lease prevents duplicate accepted frames without erasing real retry costs.
- A fresh subprocess or ordinary container sharing rootful Docker is not hostile-tenant isolation. Use trusted private local runners and separately isolated managed instances; do not place unrelated tenants in unrestricted runtimes. Never expose a Docker socket or cloud control-plane credentials to generated code.
- Scope artifact access and agent grants; test expired/revoked credentials and cross-installation/tenant denials. A local agent does not gain human payment authority. Prefer authenticated outbound runner connections; do not expose workstation management ports publicly.
- Local-only policy is enforced, not advisory. No cloud subscription/private repository dependency or silent fallback. Checkpoint uploads require specific consent; checkpoint contents and arbitrary scripts remain sensitive.

## Verification and bounded delivery discipline

This PR changes documentation only. Use focused document/link/consistency checks, `git diff --check`, the existing tracked-file secret scanner, and exact diff review. Do not manufacture RED/GREEN runtime evidence for a prose change. Retain failed CI evidence and let this normal documentation commit trigger a new PR run; no blind rerun, workflow weakening, protection changes or merge.

The predecessor run [44182080432](https://github.com/michaelgold/fstack/actions/runs/44182080432), bound to `7831f47465320fc93c38a5edeb278385f72e11bd`, failed in PostgreSQL service initialization: Docker timed out starting the userland proxy for port 5432. Checkout/tests never ran. Preserve that failure as pre-test infrastructure evidence, not a product regression or passing test result. Fresh protection inspection requires strict `verify`, enforces admins and requires zero approving reviews; zero required reviews is not permission to bypass CI or this task's no-merge boundary.

For future code: exercise the actual caller interface with behavioral TDD; use real PostgreSQL for durable ownership/concurrency where required, never SQLite substitution. Confirm each expected failure before the minimal implementation, then affected integration tests and exact-source review before protected PR delivery. After two failed fixes sharing a premise, stop and test the premise/state ownership rather than adding guards or weakening gates.

Current public CI uses `uv sync --frozen`, `uv lock --check`, `git diff --check`, `uv run python tools/scan_tracked_secrets.py`, Ruff, mypy, migration roundtrip, `uv run pytest -q` with required PostgreSQL, and an installed-image build. Read `.github/workflows/ci.yml` at execution time; include new acceptance tests in mandatory discovery. A local focused document check is not a full-suite result. Keep skipped, blocked, fixture, real-runtime, live-provider, published and deployed evidence separate.

## Illustrative commercial proposal — unapproved

Customer-operated core remains useful and open. Provided cloud compute and optional managed operation can be prepaid/bounded; bring-your-own-agent avoids forcing a second reasoning subscription. Local execution has no proposed vendor compute charge, but customer electricity/hardware are not free.

**The following USD numbers are an unapproved discussion example, not published customer terms, a price commitment, a provider quote, measured costs, or authorization to spend or charge.** Approval, provider selection and workload measurements are still required before adopting a rate card.

| Illustrative item | Hypothetical proposal |
| --- | --- |
| Credit denomination | 1 credit = $0.01 USD; a $10 balance = 1,000 credits |
| Cloud CPU compute | 20 credits per allocated CPU-instance hour = $0.20/hour; instance specification still to be chosen |
| Cloud GPU compute | 120 credits per allocated GPU-instance hour = $1.20/hour; GPU specification still to be chosen |
| Metering example | 10 allocated GPU minutes at that rate = 20 credits = $0.20, before separately disclosed storage/transfer/taxes |
| Hypothetical economics | If provider GPU cost were $0.80/hour, $1.20/hour revenue leaves $0.40/hour (33.33%) before storage, transfer, idle capacity, payment fees, support, taxes and other overhead |

Arithmetic was checked with Python; the cost assumption was **not measured**. Do not infer a margin or sustainable business from it. Measure provisioned versus useful runtime, startup/teardown, idle capacity, retries and all ancillary charges; define rounding, minimums, refunds, cap headroom and retention before a real offer. Show dollar equivalents and distinguish cloud charges from local work. Keep rate revisions/ledger policy private and provider-neutral usage facts public; preserve stock OSS Lago/native Stripe rather than building billing into the local loop.

## Non-goals and next gate

No new chat/editor, full Patch clone, universal multi-cloud scheduler, price arbitrage, Kubernetes requirement, new identity/billing/queue framework, shared cross-business customer database, production deployment or paid cloud test. No Remesher/Crusher adapter implementation in this delivery sequence.

Next implementation gate (not authorized by this PR): freeze the exact local runtime file map and acceptance fixture, then prove the first local loop. Before cloud work, verify compatible local/cloud runtimes and actual provider capacity, secure explicit upload permission and a real dollar budget. The unapproved rate illustration is not that budget. Full Hermes capability parity, hosted live-session provisioning, plugin distribution and later hybrid rendering remain separately evidenced work; do not let them replace the first useful local outcome.

## Reference and compatibility notes

The earlier plan inspected https://docs.patch.gg/blender, https://docs.patch.gg/cli, https://docs.patch.gg/mcp-server, https://patch.gg/api/v1/openapi.json and https://www.npmjs.com/package/@patchgg/cli on 2026-11-17. These are interface references, not interoperability certification. The CLI package's MIT metadata does not imply its hosted backend is open source. Verify actual code licenses/notices before reuse and preserve Fstack core's Apache-2.0 boundary.
