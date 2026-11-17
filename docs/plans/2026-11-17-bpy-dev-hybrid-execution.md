# bpy.dev Hybrid Execution and Fstack Boundaries Implementation Plan

> **For Hermes:** Use the subagent-driven-development skill to implement this plan task-by-task. This document authorizes no deployment, cloud spending, or implementation by itself.

**Goal:** Let an existing coding agent create a Blender scene and render independent frames across a customer-owned GPU and bpy.dev cloud capacity, with a complete local-only path.

**Architecture:** Fstack manages installations, releases, registered compute capacity, access, placement policy, and durable operations. Product adapters own workload semantics: bpy.dev owns Blender sessions and frame decomposition; Remesher owns mesh pipelines; Crusher owns email behavior. Start with explicit placement and one cloud provider rather than a general-purpose scheduler.

**Tech stack:** Reuse Fstack's Python/FastAPI, PostgreSQL, identity, and durable provisioning contracts where applicable. Use Blender/bpy runners on existing Docker hosts and RunPod as the first proposed cloud GPU provider. Audit the existing product repositories before assigning runtime modules or introducing dependencies; do not select another queue or workflow engine merely for this slice.

## Status and approved decisions

Planning only. These capabilities are proposed, not implemented or certified by this document.

The user approved these defaults in conversation:

- Agent-first CLI/MCP, with a small web view for previews, jobs, artifacts, and costs; no new chat application or browser Blender editor.
- Initial hybrid demonstration: the user's RTX 5090 machine plus one RunPod GPU; CPU execution uses the same runner contract. Availability and compatible Blender/GPU runtimes must be checked before execution.
- Open-source deployment management, runners, product adapters, and enough coordination to run entirely on customer-owned machines without a vendor login or billing service.
- Charge for provided cloud compute and managed operations. Decide optional commercial team/compliance features separately; basic local security remains core.
- bpy.dev, Remesher, and Crusher remain separate branded businesses with separate customer identity, credentials, billing, storage, and data boundaries.
- Patch is a workflow/interface reference, not the product specification. Do not commit to complete Patch compatibility.

## Repository context and reconciliation

On 2026-11-17, the public Fstack main README and tree were inspected through GitHub. Existing modules include `src/fstack/identity.py`, `provisioning.py`, `runtime_state.py`, `runtime_submission.py`, `runtime_application.py`, `docker_cli.py`, and the Temporal workflow/dispatch modules. Existing public contracts are documented in `docs/architecture/first-crusher-slice.md`, `local-crusher-execution-slice.md`, `temporal-provisioning.md`, and `first-billing-plane-slice.md`.

The README currently puts managed fleet orchestration and hosted billing in private `fstack-cloud`, and names Remesher as the second abstraction proof. This plan proposes a narrower refinement: customer-operated runner registration, explicit placement, and coordination belong in public core; vendor capacity management and hosted commercial operations remain private. bpy.dev becomes the first hybrid-compute proof, while Remesher remains the second distinct GPU workload. Reconcile these statements in a documentation PR before implementing; do not silently rewrite frozen acceptance contracts.

### Repository ownership

- **`michaelgold/fstack` (public core):** installation/release contracts, runner registration, authorization, explicit placement/reservation primitives, local coordination, and provider adapter interfaces. Owns the platform portions of Slices 0–4.
- **`michaelgold/fstack-cloud` (private cloud):** vendor capacity procurement/operations, managed credentials, hosted quotas, rating and billing integration. Owns the managed-service portion of Slice 3 and managed cost reporting in Slices 4–5. Preserve unmodified OSS Lago and native Stripe; extend around Lago, not inside its core.
- **bpy.dev product repository (locate in Slice 0):** Blender runners, sessions, scene packaging, frame decomposition, rendering validation, output assembly, and product HTTP/SDK/CLI/MCP/web interfaces. Owns product execution in Slices 1–5.
- **Remesher and Crusher product repositories:** their domain-specific adapters and validation in Slice 6; no migration of product semantics into Fstack core.

This plan does not create another core/cloud repository or merge the existing repositories. Product runners and self-hosted adapters remain open source; managed business operations remain private. A generic self-hosted coordinator dispatches opaque tasks, while the product creates their dependency and retry semantics.

## Product/platform dividing line

### Public Fstack responsibilities

- Installation and release identity; desired/observed deployment state.
- Registration of explicitly trusted runners, supported runtime capabilities, health, and availability.
- Lifecycle operations and durable placement/reservation handles.
- Human-owned delegated agent credentials, authorization, revocation, and local audit records.
- Local job coordination with explicit worker selection; customer-operated installations must not phone home.
- Neutral usage facts and adapter interfaces, without embedded vendor billing identities.

### Product adapter responsibilities

- **bpy.dev:** Blender runtime/version selection, persistent sessions, bpy execution, inspection/previews, scene packaging, checkpoints, frame/variation decomposition, render validation and output assembly.
- **Remesher:** model/pipeline selection, stage dependencies, VRAM requirements, mesh/rig/export artifacts and domain-specific validation.
- **Crusher:** services, migration/readiness/drain hooks, mail transport, sender policy, suppression and delivery/recovery semantics. Do not model email deliveries as generic GPU jobs.

Adapters declare required resources and supply lifecycle/readiness/drain hooks. Fstack must not import Blender scene logic, mesh logic, or email-recipient logic into its generic placement layer.

### Private managed-service responsibilities

Vendor fleet capacity and procurement, managed credentials, hosted billing/rating, commercial quotas, and cross-installation operations. Keep each branded business's customer realm and ledger separate. Public usage/security contracts remain usable without these services.

## First acceptance demonstration

1. An existing coding agent connects through CLI or MCP to a local/self-hosted endpoint.
2. A CPU runner creates a small editable scene, returns a preview, and saves an immutable scene/artifact manifest.
3. The user explicitly permits cloud upload for this scene and specifies a cloud spending cap.
4. bpy.dev decomposes independent animation frames into deterministic tasks.
5. Explicit placement assigns some tasks to the customer RTX 5090 runner and some to one RunPod GPU runner.
6. The coordinator collects verified outputs without duplicate accepted frames and reports which runner produced each one.
7. The user downloads the editable `.blend`, frame outputs and provenance, and sees cloud charges separately from local execution.
8. A local-only run succeeds with cloud credentials absent and vendor endpoints unavailable.

No claim of shared GPU memory or transparent cross-machine rendering of one arbitrary frame. Multiple GPUs within one machine are a separate Blender/backend capability. Cross-machine work initially consists of independent frames, scenes, cameras or parameter variations. Simulations requiring sequential cache state are excluded until explicitly supported.

## Minimal execution model

Reuse existing entities and operation contracts before adding new ones. The implementation audit should establish the minimum equivalent of:

- **Runner:** installation-scoped identity, allowed capabilities, pinned runtime, resource limits and heartbeat.
- **Job:** owner, immutable inputs, product operation, placement policy and maximum cloud spend.
- **Task attempt:** deterministic logical task identity plus execution generation, claimed runner, lease and durable outcome.
- **Artifact manifest:** authorized object references, content hashes, sizes, runtime provenance and dependencies.

Use enforced transitions, transactional conditional updates and narrow typed operations—not arbitrary enum assignments. A timed-out caller must retrieve the existing operation rather than start another billable job. Keep logical task success separate from attempt-level usage: a retried frame can incur real compute even when only one output is accepted.

Interactive scene mutation stays on one runner. Save/checkpoint before parallel rendering. Pin compatible Blender versions, render settings and dependencies across workers. Do not promise byte-identical GPU images across different architectures; define output completeness and appropriate visual tolerance instead.

## Delivery sequence

### Slice 0 — audit and freeze the smallest contract

**Files to read:** current Fstack modules and architecture documents listed above; `docs/STATUS.md`; current bpy.dev/buildbpy, Remesher and private cloud repository interfaces after locating their authoritative repositories.

**Documentation outputs proposed:**
- `docs/architecture/hybrid-product-execution.md`
- Update `README.md` and `docs/STATUS.md` to link this plan and explain the public/private boundary refinement.

Steps:
1. Record exact repository/base revisions and existing reusable APIs, runner code and authentication.
2. Locate the actual bpy.dev repository/package; do not invent one or put Blender execution into Fstack to avoid the question.
3. Map each acceptance step to an existing component or one required addition.
4. Select supported runtime/version pins and write explicit request/result/error contracts.
5. Record whether existing Temporal machinery is sufficient. Do not generalize workflow infrastructure without a concrete missing behavior.

Gate: reviewed contract and exact implementation file map. No new universal scheduler, Kubernetes requirement or provider marketplace.

### Slice 1 — one local CPU runner end to end

**Proposed Fstack additions, subject to Slice 0 reuse audit:** `src/fstack/runner_contracts.py`, `src/fstack/runner_registry.py`; tests `tests/test_runner_contracts.py`, `tests/test_runner_registry_postgres.py`. Extend existing identity/API seams only where needed. Product-specific execution files belong in the located bpy.dev repository.

Steps:
1. Write failing tests for scoped runner registration, unauthorized cross-installation access and bounded capability declarations.
2. Implement only enough registration and dispatch to execute one Blender script on a local CPU runner.
3. Return structured status, logs, a preview and downloadable `.blend` via the product API.
4. Verify actual Blender execution, ownership isolation and local-only operation with vendor network access disabled.

Gate: real CPU-produced artifacts, not mocked execution; no cloud account required.

### Slice 2 — local GPU with the same client surface

**Proposed tests:** `tests/test_runner_placement_postgres.py`; product tests for CPU/GPU capability rejection and render output validity.

Steps:
1. Add explicit compatible-runner selection and resource reservations.
2. Verify live GPU/runtime capability rather than relying on machine labels.
3. Run the same saved scene on the local GPU, preserving inputs and runtime provenance.
4. Reject unsupported backends or insufficient memory with useful errors; never silently start paid cloud work.

Gate: CPU and GPU operations use the same product-facing interface with explicit capabilities.

### Slice 3 — one cloud GPU and one bounded job

**Location:** RunPod target adapter at the public provider-extension boundary established in Slice 0; vendor-owned credentials and billing composition in private `fstack-cloud`. Customer-supplied RunPod credentials must remain usable without our managed cloud.

Steps:
1. Test launch, lookup, cancellation and teardown using provider fixtures, including lost responses and failed termination.
2. Add immutable image/artifact staging and explicit upload authorization.
3. Reserve a bounded spend allowance before starting paid resources; reconcile actual usage afterward.
4. Run one authorized real-provider smoke with a user-approved dollar cap and termination deadline.
5. Read back actual resource termination; report residual provider charges and storage separately.

Gate: actual cloud output plus verified teardown and metering. Fixtures are not live-provider evidence. A client timeout does not authorize a second instance.

### Slice 4 — hybrid frame fan-out

**Proposed Fstack additions if not already present:** `src/fstack/job_dispatch.py`; tests `tests/test_job_dispatch_postgres.py`, `tests/test_hybrid_job_recovery_postgres.py`. bpy.dev owns frame selection and assembly.

Steps:
1. Split a fixed saved scene into independent frame tasks with immutable input identity.
2. Place tasks explicitly across the local GPU and one cloud runner; keep policies local-only/cloud-only/hybrid.
3. Handle a disconnected local runner and an interrupted cloud attempt with durable attempt reconciliation.
4. Verify exactly one accepted output per requested frame, preserved provenance, and honest attempt-level cost reporting.
5. Demonstrate cancellation, exhausted budget and denied cloud upload without leaving untracked paid resources.

Gate: the full acceptance demonstration above, including local-only negative network control.

### Slice 5 — thin clients and product presentation

Implement one canonical HTTP contract, then a small Python SDK, CLI and MCP mapping around it. Do not maintain three independent execution engines. Proposed client methods are illustrative, not a frozen API: start session, run script, preview, save, submit render, inspect job, download outputs, cancel.

Add only a basic page showing job/session state, preview, files and costs. No new agent chat, node editor or streamed Blender desktop in this slice. Provide useful structured errors and resumable job handles.

Evaluate a narrow Patch Blender adapter only after a real existing client/workflow benefits. Compatibility must explicitly identify supported endpoints and behavior; a look-alike interface is not a drop-in compatibility claim.

### Slice 6 — validate reuse without broadening the first release

- Crusher: map its existing service deployment/readiness/drain hooks into the same installation/release management boundary; preserve its product-owned migration and mail semantics.
- Remesher: register one GPU pipeline through the runner capability/artifact contracts. Keep model-stage orchestration inside Remesher.
- Generalize only a concrete contract mismatch observed in these integrations. Do not require all products to launch together.

## Verification and implementation discipline

For every code slice: demonstrate the intended failing test, make the minimal implementation pass, run affected integration tests, review the exact candidate, then use protected PR delivery. Use real PostgreSQL for persistence, concurrency and ownership assertions; no SQLite substitution.

Existing Fstack development uses `uv sync --frozen` and `uv run pytest -q` with an explicit `FSTACK_TEST_DATABASE_URL`. Re-read current CI before choosing exhaustive commands: this document does not claim that a generic pytest invocation covers every required gate. Proposed test paths above must be included in mandatory CI discovery. Use separate disposable test databases, not production.

Live acceptance must separately establish:
- Local CPU and actual GPU execution.
- Agent ownership/scopes, runner revocation and cross-installation isolation.
- Immutable input packaging, complete outputs and authorized artifact access.
- Retry/cancellation correctness, orphan-resource detection and cleanup readback.
- Cloud upload consent and local-only operation without vendor services.
- Spend reservation, hard admission limits, provider termination behavior and final cost reconciliation.
- Version/dependency compatibility and representative visual correctness.

A budget cap is not an assertion that a provider can never bill after an API outage. Document stopping latency and conservative headroom; stop new admission before the remaining allowance is insufficient.

## Security and data handling

A runner executes agent-controlled code. An ordinary container sharing a rootful Docker daemon is not a hostile-tenant isolation guarantee. For the first slice, use customer-trusted private runners and isolated managed job instances; do not schedule unrelated tenants into the same unrestricted runtime. Never hand agent code cloud control-plane credentials or a Docker socket.

Customer-owned runners should make authenticated outbound connections; do not require exposing a workstation's management port publicly. Scope artifact access per installation/job. Explicitly decide which scene assets may leave the machine, and offer a local-only policy that cannot silently fall back to cloud.

## Non-goals

- Full Patch platform or SDK cloning; games, repository hosting and publishing.
- Universal multi-cloud scheduling or automatic price arbitrage.
- Transparent multi-machine single-frame rendering, distributed memory or arbitrary distributed simulation.
- New identity platform, billing engine, queue framework or workflow rewrite.
- A shared customer account/database across the three businesses.
- Production fleet/compliance features before the first usable hybrid render.
- Deployment of this plan or a paid cloud test without the appropriate separate authorization.

## Business model

Keep customer-operated core useful and open. Monetize provided cloud CPU/GPU capacity and optional managed operation. Show understandable money equivalents and distinguish cloud charges from customer-owned compute; do not imply local electricity is free. Start with prepaid/bounded cloud usage rather than an unlimited AI subscription. Bring-your-own-agent avoids forcing customers to purchase a second reasoning product.

## Reference and compatibility notes

Inspected on 2026-11-17:
- https://docs.patch.gg/blender — session, checkpoint, rendering and usage workflow.
- https://docs.patch.gg/cli — CLI/agent integration and structured responses.
- https://docs.patch.gg/mcp-server — MCP mapping over the HTTP API.
- https://patch.gg/api/v1/openapi.json — published HTTP surface.
- https://www.npmjs.com/package/@patchgg/cli — inspected package metadata declares MIT and links to https://github.com/patch-gg/pch.

These sources establish an interface reference, not a tested interoperability claim. Confirm actual repository licenses and dependency notices before reusing code; the CLI license does not imply that Patch's hosted backend is open source. Preserve Fstack core's Apache-2.0 boundary and any third-party notices.

## Decisions still requiring implementation-time evidence

No further product-direction decision is needed to begin Slice 0. Before spending or deployment, establish the actual bpy.dev repository, usable local runner host, supported Blender/GPU version combination, RunPod account/region capacity, an explicit test budget, and data-upload permission. Pick concrete numbers from measured workloads rather than adding speculative configuration up front.
