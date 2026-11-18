# Remesher and Crusher Adapter Follow-up Implementation Plan

> **For Hermes:** Use the subagent-driven-development skill after separate implementation authorization. This is a small deferred plan, not an approved implementation or deployment.

**Goal:** Test one concrete existing product integration at a time after the useful bpy.dev local/checkpoint-cloud increments, without forcing all products to launch together.

**Architecture:** Keep installation/release and provider-neutral lifecycle contracts in public Fstack; keep product semantics in the owning product. Generalize only a demonstrated mismatch from a second real caller, not speculative reuse.

**Tech stack:** Existing Fstack and product runtime/test tools after source audit; no additional scheduler, workflow framework or billing engine.

## Independence and boundaries

This follow-up is not a dependency or first-release gate for the [bpy.dev local-first plan](2026-11-17-bpy-dev-hybrid-execution.md). Do not add its tests to bpy.dev acceptance as a prerequisite. Separate customer identity, credentials, billing, storage and data for each business. Public core and private `michaelgold/fstack-cloud` remain separate; keep managed commercial operations private, with stock OSS Lago/native Stripe constraints unchanged.

Authenticated repository listing identified `michaelgold/remesher.com` and `michaelgold/crusher` as product candidates. Their authoritative adapter paths/current interfaces must be audited before implementation; repository names alone do not establish runtime contracts. No new product repository is authorized or needed to start that audit.

## Task 1 — select one observed integration need

1. Inspect current product source, README, tests and existing deployment hooks at exact recorded heads. Read public `docs/architecture/first-crusher-slice.md` and `local-crusher-execution-slice.md` before changing Crusher boundaries.
2. Choose one user-visible outcome below and record its exact implementation/test files and actual test command. Stop if ownership or the callable contract is unresolved; do not invent paths.
3. Write one failing caller-facing acceptance test for the actual missing behavior, not a platform model inventory. If existing behavior already passes, retain characterization evidence and avoid an unnecessary implementation.

## Task 2 — implement only the selected adapter outcome

- **Crusher option:** map one existing deployment/readiness/drain lifecycle through installation/release management. Verify correct readiness and drain observations and denied cross-installation access without sending mail. Crusher retains migrations, mail transport, sender policy, suppression and delivery/recovery; email is not a generic GPU task.
- **Remesher option:** execute one bounded pipeline on an explicitly selected compatible local GPU; validate its actual mesh/export artifact and provenance. Remesher retains model-stage dependencies, VRAM selection and domain validation. No cloud spending or shared Blender orchestration is implied.

Run the failing test, implement the smallest adapter change, rerun focused and affected tests, then remove demonstrated duplication. Use real PostgreSQL for persistence/concurrency claims and actual product runtime for output claims. Mocks may isolate errors but do not establish real lifecycle or mesh output acceptance. Keep paid providers, mail sending and deployment outside this follow-up unless separately authorized.

## Task 3 — close the one outcome before expanding

Review exact source and run existing required CI through protected PR delivery. Record caller-visible output, failures, ownership denials, cleanup and version/source evidence. If a concrete contract mismatch warrants a public interface change, review that small change and its second-caller evidence separately; do not introduce a universal abstraction. Re-audit before selecting the other adapter.

**Exit:** one useful verified integration, or an explicit source/contract blocker. Neither completion nor failure changes bpy.dev's local/checkpoint-cloud acceptance or grants permission to deploy, bill or merge.
