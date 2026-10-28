# Authentication delivery status

## Implemented and reviewed locally

- Human magic-email login through the installation's configured Crusher/Kumo delivery path; no live mail delivery claimed.
- Explicit owner-approved agent delegation, limited to `account:read`; agents need no inbox.
- Independent security review's expiry-under-contention blocker repaired and re-reviewed with no blocking repair findings.
- Retained full Python 3.12/PostgreSQL 17 suite: 1,691 passes, zero failures/errors/skips. This publication documentation update did not rerun that suite.
- Retained isolated sandboxed Firefox acceptance: 52 executed assertions and six screenshots. Agent read returned 200 and revocation denied the next read with 403. Six nonblocking favicon CSP events remain; not a clean-console or Chromium pass.

## Publication and delivery

Owner approval for Apache-2.0 publication of the adapted candidate is recorded. The license/metadata/documentation delta is prepared but requires independent review before commit/push. Protected PR/CI delivery and merge have not occurred. No hosted test count or production activation is claimed.

Basic security remains in core. The separately recorded [Pro proposal](pro-auditing-proposal.md) is tentative; no paywall or security removal is implemented. The installation-local authentication path requires no vendor cloud account or license service. Mail delivery uses an operator-configured service, not a mandatory Fstack Cloud integration.

## Explicitly not delivered

Optional OIDC/federation, CLI credential storage, private payment-readiness integration, expanded agent permissions, agent inboxes, production deployment, and live email verification. Private candidate work and other worktrees remain separate; this release does not complete their acceptance gates.
