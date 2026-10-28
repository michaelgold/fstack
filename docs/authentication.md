# Human authentication and agent delegation

**Local review candidate, not published or production enabled.** Built-in login belongs to installation-local Fstack. Agents never need a mailbox, email identity, email approval URL or a billing role.

## Operator setup

Run migrations through `20261028_0007`. Existing organizations remain unchanged and acquire no owner automatically. An installation administrator explicitly calls `fstack.identity.enroll_owner(engine, account_uuid, email)` on the trusted installation, after independently verifying ownership. It creates a new UUID principal and explicit owner membership with only `account:read` delegation. It rejects an already-enrolled email rather than silently merging legacy Crusher integer identities or attaching it to another organization. There is no public bootstrap HTTP route.

Enable `FSTACK_AUTH_MODE=magic` and supply all of:

- `FSTACK_AUTH_ORIGIN`: exact HTTPS browser origin, no trailing slash, path or credentials.
- `FSTACK_AUTH_SECRET`: installation-generated random secret of at least 32 characters, held in the installation's secret store. No default. Rotation invalidates outstanding links, sessions and agent credentials.
- `FSTACK_AUTH_MAIL_ENDPOINT`: installation-owned Crusher HTTPS endpoint ending exactly `/v1/agent/emails/send`.
- `FSTACK_AUTH_MAIL_TOKEN`: installation-owned Crusher service credential with `send:execute`; never a newly delegated Fstack agent credential.
- `FSTACK_AUTH_MAIL_SOURCE`: verified sender configured in that Crusher installation.

Crusher's existing provider configuration routes this normal email API to Kumo; Fstack adds no SMTP service, SES client or Kumo-specific infrastructure. Public trust roots and hostname verification are mandatory. Mail endpoint DNS is resolved once at startup; restart after address changes. The request has a three-second connect/TLS budget, five-second total HTTP budget, 8 KiB header and 4 KiB response body limits through the existing strict HTTP transport. No redirects or retries. An ambiguous mail failure invalidates that pending login instead of reusing the link. No delivery error or inline magic link is returned to the caller. The fixture verifies HTTP/TLS behavior, **not live mail delivery**.

## Callable flow

1. Human opens `/auth/sign-in`, requests email (`POST /v1/auth/login`). Response is uniform `202 check_email`, including an unknown address or mail failure; no account creation.
2. Link opens `/auth/confirm#<signed-token>`. GET is nonconsuming; fragment avoids request/access-log token exposure. The person must click confirmation in the browser that requested the email. Explicit `POST /v1/auth/confirm` requires matching login-binding cookie, CSRF and exact Origin. Replay/expiry fails. The new 14-day session uses a Secure, HttpOnly, SameSite=Strict, host-only cookie.
3. Agent calls `POST /v1/auth/device/start` with `account_id`, a bounded client label and exactly `["account:read"]`. Returns device/user codes, a 10-minute pending expiry and `/auth/device` verification URI. Neither code authorizes approval.
4. Signed-in owner enters the user code at `/auth/device`. Review displays exact account UUID, client label, scopes, request expiry, token lifetime and audience. Explicit approval POST requires the current owner membership, delegable permission, session-bound CSRF and Origin. No owner or billing-admin permission is delegated.
5. Agent polls `POST /v1/auth/device/token` with only its device code. Pending/denied/expired states return denial; only one exchange consumes the approved row. It returns a distinct one-hour `fstack-agent` credential. Request scopes/client/account are immutable; exchange accepts no scope override. Stored credentials are keyed hashes, not raw secrets.
6. `GET /v1/agent/accounts/{account_id}` uses the agent bearer credential for the read-only account operation. Every call rechecks grant expiry/revocation, audience and current authorizing principal/membership/permission. Browser cookies do not substitute for this bearer token and agent tokens do not authenticate browser session routes.
7. Owner uses `POST /v1/auth/grants/{grant_id}/revoke` with browser CSRF/Origin. Subsequent agent requests fail. `POST /v1/auth/logout` revokes the browser session only; it intentionally does not revoke all grants.

All authentication responses are no-store/no-referrer; pages disallow framing and use same-origin scripts. Global installation rate limits bound login, confirmation, device start/review/approval/exchange. Global limits are deliberately simple and can cause installation-wide denial under abuse; they are not a distributed anti-abuse service or constant-time account-enumeration defense.

## Boundary and remaining work

- No optional OIDC/vendor integration, federation, CLI credential storage, private payment-readiness connection, billing changes, production configuration, deployment or live email. The stable UUID principal/membership schema is the future shared identity seam, not a claim of implemented third-party login.
- The only delegated operation is read-only account metadata. Existing installation-operator provisioning and billing service-token APIs are untouched; agent permissions do not implicitly grant those operations.
- Retained isolated sandboxed Firefox acceptance passed 52 assertions with six screenshots, including human confirmation, owner approval, agent read (200) and revocation (403). Revoke/logout used browser-origin API calls, not product UI screens. Six implicit favicon CSP violations were retained as nonblocking; this is not clean-console or Chromium certification. This documentation update did not rerun browser acceptance.
- Identity-table retention/admin membership editing UI and migration of legacy Crusher identities are not included. Installation administrators own trusted enrollment, revocation and retention. Global request ingress size/rate controls remain an installation responsibility.
- The owner explicitly approved publication of the adapted authentication candidate under Apache-2.0. See [provenance and scope](authentication-provenance.md); this is not a license grant for the entire Crusher repository. No proprietary billing source, source inventory rewrites, customer fixtures or live credentials were copied. Publication metadata review and protected PR/CI delivery remain pending.

## Verification

Run with Python 3.12 and an owned PostgreSQL 17 database ending `_test`:

```sh
unset PYTHONPATH
export PYTHONDONTWRITEBYTECODE=1 FSTACK_REQUIRE_POSTGRES=1
# Set FSTACK_TEST_DATABASE_URL to the disposable database, never production.
uv sync --frozen
uv run pytest -q
uv run ruff check .
uv run ruff format --check .
uv run mypy
```

CI runs the existing full test partition with PostgreSQL required. New controls exercise mounted login/delegation/revocation, real verified TLS fixture mail, immutable state, wrong account/owner/CSRF, exchange escalation/replay/expiry, current membership revocation, secret rotation, privacy and deterministic overlapping exchange with four observed PostgreSQL row-lock waiters and exactly one grant. Initial flow, immutable approved binding and missing browser pages have retained RED evidence; other controls are source-derived retrospective characterization. Full-run failures exposed an application engine disposal leak and hard-coded migration-head assertions; the candidate closes engine pools at lifespan shutdown and updates only current-head expectations, not historical migrations.
