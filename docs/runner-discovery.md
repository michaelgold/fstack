# User-managed runner availability (bounded prerequisite)

This optional Core path lets a signed-in owner enroll a machine, a runner send an outbound availability report, and the owner's existing `account:read` agent discover it. **It cannot launch sessions, return Tailcat addresses, execute jobs, spend credits, or provision cloud resources.** Account-free direct bpy.dev operation is unchanged.

Enable the existing installation identity configuration and migrate to head. No vendor account, private package or billing service is required. The installation uses its existing HTTPS API; the runner opens no listener and needs no Docker socket.

## Owner enrollment

Using the existing browser's owner session, Origin and CSRF header, POST:

```text
/v1/auth/accounts/<account UUID>/runners
```

```json
{
  "machine_class": "user-cpu",
  "image": "YOUR_REGISTRY/YOUR_IMAGE@sha256:YOUR_VERIFIED_DIGEST",
  "ttl_seconds": 3600
}
```

The image must be a real lowercase digest reference, not the placeholder above. This records the owner's declaration; it does not attest that an image was pulled or a workload ran. TTL is 60–86400 seconds. Save the returned `runner_token` in an owner-only regular file (mode 0600) on the runner. Do not put it in command arguments or logs. Only its HMAC digest is stored by the coordinator. Enrollment is an explicit one-shot owner action, not an automatic retry operation: a lost response creates an unusable enrollment that expires; re-enrollment creates a new identity. No execution or financial side effect exists in this slice.

## Outbound runner report

```sh
uv run python -m fstack.runner_client \
  --origin https://YOUR_INSTALLATION \
  --runner-id OWNER_ENROLLED_UUID \
  --token-file /PRIVATE/PATH/runner-token \
  --sequence 1 --available-slots 1 --queue-depth 0
```

The command performs one bounded HTTPS POST. It disables redirects and ambient proxies, never prints the credential or server error body, and returns only an acknowledgement. Send a larger integer sequence for each new observation. After an ambiguous failure retry the **identical** sequence and body; duplicate reports preserve the original timestamp. Older or changed replays fail. Slots and queue depth are runner observations, not capacity guarantees or latency measurements. There is no daemon, periodic scheduler or assignment poll in this increment.

## Agent discovery

Use the existing owner-approved `account:read` bearer grant:

```text
GET /v1/agent/accounts/<account UUID>/runners
```

Each machine includes its immutable digest/class, last-seen time, freshness, and observed queue/slots. Never-seen machines are `unknown`; observations older than 30 seconds are `stale` and return null available capacity/queue. Expired or revoked enrollments are excluded.

The estimate is explicitly **zero vendor compute charge for user-managed execution**, in decimal-string USD per resource-second, not a measured electricity/hardware price. Quote UUID/expiry identify an advisory snapshot, not a persisted accepted offer or admission authority. Job duration, confidence, queue time, cold start, transfer, assembly, accrued and settled cost remain unknown. No managed provider price or credit conversion is invented. `capabilities` describes only the reporting protocol, not Blender/CUDA compatibility.

Owner revocation uses the same session/CSRF authority:

```text
POST /v1/auth/accounts/<account UUID>/runners/<runner UUID>/revoke
```

Revocation is irreversible; re-enroll explicitly. Generic operator tokens and read-only agent grants cannot enroll/revoke a runner.

## Executable acceptance

Use an exclusively owned disposable PostgreSQL database with a name ending `_test`: the repository fixture destructively resets it.

```sh
FSTACK_TEST_DATABASE_URL=postgresql://USER:replace-me@HOST/runner_discovery_test \
FSTACK_REQUIRE_POSTGRES=1 uv run pytest \
  tests/test_runner_discovery_postgres.py tests/test_runner_client.py -q
```

One test opens a real loopback HTTP socket and exercises the outbound client against the packaged API and real PostgreSQL; loopback plaintext is explicitly test-only and unavailable through the CLI. Production TLS is required by the client but this test is not TLS acceptance. Other tests cover current owner/grant scoping, replay, stale/unknown observations, database constraints, expiry and irreversible revocation.

See [the follow-up plan](plans/2026-11-19-runner-discovery.md) for the unimplemented coordinator→session→native MCP path, protected addresses, lifecycle usage, private prepaid credits and dynamic provider offers. This prerequisite is not completion of that vertical.
