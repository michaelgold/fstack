# Trusted identity consumers

`fstack.identity.IdentityAuthority(configuration)` exposes the same authorization
checks used by the installed public identity routes. This is a trusted in-process
composition surface, not a new authentication protocol or remote assertion format.

```python
with engine.begin() as connection:
    principal_id = authority.require_owner(connection, request, organization_id)
```

`require_owner` validates a real current browser session and current active owner
membership, rejects Authorization headers (including agent credentials), locks the
session/principal/membership rows, and rechecks session expiry using a fresh database
clock after the last authority lock. `organization_id` selects the membership to
check; possessing its UUID is never authorization. It returns the existing UUID
principal ID without inventing a second identity namespace.

The caller owns the transaction and must set appropriate bounded database/pool
limits. End this short transaction before external I/O. Reauthorize the **request**
in a new transaction after I/O before returning protected data; an earlier principal
UUID does not remain authorized indefinitely. Basic `human`, `member`, and `live`
methods remain available to the existing router for routes with additional locks;
callers must perform the final `live` check after every later authority lock/write.

All normal self-hosted routes continue to use this implementation. No third-party
identity service or private distribution is required. Agent delegation retains its
separate scopes/audience and does not implicitly acquire human-owner permissions.
