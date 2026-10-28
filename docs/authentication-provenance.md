# Authentication provenance and publication scope

The owner explicitly approved Apache-2.0 publication of this adapted Fstack authentication candidate. The public core license is in `LICENSE`, with matching Python package metadata. This approval covers the adaptation published here, not a blanket relicensing of the Crusher repository, private Fstack Cloud code, or third-party dependencies. External dependencies retain their respective licenses and notices.

The inspected Crusher source revision was `06e393ad832587a8d7c69498d346ed386f24371b`. It had no inspected repository LICENSE or project license declaration; approval is therefore recorded as the owner's explicit permission for this adaptation, not inferred from a preexisting Crusher open-source license.

Fstack adapts the pending-dashboard-session, hashed-secret, locked one-use consumption, distinct browser/bearer credentials and membership-recheck design. The adaptation uses UUID principals, trusted explicit owner enrollment, browser-bound explicit confirmation, fixed delegated scopes, bounded agent lifetime and fresh expiry checks after authority-lock waits. It does not copy proprietary billing implementation, customer fixtures or live credentials. Existing third-party attribution is not removed or superseded.

The owner’s publication decision does not establish production readiness or authorize deployment. Independent implementation review and retained isolated browser acceptance are separate from the license/metadata/documentation review and protected PR/CI delivery gate. See `STATUS.md`.
