import json
from concurrent.futures import ThreadPoolExecutor
from uuid import UUID

import pytest
from sqlalchemy import Engine, text

from fstack.billing import parse_entitlement_revision_request
from fstack.billing_service import (
    EntitlementPersistenceError,
    EntitlementRevisionConflict,
    synchronize_entitlement_revision,
)


def _request(**overrides: object):
    payload: dict[str, object] = {
        "organization_slug": "acme",
        "entitlement_revision_id": "11111111-1111-4111-8111-111111111111",
        "generation": 1,
        "plan_key": "crusher.single.v1",
        "state": "active",
        "capability": "crusher.deployment.create",
        "quota_dimension": "crusher.deployment.active",
        "quota_limit": 1,
        "observed_at": "2026-10-17T16:00:00.000000Z",
        "valid_until": "2026-10-17T16:05:00.000000Z",
        "source_revision_id": "33333333-3333-4333-8333-333333333333",
    }
    payload.update(overrides)
    return parse_entitlement_revision_request(json.dumps(payload).encode())


@pytest.mark.postgres
def test_entitlement_revision_sync_creates_stable_subject_and_replays_exact_source(
    migrated_database: Engine,
) -> None:
    request = _request()

    with migrated_database.begin() as connection:
        created = synchronize_entitlement_revision(connection, request=request)
    with migrated_database.begin() as connection:
        replayed = synchronize_entitlement_revision(connection, request=request)

    assert created.entitlement_subject_id == replayed.entitlement_subject_id
    assert created.entitlement_revision_id == UUID("11111111-1111-4111-8111-111111111111")
    assert created.replayed is False
    assert replayed.replayed is True

    with migrated_database.connect() as connection:
        subject_count = connection.execute(
            text("SELECT count(*) FROM entitlement_subjects")
        ).scalar_one()
        revision = connection.execute(
            text(
                """
                SELECT subject_id, generation, state, payload_digest
                FROM entitlement_revisions
                """
            )
        ).one()
    assert subject_count == 1
    assert revision == (
        created.entitlement_subject_id,
        1,
        "active",
        request.payload_digest(created.entitlement_subject_id),
    )


@pytest.mark.postgres
def test_entitlement_revision_sync_conflicts_leave_committed_revision_unchanged(
    migrated_database: Engine,
) -> None:
    original = _request()
    with migrated_database.begin() as connection:
        created = synchronize_entitlement_revision(connection, request=original)

    conflicts = [
        _request(state="inactive"),
        _request(
            entitlement_revision_id="44444444-4444-4444-8444-444444444444",
            source_revision_id="55555555-5555-4555-8555-555555555555",
        ),
        _request(
            organization_slug="other",
            entitlement_revision_id="66666666-6666-4666-8666-666666666666",
        ),
    ]
    for conflicting in conflicts:
        with pytest.raises(EntitlementRevisionConflict), migrated_database.begin() as connection:
            synchronize_entitlement_revision(connection, request=conflicting)

    with migrated_database.connect() as connection:
        subjects = (
            connection.execute(
                text("SELECT subject_key FROM entitlement_subjects ORDER BY subject_key")
            )
            .scalars()
            .all()
        )
        revision = connection.execute(
            text(
                "SELECT id, subject_id, generation, state, source_revision_id, payload_digest "
                "FROM entitlement_revisions"
            )
        ).one()
    assert subjects == ["acme"]
    assert revision == (
        original.entitlement_revision_id,
        created.entitlement_subject_id,
        1,
        "active",
        original.source_revision_id,
        original.payload_digest(created.entitlement_subject_id),
    )


@pytest.mark.postgres
def test_entitlement_revision_sync_accepts_only_strictly_new_generation(
    migrated_database: Engine,
) -> None:
    first = _request()
    second = _request(
        entitlement_revision_id="44444444-4444-4444-8444-444444444444",
        source_revision_id="55555555-5555-4555-8555-555555555555",
        generation=2,
        state="inactive",
    )
    with migrated_database.begin() as connection:
        first_result = synchronize_entitlement_revision(connection, request=first)
    with migrated_database.begin() as connection:
        second_result = synchronize_entitlement_revision(connection, request=second)

    assert second_result.entitlement_subject_id == first_result.entitlement_subject_id
    assert second_result.replayed is False
    with migrated_database.connect() as connection:
        assert connection.execute(
            text("SELECT generation, state FROM entitlement_revisions ORDER BY generation")
        ).all() == [(1, "active"), (2, "inactive")]


@pytest.mark.postgres
def test_concurrent_first_sync_adopts_one_subject_and_revision(
    migrated_database: Engine,
) -> None:
    request = _request()

    def synchronize():
        with migrated_database.begin() as connection:
            return synchronize_entitlement_revision(connection, request=request)

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(lambda _index: synchronize(), range(2)))

    assert results[0].entitlement_subject_id == results[1].entitlement_subject_id
    assert sorted(result.replayed for result in results) == [False, True]
    with migrated_database.connect() as connection:
        assert (
            connection.execute(text("SELECT count(*) FROM entitlement_subjects")).scalar_one() == 1
        )
        assert (
            connection.execute(text("SELECT count(*) FROM entitlement_revisions")).scalar_one() == 1
        )


@pytest.mark.postgres
def test_revision_insert_suppression_is_persistence_failure_and_rolls_back_subject(
    migrated_database: Engine,
) -> None:
    with migrated_database.begin() as connection:
        connection.execute(
            text(
                """
                CREATE FUNCTION suppress_entitlement_revision_insert() RETURNS trigger
                LANGUAGE plpgsql AS $$ BEGIN RETURN NULL; END; $$
                """
            )
        )
        connection.execute(
            text(
                """
                CREATE TRIGGER tr_suppress_entitlement_revision_insert
                BEFORE INSERT ON entitlement_revisions
                FOR EACH ROW EXECUTE FUNCTION suppress_entitlement_revision_insert()
                """
            )
        )

    with pytest.raises(EntitlementPersistenceError), migrated_database.begin() as connection:
        synchronize_entitlement_revision(connection, request=_request())

    with migrated_database.connect() as connection:
        assert (
            connection.execute(text("SELECT count(*) FROM entitlement_subjects")).scalar_one() == 0
        )
        assert (
            connection.execute(text("SELECT count(*) FROM entitlement_revisions")).scalar_one() == 0
        )
