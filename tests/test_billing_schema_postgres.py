from __future__ import annotations

import json
import runpy
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from threading import Event
from time import sleep
from uuid import UUID, uuid4

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import Engine, inspect, text
from sqlalchemy.exc import DBAPIError, IntegrityError

from fstack import ingress_state, runtime_state
from fstack.ingress_submission import prepare_operation_bundle
from fstack.provisioning import ProvisioningRequest
from fstack.service import create_provisioning_operation


def _create_operation(engine: Engine, suffix: str) -> dict[str, str]:
    request = ProvisioningRequest(
        organization_slug=f"billing-{suffix}",
        organization_name=f"Billing {suffix}",
        business_slug=f"mailer-{suffix}",
        business_name=f"Mailer {suffix}",
        environment_slug="production",
        public_domain=f"billing-{suffix}.example.com",
        crusher_image="ghcr.io/michaelgold/crusher@sha256:" + "a" * 64,
        database_secret_ref=f"secret://vault/{suffix}/database",
        object_storage_secret_ref=f"secret://vault/{suffix}/storage",
    )
    with engine.begin() as connection:
        result = create_provisioning_operation(
            connection,
            request=request,
            idempotency_key=f"billing-schema-{suffix}",
            actor_principal="installation-operator",
            entitlement_mode="disabled",
        )
    return {
        "operation_id": str(result.operation_id),
        "organization_id": str(result.organization_id),
        "deployment_id": str(result.deployment_id),
    }


def _set_fixture_original_required(engine: Engine, operation_id: object) -> None:
    with engine.begin() as connection:
        for trigger in (
            "tr_operations_authorization_bundle",
            "tr_operations_authorization_mode_immutable",
        ):
            connection.execute(
                text("ALTER TABLE provisioning_operations DISABLE TRIGGER " + trigger)
            )
        connection.execute(
            text(
                "UPDATE provisioning_operations SET authorization_mode='required' "
                "WHERE id=:operation_id"
            ),
            {"operation_id": operation_id},
        )
        for trigger in (
            "tr_operations_authorization_mode_immutable",
            "tr_operations_authorization_bundle",
        ):
            connection.execute(
                text("ALTER TABLE provisioning_operations ENABLE TRIGGER " + trigger)
            )


def _create_required_authorization(
    engine: Engine,
    suffix: str,
    *,
    revision_state: str = "active",
    observed_at: datetime | None = None,
    valid_until: datetime | None = None,
    decided_at: datetime | None = None,
    request_fingerprint: str | None = None,
    newer_revision_state: str | None = None,
    fixture_original_required: bool = True,
    fixture_not_required_authorization: bool = False,
) -> dict[str, object]:
    created = _create_operation(engine, suffix)
    if fixture_original_required and not fixture_not_required_authorization:
        _set_fixture_original_required(engine, created["operation_id"])
    subject_id = uuid4()
    revision_id = uuid4()
    authorization_id = uuid4()
    usage_fact_id = uuid4()
    subject_key = f"billing-{suffix}"
    boundary_time = decided_at or datetime.now(UTC)
    observed_at = observed_at or boundary_time - timedelta(minutes=1)
    valid_until = valid_until or boundary_time + timedelta(minutes=4)

    with engine.begin() as connection:
        operation_fingerprint = connection.execute(
            text("SELECT request_fingerprint FROM provisioning_operations WHERE id=:operation_id"),
            {"operation_id": created["operation_id"]},
        ).scalar_one()
        connection.execute(
            text(
                """
                INSERT INTO entitlement_subjects (id, subject_type, subject_key)
                VALUES (:id, 'organization_slug', :subject_key)
                """
            ),
            {"id": subject_id, "subject_key": subject_key},
        )
        connection.execute(
            text(
                """
                INSERT INTO entitlement_revisions (
                    id, subject_id, generation, plan_key, state, capability,
                    quota_dimension, quota_limit, observed_at, valid_until,
                    source_revision_id, payload_digest
                ) VALUES (
                    :id, :subject_id, 1, 'crusher.single.v1', :state,
                    'crusher.deployment.create', 'crusher.deployment.active', 1,
                    :observed_at, :valid_until,
                    :source_revision_id, :payload_digest
                )
                """
            ),
            {
                "id": revision_id,
                "subject_id": subject_id,
                "state": revision_state,
                "observed_at": observed_at,
                "valid_until": valid_until,
                "source_revision_id": uuid4(),
                "payload_digest": "d" * 64,
            },
        )
        if newer_revision_state is not None:
            connection.execute(
                text(
                    """
                    INSERT INTO entitlement_revisions (
                        id, subject_id, generation, plan_key, state, capability,
                        quota_dimension, quota_limit, observed_at, valid_until,
                        source_revision_id, payload_digest
                    ) VALUES (
                        :id, :subject_id, 2, 'crusher.single.v1', :state,
                        'crusher.deployment.create', 'crusher.deployment.active', 1,
                        :observed_at, :valid_until, :source_revision_id, :payload_digest
                    )
                    """
                ),
                {
                    "id": uuid4(),
                    "subject_id": subject_id,
                    "state": newer_revision_state,
                    "observed_at": observed_at,
                    "valid_until": valid_until,
                    "source_revision_id": uuid4(),
                    "payload_digest": "c" * 64,
                },
            )
        if not fixture_original_required and not fixture_not_required_authorization:
            connection.execute(
                text(
                    "UPDATE provisioning_operations SET authorization_mode='required' "
                    "WHERE id=:operation_id"
                ),
                {"operation_id": created["operation_id"]},
            )
        if fixture_not_required_authorization:
            connection.execute(
                text(
                    "ALTER TABLE provisioning_authorizations DISABLE TRIGGER "
                    "tr_authorizations_operation_bundle"
                )
            )
        connection.execute(
            text(
                """
                INSERT INTO provisioning_authorizations (
                    id, usage_fact_id, subject_id, revision_id,
                    organization_id, operation_id, deployment_id, capability,
                    quota_dimension, units, request_fingerprint, decided_at
                ) VALUES (
                    :id, :usage_fact_id, :subject_id, :revision_id,
                    :organization_id, :operation_id, :deployment_id,
                    'crusher.deployment.create', 'crusher.deployment.active', 1,
                    :request_fingerprint, :decided_at
                )
                """
            ),
            {
                "id": authorization_id,
                "usage_fact_id": usage_fact_id,
                "subject_id": subject_id,
                "revision_id": revision_id,
                "organization_id": created["organization_id"],
                "operation_id": created["operation_id"],
                "deployment_id": created["deployment_id"],
                "request_fingerprint": request_fingerprint or operation_fingerprint,
                "decided_at": boundary_time,
            },
        )

    if fixture_not_required_authorization:
        with engine.begin() as connection:
            connection.execute(
                text(
                    "ALTER TABLE provisioning_authorizations ENABLE TRIGGER "
                    "tr_authorizations_operation_bundle"
                )
            )

    return {
        **created,
        "subject_id": subject_id,
        "revision_id": revision_id,
        "authorization_id": authorization_id,
        "usage_fact_id": usage_fact_id,
        "boundary_time": boundary_time,
        "request_fingerprint": operation_fingerprint,
    }


def _commit_terminal_success_bundle(
    engine: Engine,
    created: dict[str, object],
    monkeypatch: pytest.MonkeyPatch,
    *,
    boundary_time: datetime | None = None,
) -> datetime:
    monkeypatch.setenv("FSTACK_DOCKER_NETWORK", "fstack-test")
    monkeypatch.setenv("FSTACK_SECRET_STAGE_ROOT", "/var/lib/fstack-test-secrets")
    operation_id = UUID(str(created["operation_id"]))
    bundle = prepare_operation_bundle(operation_id)
    runtime_state.persist_submitted(bundle.runtime_submission_id, "billing-schema-runtime")
    ingress_state.mark_submitted(bundle.ingress_submission_id, operation_id)

    with engine.begin() as connection:
        if boundary_time is None:
            boundary_time = connection.execute(text("SELECT clock_timestamp()")).scalar_one()
        connection.execute(
            text(
                "UPDATE runtime_submissions SET state='confirmed', confirmed_at=:boundary "
                "WHERE id=:id AND state='submitted'"
            ),
            {"id": bundle.runtime_submission_id, "boundary": boundary_time},
        )
        connection.execute(
            text(
                "UPDATE ingress_submissions SET state='confirmed', confirmed_at=:boundary "
                "WHERE id=:id AND state='submitted'"
            ),
            {"id": bundle.ingress_submission_id, "boundary": boundary_time},
        )
        connection.execute(
            text(
                "UPDATE provisioning_operations SET state='succeeded', "
                "completed_at=:boundary, updated_at=:boundary "
                "WHERE id=:id AND state='running'"
            ),
            {"id": operation_id, "boundary": boundary_time},
        )
        actor_principal = connection.execute(
            text("SELECT actor_principal FROM provisioning_operations WHERE id=:id"),
            {"id": operation_id},
        ).scalar_one()
        connection.execute(
            text(
                """
                INSERT INTO audit_events (
                    id, organization_id, operation_id, event_type,
                    actor_principal, payload
                ) VALUES (
                    :id, :organization_id, :operation_id, 'provisioning.succeeded',
                    :actor_principal, CAST(:payload AS jsonb)
                )
                """
            ),
            {
                "id": uuid4(),
                "organization_id": created["organization_id"],
                "operation_id": operation_id,
                "actor_principal": actor_principal,
                "payload": json.dumps(
                    {
                        "runtime_submission_id": str(bundle.runtime_submission_id),
                        "ingress_submission_id": str(bundle.ingress_submission_id),
                    },
                    sort_keys=True,
                    separators=(",", ":"),
                ),
            },
        )
    assert isinstance(boundary_time, datetime)
    return boundary_time


@pytest.mark.postgres
def test_billing_schema_foundation_is_exact_and_append_only(
    migrated_database: Engine,
) -> None:
    inspector = inspect(migrated_database)
    tables = set(inspector.get_table_names())

    assert {
        "entitlement_subjects",
        "entitlement_revisions",
        "provisioning_authorizations",
        "usage_facts",
    } <= tables
    expected_columns = {
        "entitlement_subjects": {"id", "subject_type", "subject_key", "created_at"},
        "entitlement_revisions": {
            "id",
            "subject_id",
            "generation",
            "plan_key",
            "state",
            "capability",
            "quota_dimension",
            "quota_limit",
            "observed_at",
            "valid_until",
            "source_revision_id",
            "payload_digest",
            "recorded_at",
        },
        "provisioning_authorizations": {
            "id",
            "usage_fact_id",
            "subject_id",
            "revision_id",
            "organization_id",
            "operation_id",
            "deployment_id",
            "capability",
            "quota_dimension",
            "units",
            "request_fingerprint",
            "decided_at",
        },
        "usage_facts": {
            "id",
            "subject_id",
            "revision_id",
            "authorization_id",
            "organization_id",
            "operation_id",
            "deployment_id",
            "metric",
            "unit",
            "quantity",
            "source_event",
            "occurred_at",
            "recorded_at",
            "schema_version",
            "payload_digest",
        },
    }
    for table_name, column_names in expected_columns.items():
        assert {column["name"] for column in inspector.get_columns(table_name)} == column_names

    authorization_fks = {
        foreign_key["name"]: foreign_key
        for foreign_key in inspector.get_foreign_keys("provisioning_authorizations")
    }
    operation_fk = authorization_fks["fk_provisioning_authorizations_operation_deployment"]
    assert operation_fk["constrained_columns"] == [
        "organization_id",
        "operation_id",
        "deployment_id",
    ]
    assert operation_fk["referred_columns"] == [
        "organization_id",
        "id",
        "deployment_id",
    ]
    assert operation_fk["options"] == {
        "ondelete": "RESTRICT",
        "initially": "DEFERRED",
        "deferrable": True,
    }

    usage_fk = {
        foreign_key["name"]: foreign_key
        for foreign_key in inspector.get_foreign_keys("usage_facts")
    }["fk_usage_facts_authorization_bundle"]
    assert usage_fk["constrained_columns"] == [
        "authorization_id",
        "id",
        "subject_id",
        "revision_id",
        "organization_id",
        "operation_id",
        "deployment_id",
    ]
    assert usage_fk["referred_columns"] == [
        "id",
        "usage_fact_id",
        "subject_id",
        "revision_id",
        "organization_id",
        "operation_id",
        "deployment_id",
    ]

    with migrated_database.connect() as connection:
        trigger_rows = connection.execute(
            text(
                """
                SELECT c.relname, t.tgname
                FROM pg_trigger AS t
                JOIN pg_class AS c ON c.oid = t.tgrelid
                JOIN pg_namespace AS n ON n.oid = c.relnamespace
                WHERE n.nspname = current_schema()
                  AND NOT t.tgisinternal
                  AND c.relname = ANY(:tables)
                """
            ),
            {"tables": list(expected_columns) + ["provisioning_operations"]},
        ).all()
    triggers: dict[str, set[str]] = {}
    for table_name, trigger_name in trigger_rows:
        triggers.setdefault(table_name, set()).add(trigger_name)
    assert triggers["entitlement_subjects"] == {"tr_entitlement_subjects_append_only"}
    assert triggers["entitlement_revisions"] == {"tr_entitlement_revisions_append_only"}
    assert triggers["provisioning_authorizations"] == {
        "tr_provisioning_authorizations_append_only",
        "tr_authorizations_operation_bundle",
    }
    assert triggers["usage_facts"] == {
        "tr_usage_facts_append_only",
        "tr_usage_facts_success_bundle",
    }
    assert {
        "tr_operations_authorization_bundle",
        "tr_operations_authorization_mode_immutable",
    } <= triggers["provisioning_operations"]

    operation_columns = {
        column["name"]: column for column in inspector.get_columns("provisioning_operations")
    }
    assert operation_columns["authorization_mode"]["nullable"] is False
    assert str(operation_columns["authorization_mode"]["type"]) == 'VARCHAR(16) COLLATE "C"'
    assert operation_columns["authorization_mode"]["default"] == "'not_required'::character varying"

    subject_id = uuid4()
    revision_id = uuid4()
    source_revision_id = uuid4()
    with migrated_database.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO entitlement_subjects (id, subject_type, subject_key)
                VALUES (:id, 'organization_slug', 'billing-schema-test')
                """
            ),
            {"id": subject_id},
        )
        connection.execute(
            text(
                """
                INSERT INTO entitlement_revisions (
                    id, subject_id, generation, plan_key, state, capability,
                    quota_dimension, quota_limit, observed_at, valid_until,
                    source_revision_id, payload_digest
                ) VALUES (
                    :id, :subject_id, 1, 'crusher.single.v1', 'active',
                    'crusher.deployment.create', 'crusher.deployment.active', 1,
                    clock_timestamp(), clock_timestamp() + interval '4 minutes',
                    :source_revision_id, :payload_digest
                )
                """
            ),
            {
                "id": revision_id,
                "subject_id": subject_id,
                "source_revision_id": source_revision_id,
                "payload_digest": "a" * 64,
            },
        )

    for statement in (
        "UPDATE entitlement_subjects SET subject_key='rebound' WHERE id=:id",
        "DELETE FROM entitlement_subjects WHERE id=:id",
        "UPDATE entitlement_revisions SET state='inactive' WHERE id=:revision_id",
        "DELETE FROM entitlement_revisions WHERE id=:revision_id",
    ):
        with pytest.raises(IntegrityError) as exc_info, migrated_database.begin() as connection:
            connection.execute(
                text(statement),
                {"id": subject_id, "revision_id": revision_id},
            )
        assert getattr(exc_info.value.orig, "sqlstate", None) == "23000"

    with migrated_database.connect() as connection:
        subject = connection.execute(
            text("SELECT subject_type, subject_key FROM entitlement_subjects WHERE id=:id"),
            {"id": subject_id},
        ).one()
        revision = connection.execute(
            text("SELECT state, payload_digest FROM entitlement_revisions WHERE id=:id"),
            {"id": revision_id},
        ).one()

    assert subject == ("organization_slug", "billing-schema-test")
    assert revision == ("active", "a" * 64)


@pytest.mark.postgres
def test_authorization_requires_exact_operation_subject_agreement(
    migrated_database: Engine,
) -> None:
    created = _create_operation(migrated_database, "subject-agreement")
    wrong_subject_id = uuid4()
    revision_id = uuid4()

    with migrated_database.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO entitlement_subjects (id, subject_type, subject_key)
                VALUES (:id, 'organization_slug', 'different-organization')
                """
            ),
            {"id": wrong_subject_id},
        )
        connection.execute(
            text(
                """
                INSERT INTO entitlement_revisions (
                    id, subject_id, generation, plan_key, state, capability,
                    quota_dimension, quota_limit, observed_at, valid_until,
                    source_revision_id, payload_digest
                ) VALUES (
                    :id, :subject_id, 1, 'crusher.single.v1', 'active',
                    'crusher.deployment.create', 'crusher.deployment.active', 1,
                    clock_timestamp(), clock_timestamp() + interval '4 minutes',
                    :source_revision_id, :payload_digest
                )
                """
            ),
            {
                "id": revision_id,
                "subject_id": wrong_subject_id,
                "source_revision_id": uuid4(),
                "payload_digest": "b" * 64,
            },
        )

    _set_fixture_original_required(migrated_database, created["operation_id"])
    with pytest.raises(IntegrityError), migrated_database.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO provisioning_authorizations (
                    id, usage_fact_id, subject_id, revision_id,
                    organization_id, operation_id, deployment_id, capability,
                    quota_dimension, units, request_fingerprint, decided_at
                ) VALUES (
                    :id, :usage_fact_id, :subject_id, :revision_id,
                    :organization_id, :operation_id, :deployment_id,
                    'crusher.deployment.create', 'crusher.deployment.active', 1,
                    :request_fingerprint, clock_timestamp()
                )
                """
            ),
            {
                "id": uuid4(),
                "usage_fact_id": uuid4(),
                "subject_id": wrong_subject_id,
                "revision_id": revision_id,
                "organization_id": created["organization_id"],
                "operation_id": created["operation_id"],
                "deployment_id": created["deployment_id"],
                "request_fingerprint": "c" * 64,
            },
        )
        connection.execute(text("SET CONSTRAINTS ALL IMMEDIATE"))


@pytest.mark.postgres
def test_not_required_operation_rejects_every_authorization(
    migrated_database: Engine,
) -> None:
    created = _create_operation(migrated_database, "disabled-malformed-auth")
    subject_id = uuid4()
    revision_id = uuid4()
    decided_at = datetime.now(UTC)
    with migrated_database.begin() as connection:
        operation_fingerprint = connection.execute(
            text("SELECT request_fingerprint FROM provisioning_operations WHERE id=:operation_id"),
            {"operation_id": created["operation_id"]},
        ).scalar_one()
        connection.execute(
            text(
                "INSERT INTO entitlement_subjects (id, subject_type, subject_key) "
                "VALUES (:id, 'organization_slug', 'unrelated-subject')"
            ),
            {"id": subject_id},
        )
        connection.execute(
            text(
                """
                INSERT INTO entitlement_revisions (
                    id, subject_id, generation, plan_key, state, capability,
                    quota_dimension, quota_limit, observed_at, valid_until,
                    source_revision_id, payload_digest
                ) VALUES (
                    :id, :subject_id, 1, 'crusher.single.v1', 'inactive',
                    'crusher.deployment.create', 'crusher.deployment.active', 1,
                    :observed_at, :valid_until, :source_revision_id, :payload_digest
                )
                """
            ),
            {
                "id": revision_id,
                "subject_id": subject_id,
                "observed_at": decided_at - timedelta(minutes=1),
                "valid_until": decided_at + timedelta(minutes=4),
                "source_revision_id": uuid4(),
                "payload_digest": "9" * 64,
            },
        )

    with pytest.raises(IntegrityError) as exc_info, migrated_database.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO provisioning_authorizations (
                    id, usage_fact_id, subject_id, revision_id, organization_id,
                    operation_id, deployment_id, capability, quota_dimension,
                    units, request_fingerprint, decided_at
                ) VALUES (
                    :id, :usage_fact_id, :subject_id, :revision_id, :organization_id,
                    :operation_id, :deployment_id, 'crusher.deployment.create',
                    'crusher.deployment.active', 1, :request_fingerprint, :decided_at
                )
                """
            ),
            {
                "id": uuid4(),
                "usage_fact_id": uuid4(),
                "subject_id": subject_id,
                "revision_id": revision_id,
                "organization_id": created["organization_id"],
                "operation_id": created["operation_id"],
                "deployment_id": created["deployment_id"],
                "request_fingerprint": operation_fingerprint,
                "decided_at": decided_at,
            },
        )
        connection.execute(text("SET CONSTRAINTS ALL IMMEDIATE"))
    assert getattr(exc_info.value.orig, "sqlstate", None) == "23000"


@pytest.mark.postgres
@pytest.mark.parametrize(
    ("suffix", "revision_state", "observed_offset", "valid_offset"),
    [
        ("inactive-authority", "inactive", -60, 240),
        ("expired-authority", "active", -300, -1),
        ("future-authority", "active", 1, 240),
    ],
)
def test_authorization_requires_current_active_revision(
    migrated_database: Engine,
    suffix: str,
    revision_state: str,
    observed_offset: int,
    valid_offset: int,
) -> None:
    decided_at = datetime.now(UTC)
    with pytest.raises(IntegrityError) as exc_info:
        _create_required_authorization(
            migrated_database,
            suffix,
            revision_state=revision_state,
            observed_at=decided_at + timedelta(seconds=observed_offset),
            valid_until=decided_at + timedelta(seconds=valid_offset),
            decided_at=decided_at,
        )
    assert getattr(exc_info.value.orig, "sqlstate", None) == "23000"


@pytest.mark.postgres
def test_authorization_requires_operation_request_fingerprint(
    migrated_database: Engine,
) -> None:
    with pytest.raises(IntegrityError) as exc_info:
        _create_required_authorization(
            migrated_database,
            "wrong-fingerprint",
            request_fingerprint="0" * 64,
        )
    assert getattr(exc_info.value.orig, "sqlstate", None) == "23000"


@pytest.mark.postgres
def test_authorization_requires_highest_current_revision(
    migrated_database: Engine,
) -> None:
    with pytest.raises(IntegrityError) as exc_info:
        _create_required_authorization(
            migrated_database,
            "stale-generation",
            newer_revision_state="inactive",
        )
    assert getattr(exc_info.value.orig, "sqlstate", None) == "23000"


@pytest.mark.postgres
def test_authorization_rejects_backdated_decision_after_expiry(
    migrated_database: Engine,
) -> None:
    now = datetime.now(UTC)
    with pytest.raises(IntegrityError) as exc_info:
        _create_required_authorization(
            migrated_database,
            "backdated-expired",
            observed_at=now - timedelta(minutes=5),
            valid_until=now - timedelta(seconds=1),
            decided_at=now - timedelta(minutes=2),
        )
    assert getattr(exc_info.value.orig, "sqlstate", None) == "23000"


@pytest.mark.postgres
def test_authorization_rejects_future_dated_decision(
    migrated_database: Engine,
) -> None:
    now = datetime.now(UTC)
    with pytest.raises(IntegrityError) as exc_info:
        _create_required_authorization(
            migrated_database,
            "future-decision",
            observed_at=now + timedelta(minutes=1),
            valid_until=now + timedelta(minutes=4),
            decided_at=now + timedelta(minutes=2),
        )
    assert getattr(exc_info.value.orig, "sqlstate", None) == "23000"


@pytest.mark.postgres
def test_revision_publication_serializes_before_authorization_decision(
    migrated_database: Engine,
) -> None:
    created = _create_operation(migrated_database, "concurrent-revision")
    _set_fixture_original_required(migrated_database, created["operation_id"])
    subject_id = uuid4()
    old_revision_id = uuid4()
    now = datetime.now(UTC)
    with migrated_database.begin() as connection:
        fingerprint = connection.execute(
            text("SELECT request_fingerprint FROM provisioning_operations WHERE id=:operation_id"),
            {"operation_id": created["operation_id"]},
        ).scalar_one()
        connection.execute(
            text(
                "INSERT INTO entitlement_subjects (id, subject_type, subject_key) "
                "VALUES (:id, 'organization_slug', :subject_key)"
            ),
            {"id": subject_id, "subject_key": "billing-concurrent-revision"},
        )
        connection.execute(
            text(
                """
                INSERT INTO entitlement_revisions (
                    id, subject_id, generation, plan_key, state, capability,
                    quota_dimension, quota_limit, observed_at, valid_until,
                    source_revision_id, payload_digest
                ) VALUES (
                    :id, :subject_id, 1, 'crusher.single.v1', 'active',
                    'crusher.deployment.create', 'crusher.deployment.active', 1,
                    :observed_at, :valid_until, :source_revision_id, :payload_digest
                )
                """
            ),
            {
                "id": old_revision_id,
                "subject_id": subject_id,
                "observed_at": now - timedelta(minutes=1),
                "valid_until": now + timedelta(minutes=4),
                "source_revision_id": uuid4(),
                "payload_digest": "7" * 64,
            },
        )

    trigger_started = Event()

    def attempt_authorization() -> str | None:
        try:
            with migrated_database.begin() as connection:
                connection.execute(text("SET LOCAL lock_timeout = '2s'"))
                connection.execute(
                    text(
                        """
                        INSERT INTO provisioning_authorizations (
                            id, usage_fact_id, subject_id, revision_id,
                            organization_id, operation_id, deployment_id, capability,
                            quota_dimension, units, request_fingerprint, decided_at
                        ) VALUES (
                            :id, :usage_fact_id, :subject_id, :revision_id,
                            :organization_id, :operation_id, :deployment_id,
                            'crusher.deployment.create', 'crusher.deployment.active', 1,
                            :request_fingerprint, :decided_at
                        )
                        """
                    ),
                    {
                        "id": uuid4(),
                        "usage_fact_id": uuid4(),
                        "subject_id": subject_id,
                        "revision_id": old_revision_id,
                        "organization_id": created["organization_id"],
                        "operation_id": created["operation_id"],
                        "deployment_id": created["deployment_id"],
                        "request_fingerprint": fingerprint,
                        "decided_at": now,
                    },
                )
                trigger_started.set()
                connection.execute(text("SET CONSTRAINTS ALL IMMEDIATE"))
            return None
        except DBAPIError as exc:
            return getattr(exc.orig, "sqlstate", None)

    with migrated_database.connect() as publisher:
        transaction = publisher.begin()
        publisher.execute(
            text("SELECT id FROM entitlement_subjects WHERE id=:id FOR UPDATE"),
            {"id": subject_id},
        )
        publisher.execute(
            text(
                """
                INSERT INTO entitlement_revisions (
                    id, subject_id, generation, plan_key, state, capability,
                    quota_dimension, quota_limit, observed_at, valid_until,
                    source_revision_id, payload_digest
                ) VALUES (
                    :id, :subject_id, 2, 'crusher.single.v1', 'inactive',
                    'crusher.deployment.create', 'crusher.deployment.active', 1,
                    :observed_at, :valid_until, :source_revision_id, :payload_digest
                )
                """
            ),
            {
                "id": uuid4(),
                "subject_id": subject_id,
                "observed_at": now,
                "valid_until": now + timedelta(minutes=4),
                "source_revision_id": uuid4(),
                "payload_digest": "8" * 64,
            },
        )
        with ThreadPoolExecutor(max_workers=1) as executor:
            result = executor.submit(attempt_authorization)
            assert trigger_started.wait(timeout=1)
            sleep(0.1)
            assert result.done() is False
            transaction.commit()
            assert result.result(timeout=2) == "23000"


@pytest.mark.postgres
def test_authorization_mode_cannot_be_retrofitted_after_insert(
    migrated_database: Engine,
) -> None:
    with pytest.raises(IntegrityError) as exc_info:
        _create_required_authorization(
            migrated_database,
            "retrofit-mode",
            fixture_original_required=False,
        )
    assert getattr(exc_info.value.orig, "sqlstate", None) == "23000"


@pytest.mark.postgres
def test_required_operation_cannot_commit_without_exact_authorization(
    migrated_database: Engine,
) -> None:
    created = _create_operation(migrated_database, "required-bundle")

    _set_fixture_original_required(migrated_database, created["operation_id"])
    with pytest.raises(IntegrityError) as exc_info, migrated_database.begin() as connection:
        connection.execute(
            text(
                "UPDATE provisioning_operations SET organization_slug=organization_slug "
                "WHERE id=:operation_id"
            ),
            {"operation_id": created["operation_id"]},
        )
        connection.execute(text("SET CONSTRAINTS ALL IMMEDIATE"))

    assert getattr(exc_info.value.orig, "sqlstate", None) == "23000"


@pytest.mark.postgres
def test_authorization_schema_enforces_single_permanent_quota_unit(
    migrated_database: Engine,
) -> None:
    uniques = {
        constraint["name"]: tuple(constraint["column_names"])
        for constraint in inspect(migrated_database).get_unique_constraints(
            "provisioning_authorizations"
        )
    }
    assert uniques["uq_provisioning_authorizations_subject_quota"] == (
        "subject_id",
        "quota_dimension",
    )


@pytest.mark.postgres
def test_usage_fact_schema_supports_stable_export_pagination(
    migrated_database: Engine,
) -> None:
    indexes = {
        index["name"]: (tuple(index["column_names"]), index["unique"])
        for index in inspect(migrated_database).get_indexes("usage_facts")
    }
    assert indexes["ix_usage_facts_recorded_id"] == (
        ("recorded_at", "id"),
        False,
    )


@pytest.mark.postgres
def test_usage_fact_requires_exact_terminal_success_bundle(
    migrated_database: Engine,
) -> None:
    created = _create_required_authorization(migrated_database, "forged-success")

    with pytest.raises(IntegrityError) as exc_info, migrated_database.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO usage_facts (
                    id, subject_id, revision_id, authorization_id,
                    organization_id, operation_id, deployment_id, metric, unit,
                    quantity, source_event, occurred_at, schema_version,
                    payload_digest
                ) VALUES (
                    :id, :subject_id, :revision_id, :authorization_id,
                    :organization_id, :operation_id, :deployment_id,
                    'crusher.deployment.activated', 'deployment', 1,
                    'provisioning.succeeded', clock_timestamp(), 1,
                    :payload_digest
                )
                """
            ),
            {
                "id": created["usage_fact_id"],
                "subject_id": created["subject_id"],
                "revision_id": created["revision_id"],
                "authorization_id": created["authorization_id"],
                "organization_id": created["organization_id"],
                "operation_id": created["operation_id"],
                "deployment_id": created["deployment_id"],
                "payload_digest": "f" * 64,
            },
        )
        connection.execute(text("SET CONSTRAINTS ALL IMMEDIATE"))
    assert getattr(exc_info.value.orig, "sqlstate", None) == "23000"


@pytest.mark.postgres
def test_usage_fact_requires_exact_joint_success_timestamp(
    migrated_database: Engine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    created = _create_required_authorization(migrated_database, "wrong-success-time")
    boundary_time = _commit_terminal_success_bundle(migrated_database, created, monkeypatch)

    with pytest.raises(IntegrityError) as exc_info, migrated_database.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO usage_facts (
                    id, subject_id, revision_id, authorization_id,
                    organization_id, operation_id, deployment_id, metric, unit,
                    quantity, source_event, occurred_at, schema_version,
                    payload_digest
                ) VALUES (
                    :id, :subject_id, :revision_id, :authorization_id,
                    :organization_id, :operation_id, :deployment_id,
                    'crusher.deployment.activated', 'deployment', 1,
                    'provisioning.succeeded', :occurred_at, 1,
                    :payload_digest
                )
                """
            ),
            {
                "id": created["usage_fact_id"],
                "subject_id": created["subject_id"],
                "revision_id": created["revision_id"],
                "authorization_id": created["authorization_id"],
                "organization_id": created["organization_id"],
                "operation_id": created["operation_id"],
                "deployment_id": created["deployment_id"],
                "occurred_at": boundary_time + timedelta(microseconds=1),
                "payload_digest": "f" * 64,
            },
        )
        connection.execute(text("SET CONSTRAINTS ALL IMMEDIATE"))
    assert getattr(exc_info.value.orig, "sqlstate", None) == "23000"


@pytest.mark.postgres
def test_usage_fact_rejects_forged_future_joint_success_timestamp(
    migrated_database: Engine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    created = _create_required_authorization(migrated_database, "future-success-time")
    future_boundary = datetime.now(UTC) + timedelta(minutes=1)
    boundary_time = _commit_terminal_success_bundle(
        migrated_database,
        created,
        monkeypatch,
        boundary_time=future_boundary,
    )

    with pytest.raises(IntegrityError) as exc_info, migrated_database.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO usage_facts (
                    id, subject_id, revision_id, authorization_id,
                    organization_id, operation_id, deployment_id, metric, unit,
                    quantity, source_event, occurred_at, schema_version,
                    payload_digest
                ) VALUES (
                    :id, :subject_id, :revision_id, :authorization_id,
                    :organization_id, :operation_id, :deployment_id,
                    'crusher.deployment.activated', 'deployment', 1,
                    'provisioning.succeeded', :occurred_at, 1,
                    :payload_digest
                )
                """
            ),
            {
                "id": created["usage_fact_id"],
                "subject_id": created["subject_id"],
                "revision_id": created["revision_id"],
                "authorization_id": created["authorization_id"],
                "organization_id": created["organization_id"],
                "operation_id": created["operation_id"],
                "deployment_id": created["deployment_id"],
                "occurred_at": boundary_time,
                "payload_digest": "f" * 64,
            },
        )
        connection.execute(text("SET CONSTRAINTS ALL IMMEDIATE"))
    assert getattr(exc_info.value.orig, "sqlstate", None) == "23000"


@pytest.mark.postgres
def test_usage_fact_requires_required_operation_mode(
    migrated_database: Engine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    created = _create_required_authorization(
        migrated_database,
        "disabled-usage",
        fixture_not_required_authorization=True,
    )
    boundary_time = _commit_terminal_success_bundle(migrated_database, created, monkeypatch)

    with pytest.raises(IntegrityError) as exc_info, migrated_database.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO usage_facts (
                    id, subject_id, revision_id, authorization_id,
                    organization_id, operation_id, deployment_id, metric, unit,
                    quantity, source_event, occurred_at, schema_version,
                    payload_digest
                ) VALUES (
                    :id, :subject_id, :revision_id, :authorization_id,
                    :organization_id, :operation_id, :deployment_id,
                    'crusher.deployment.activated', 'deployment', 1,
                    'provisioning.succeeded', :occurred_at, 1,
                    :payload_digest
                )
                """
            ),
            {
                "id": created["usage_fact_id"],
                "subject_id": created["subject_id"],
                "revision_id": created["revision_id"],
                "authorization_id": created["authorization_id"],
                "organization_id": created["organization_id"],
                "operation_id": created["operation_id"],
                "deployment_id": created["deployment_id"],
                "occurred_at": boundary_time,
                "payload_digest": "f" * 64,
            },
        )
        connection.execute(text("SET CONSTRAINTS ALL IMMEDIATE"))
    assert getattr(exc_info.value.orig, "sqlstate", None) == "23000"


@pytest.mark.postgres
def test_authorization_and_usage_fact_are_append_only_exact_bundle(
    migrated_database: Engine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    created = _create_required_authorization(migrated_database, "append-only-bundle")
    authorization_id = created["authorization_id"]
    usage_fact_id = created["usage_fact_id"]

    operation_id = UUID(str(created["operation_id"]))
    boundary_time = _commit_terminal_success_bundle(migrated_database, created, monkeypatch)

    with migrated_database.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO usage_facts (
                    id, subject_id, revision_id, authorization_id,
                    organization_id, operation_id, deployment_id, metric, unit,
                    quantity, source_event, occurred_at, schema_version,
                    payload_digest
                ) VALUES (
                    :id, :subject_id, :revision_id, :authorization_id,
                    :organization_id, :operation_id, :deployment_id,
                    'crusher.deployment.activated', 'deployment', 1,
                    'provisioning.succeeded', :occurred_at, 1,
                    :payload_digest
                )
                """
            ),
            {
                "id": usage_fact_id,
                "subject_id": created["subject_id"],
                "revision_id": created["revision_id"],
                "authorization_id": authorization_id,
                "organization_id": created["organization_id"],
                "operation_id": operation_id,
                "deployment_id": created["deployment_id"],
                "occurred_at": boundary_time,
                "payload_digest": "f" * 64,
            },
        )

    mutations = (
        (
            "UPDATE provisioning_authorizations SET request_fingerprint=:replacement WHERE id=:id",
            authorization_id,
        ),
        ("DELETE FROM provisioning_authorizations WHERE id=:id", authorization_id),
        (
            "UPDATE usage_facts SET payload_digest=:replacement WHERE id=:id",
            usage_fact_id,
        ),
        ("DELETE FROM usage_facts WHERE id=:id", usage_fact_id),
    )
    for statement, row_id in mutations:
        with pytest.raises(IntegrityError) as exc_info, migrated_database.begin() as connection:
            connection.execute(
                text(statement),
                {"id": row_id, "replacement": "0" * 64},
            )
        assert getattr(exc_info.value.orig, "sqlstate", None) == "23000"

    with migrated_database.connect() as connection:
        frozen = connection.execute(
            text(
                """
                SELECT auth.request_fingerprint, auth.usage_fact_id,
                       usage.payload_digest
                FROM provisioning_authorizations AS auth
                JOIN usage_facts AS usage
                  ON usage.authorization_id = auth.id
                 AND usage.id = auth.usage_fact_id
                WHERE auth.id=:authorization_id
                """
            ),
            {"authorization_id": authorization_id},
        ).one()
    assert frozen == (created["request_fingerprint"], usage_fact_id, "f" * 64)


@pytest.mark.postgres
def test_billing_history_blocks_destructive_downgrade(
    migrated_database: Engine,
) -> None:
    subject_id = uuid4()
    with migrated_database.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO entitlement_subjects (id, subject_type, subject_key)
                VALUES (:id, 'organization_slug', 'downgrade-guard')
                """
            ),
            {"id": subject_id},
        )

    configuration = Config("alembic.ini")
    with pytest.raises(RuntimeError, match="billing history prevents downgrade"):
        command.downgrade(configuration, "20261015_0005")

    with migrated_database.connect() as connection:
        assert (
            connection.execute(text("SELECT version_num FROM alembic_version")).scalar_one()
            == "20261028_0007"
        )
        assert (
            connection.execute(
                text("SELECT subject_key FROM entitlement_subjects WHERE id=:id"),
                {"id": subject_id},
            ).scalar_one()
            == "downgrade-guard"
        )


def test_billing_migration_declares_complete_fail_fast_downgrade_lock_set() -> None:
    namespace = runpy.run_path("migrations/versions/20261017_0006_billing_plane_contracts.py")
    assert namespace["DOWNGRADE_LOCK_SQL"] == (
        "LOCK TABLE provisioning_operations, entitlement_subjects, "
        "entitlement_revisions, provisioning_authorizations, usage_facts "
        "IN ACCESS EXCLUSIVE MODE NOWAIT"
    )


@pytest.mark.postgres
def test_billing_downgrade_fails_fast_on_live_operation_writer(
    migrated_database: Engine,
) -> None:
    configuration = Config("alembic.ini")
    with migrated_database.connect() as blocker:
        transaction = blocker.begin()
        blocker.execute(text("LOCK TABLE provisioning_operations IN ROW EXCLUSIVE MODE"))
        with pytest.raises(DBAPIError) as exc_info:
            command.downgrade(configuration, "20261015_0005")
        assert getattr(exc_info.value.orig, "sqlstate", None) == "55P03"
        transaction.rollback()

    with migrated_database.connect() as connection:
        assert (
            connection.execute(text("SELECT version_num FROM alembic_version")).scalar_one()
            == "20261028_0007"
        )
        assert "usage_facts" in inspect(migrated_database).get_table_names()


@pytest.mark.postgres
def test_billing_schema_incremental_and_pristine_roundtrip(
    migrated_database: Engine,
) -> None:
    configuration = Config("alembic.ini")
    command.downgrade(configuration, "base")
    command.upgrade(configuration, "20261015_0005")

    prior_inspector = inspect(migrated_database)
    assert "entitlement_subjects" not in prior_inspector.get_table_names()
    assert "authorization_mode" not in {
        column["name"] for column in prior_inspector.get_columns("provisioning_operations")
    }

    command.upgrade(configuration, "head")
    head_inspector = inspect(migrated_database)
    assert {
        "entitlement_subjects",
        "entitlement_revisions",
        "provisioning_authorizations",
        "usage_facts",
    } <= set(head_inspector.get_table_names())

    command.downgrade(configuration, "20261015_0005")
    downgraded_inspector = inspect(migrated_database)
    assert "usage_facts" not in downgraded_inspector.get_table_names()
    assert "authorization_mode" not in {
        column["name"] for column in downgraded_inspector.get_columns("provisioning_operations")
    }

    command.upgrade(configuration, "head")
