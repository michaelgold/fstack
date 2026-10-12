"""PostgreSQL-backed runtime submission preparation for Temporal activities."""

from __future__ import annotations

import hashlib
import json
import os
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal
from uuid import UUID, uuid4

from sqlalchemy import Connection, create_engine, text

PROVIDER: Literal["local_docker"] = "local_docker"
GENERATION = 1
SUBMISSION_VERSION = "fstack.local-crusher-submission.v1"
SPEC_VERSION = "fstack.local-crusher-spec.v1"


@dataclass(frozen=True)
class PreparedRuntimeSubmission:
    submission_id: UUID
    operation_id: UUID
    state: Literal["prepared", "submitted", "confirmed"]
    provider: Literal["local_docker"]
    generation: int
    submission_key: str
    spec_fingerprint: str
    api_container_name: str
    migration_container_name: str


class OperationNotClaimableError(RuntimeError):
    """The exact operation cannot produce or replay a runtime submission."""


MIGRATION_LAUNCHER_V1 = """import json, os, subprocess, sys
with open('/run/secrets/fstack/owner.json', encoding='utf-8') as stream:
    bundle = json.load(stream)
environment = os.environ.copy()
database_prefix = 'CRUSHER_DATABASE_'
environment[database_prefix + 'OWNER_URL'] = bundle['owner_url']
environment[database_prefix + 'URL'] = bundle['owner_url']
environment[database_prefix + 'APPLICATION_USER'] = bundle['application_user']
environment[database_prefix + 'APPLICATION_' + 'PASSWORD'] = bundle['application_password']
for command in (
    ['/app/.venv/bin/python', '-m', 'crusher.db.application_role'],
    ['/app/.venv/bin/python', '-m', 'crusher.migrations.core.deploy'],
    ['/app/.venv/bin/python', '-m', 'crusher.db.application_role'],
):
    result = subprocess.run(command, env=environment, check=False)
    if result.returncode != 0:
        sys.exit(result.returncode)
"""
API_LAUNCHER_V1 = """import json, os
with open('/run/secrets/fstack/application.json', encoding='utf-8') as stream:
    bundle = json.load(stream)
crusher_prefix = 'CRUSHER_'
os.environ[crusher_prefix + 'DATABASE_URL'] = bundle['application_url']
os.environ[crusher_prefix + 'MAGIC_LINK_' + 'SECRET'] = bundle['magic_link_secret']
os.environ[crusher_prefix + 'PUBLIC_BASE_URL'] = bundle['public_base_url']
os.environ[crusher_prefix + 'DASHBOARD_BASE_URL'] = bundle['dashboard_base_url']
os.environ[crusher_prefix + 'DASHBOARD_MAGIC_LINK_DELIVERY'] = 'inline'
os.environ[crusher_prefix + 'MTA_ROUTE_CACHE_BACKEND'] = 'memory'
os.execv('/app/.venv/bin/uvicorn', [
    '/app/.venv/bin/uvicorn', 'crusher.api:create_app', '--factory',
    '--host', '0.0.0.0', '--port', '8000',
])
"""
DATABASE_PROBE_V1 = """import json, psycopg, sys
with open('/run/secrets/fstack/application.json', encoding='utf-8') as stream:
    bundle = json.load(stream)
with psycopg.connect(bundle['application_url'], connect_timeout=5) as connection:
    with connection.cursor() as cursor:
        cursor.execute('SELECT 1')
        if cursor.fetchone() != (1,):
            sys.exit(1)
"""
HTTP_PROBE_V1 = """import json, sys, urllib.request
with urllib.request.urlopen('http://127.0.0.1:8000/health', timeout=5) as response:
    if response.status != 200:
        sys.exit(1)
    body = response.read(1025)
if len(body) > 1024:
    sys.exit(1)
payload = json.loads(body.decode('utf-8'))
if type(payload) is not dict or payload != {'status': 'ok', 'service': 'crusher'}:
    sys.exit(1)
"""
SOURCE_HASHES = {
    "api_launcher_v1": "33bb430f5625620c4c7c5b1ffefbe11feb2b97650590b61ffcc4b9e3e82186ca",
    "database_probe_v1": "18fd9cf708c5ad62179182abd3bf93f8e3fca2ac2eb60382cf984bc632f5493e",
    "http_probe_v1": "a6045ff06180fe16a7ca13efcf1744d377c61a9f66e00d789feecbd6bb7f4d1f",
    "migration_launcher_v1": "5cc3ebc8fdbff87500470236433ef65ca72607f9edc821e29fc8b2c5fcf1c592",
}


def _database_url() -> str:
    value = os.environ.get("FSTACK_DATABASE_URL")
    if value is None:
        raise RuntimeError("FSTACK_DATABASE_URL is required")
    return value


def _docker_network() -> str:
    value = os.environ.get("FSTACK_DOCKER_NETWORK")
    if not value:
        raise RuntimeError("FSTACK_DOCKER_NETWORK is required")
    return value


def _secret_stage_root() -> Path:
    value = os.environ.get("FSTACK_SECRET_STAGE_ROOT")
    if not value:
        raise RuntimeError("FSTACK_SECRET_STAGE_ROOT is required")
    return Path(value)


def _submission_key(operation_id: UUID) -> str:
    return hashlib.sha256(
        SUBMISSION_VERSION.encode("ascii") + b"\0" + str(operation_id).encode("ascii")
    ).hexdigest()


def _container_names(deployment_id: UUID) -> tuple[str, str]:
    return (
        f"fstack-crusher-{deployment_id.hex}",
        f"fstack-crusher-migrate-{deployment_id.hex}",
    )


def _canonical_spec(
    operation: Mapping[str, Any],
    *,
    api_container_name: str,
    migration_container_name: str,
    docker_network: str,
    secret_stage_root: Path,
) -> dict[str, Any]:
    operation_id = operation["operation_id"]
    deployment_id = operation["deployment_id"]
    stage_directory = secret_stage_root / deployment_id.hex
    common = {
        "cap_add": [],
        "cap_drop": ["ALL"],
        "devices": [],
        "device_requests": [],
        "entrypoint": [],
        "environment": "inherit-from-requested-digest",
        "environment_overrides": [],
        "extra_hosts": [],
        "image": operation["crusher_image"],
        "init": None,
        "ipc_mode": None,
        "label_policy": "inherit-from-requested-digest-plus-exact-fstack-labels",
        "links": [],
        "logging": {"driver": "json-file", "options": {}},
        "network": {"aliases": "deterministic-container-name-only", "name": docker_network},
        "pid_mode": None,
        "platform": "native-linux",
        "ports": {},
        "privileged": False,
        "readonly_rootfs": False,
        "security_opt": ["no-new-privileges:true"],
        "stop_signal": None,
        "tmpfs": {},
        "user": "0:0",
        "uts_mode": None,
        "working_dir": "/app",
    }
    return {
        "api_container_name": api_container_name,
        "crusher_image": operation["crusher_image"],
        "database_secret_ref": operation["database_secret_ref"],
        "deployment_id": str(deployment_id),
        "docker_network": docker_network,
        "migration_container_name": migration_container_name,
        "object_storage_secret_ref": operation["object_storage_secret_ref"],
        "operation_id": str(operation_id),
        "public_domain": operation["public_domain"],
        "source_hashes": SOURCE_HASHES,
        "version": SPEC_VERSION,
        "docker": {
            "common": common,
            "labels": {
                "io.fstack.managed": "true",
                "io.fstack.operation_id": str(operation_id),
                "io.fstack.organization_id": str(operation["organization_id"]),
                "io.fstack.deployment_id": str(deployment_id),
                "io.fstack.submission_key": _submission_key(operation_id),
                "io.fstack.spec_fingerprint": "self",
            },
            "migration": {
                "command": ["/app/.venv/bin/python", "-c", MIGRATION_LAUNCHER_V1],
                "healthcheck": ["NONE"],
                "mounts": [
                    {
                        "read_only": True,
                        "source": str(stage_directory / "owner.json"),
                        "target": "/run/secrets/fstack/owner.json",
                        "type": "bind",
                    }
                ],
                "name": migration_container_name,
                "restart_policy": {"maximum_retry_count": 0, "name": "no"},
            },
            "api": {
                "command": ["/app/.venv/bin/python", "-c", API_LAUNCHER_V1],
                "healthcheck": {
                    "command": ["CMD", "/app/.venv/bin/python", "-c", HTTP_PROBE_V1],
                    "interval_seconds": 30,
                    "retries": 3,
                    "start_period_seconds": 10,
                    "timeout_seconds": 5,
                },
                "mounts": [
                    {
                        "read_only": True,
                        "source": str(stage_directory / "application.json"),
                        "target": "/run/secrets/fstack/application.json",
                        "type": "bind",
                    }
                ],
                "name": api_container_name,
                "restart_policy": {"maximum_retry_count": 0, "name": "unless-stopped"},
            },
        },
    }


def _spec_fingerprint(spec: Mapping[str, Any]) -> str:
    serialized = json.dumps(spec, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode(
        "utf-8"
    )
    return hashlib.sha256(serialized).hexdigest()


def _result(row: Mapping[str, Any]) -> PreparedRuntimeSubmission:
    return PreparedRuntimeSubmission(
        submission_id=row["id"],
        operation_id=row["operation_id"],
        state=row["state"],
        provider=row["provider"],
        generation=row["generation"],
        submission_key=row["submission_key"],
        spec_fingerprint=row["spec_fingerprint"],
        api_container_name=row["api_container_name"],
        migration_container_name=row["migration_container_name"],
    )


def _prepare(connection: Connection, operation_id: UUID) -> PreparedRuntimeSubmission:
    operation = (
        connection.execute(
            text(
                """
                SELECT operation.id AS operation_id, operation.organization_id,
                       operation.deployment_id, operation.state, operation.actor_principal,
                       deployment.crusher_image, deployment.public_domain,
                       database_binding.secret_ref AS database_secret_ref,
                       storage_binding.secret_ref AS object_storage_secret_ref
                FROM provisioning_operations AS operation
                JOIN deployments AS deployment
                  ON deployment.organization_id = operation.organization_id
                 AND deployment.id = operation.deployment_id
                JOIN database_bindings AS database_binding
                  ON database_binding.organization_id = operation.organization_id
                 AND database_binding.deployment_id = operation.deployment_id
                JOIN storage_bindings AS storage_binding
                  ON storage_binding.organization_id = operation.organization_id
                 AND storage_binding.deployment_id = operation.deployment_id
                WHERE operation.id = :operation_id
                FOR UPDATE OF operation
                FOR SHARE OF deployment, database_binding, storage_binding
                """
            ),
            {"operation_id": operation_id},
        )
        .mappings()
        .one_or_none()
    )
    if operation is None:
        raise OperationNotClaimableError("operation does not exist")
    if operation["state"] == "failed":
        raise OperationNotClaimableError("operation is terminally failed")

    api_name, migration_name = _container_names(operation["deployment_id"])
    key = _submission_key(operation_id)
    fingerprint = _spec_fingerprint(
        _canonical_spec(
            dict(operation),
            api_container_name=api_name,
            migration_container_name=migration_name,
            docker_network=_docker_network(),
            secret_stage_root=_secret_stage_root(),
        )
    )
    existing = (
        connection.execute(
            text("SELECT * FROM runtime_submissions WHERE operation_id = :operation_id"),
            {"operation_id": operation_id},
        )
        .mappings()
        .one_or_none()
    )
    if existing is not None:
        if existing["state"] != "confirmed" and (
            existing["provider"] != PROVIDER
            or existing["generation"] != GENERATION
            or existing["submission_key"] != key
            or existing["spec_fingerprint"] != fingerprint
            or existing["api_container_name"] != api_name
            or existing["migration_container_name"] != migration_name
        ):
            raise OperationNotClaimableError(
                "sealed runtime submission does not match desired execution spec"
            )
        return _result(dict(existing))
    if operation["state"] != "pending":
        raise OperationNotClaimableError("operation has no replayable runtime submission")

    submission_id = uuid4()
    connection.execute(
        text(
            """
            INSERT INTO runtime_submissions (
                id, organization_id, operation_id, deployment_id, provider, generation,
                submission_key, spec_fingerprint, api_container_name,
                migration_container_name, state
            ) VALUES (
                :id, :organization_id, :operation_id, :deployment_id, :provider,
                :generation, :submission_key, :spec_fingerprint, :api_container_name,
                :migration_container_name, 'prepared'
            )
            """
        ),
        {
            "id": submission_id,
            "organization_id": operation["organization_id"],
            "operation_id": operation_id,
            "deployment_id": operation["deployment_id"],
            "provider": PROVIDER,
            "generation": GENERATION,
            "submission_key": key,
            "spec_fingerprint": fingerprint,
            "api_container_name": api_name,
            "migration_container_name": migration_name,
        },
    )
    connection.execute(
        text(
            """
            WITH boundary AS (SELECT clock_timestamp() AS now)
            UPDATE provisioning_operations
            SET state = 'running', started_at = boundary.now, updated_at = boundary.now
            FROM boundary
            WHERE id = :operation_id AND state = 'pending'
            """
        ),
        {"operation_id": operation_id},
    )
    connection.execute(
        text(
            """
            INSERT INTO audit_events (
                id, organization_id, operation_id, event_type, actor_principal, payload
            ) VALUES (
                :id, :organization_id, :operation_id, 'provisioning.started',
                :actor_principal, CAST(:payload AS jsonb)
            )
            """
        ),
        {
            "id": uuid4(),
            "organization_id": operation["organization_id"],
            "operation_id": operation_id,
            "actor_principal": operation["actor_principal"],
            "payload": json.dumps(
                {
                    "runtime_submission_id": str(submission_id),
                    "provider": PROVIDER,
                },
                sort_keys=True,
                separators=(",", ":"),
            ),
        },
    )
    return PreparedRuntimeSubmission(
        submission_id=submission_id,
        operation_id=operation_id,
        state="prepared",
        provider=PROVIDER,
        generation=GENERATION,
        submission_key=key,
        spec_fingerprint=fingerprint,
        api_container_name=api_name,
        migration_container_name=migration_name,
    )


def prepare_operation(operation_id: UUID | str) -> PreparedRuntimeSubmission:
    """Prepare or replay one exact runtime submission transactionally."""
    operation_uuid = operation_id if isinstance(operation_id, UUID) else UUID(operation_id)
    engine = create_engine(_database_url())
    try:
        with engine.begin() as connection:
            return _prepare(connection, operation_uuid)
    finally:
        engine.dispose()
