from __future__ import annotations

import hashlib
import json
from pathlib import Path
from uuid import UUID

import fstack.runtime_submission as runtime_submission


def test_frozen_launcher_and_probe_sources_have_contract_hashes() -> None:
    expected = {
        "MIGRATION_LAUNCHER_V1": "5cc3ebc8fdbff87500470236433ef65ca72607f9edc821e29fc8b2c5fcf1c592",
        "API_LAUNCHER_V1": "33bb430f5625620c4c7c5b1ffefbe11feb2b97650590b61ffcc4b9e3e82186ca",
        "DATABASE_PROBE_V1": "18fd9cf708c5ad62179182abd3bf93f8e3fca2ac2eb60382cf984bc632f5493e",
        "HTTP_PROBE_V1": "a6045ff06180fe16a7ca13efcf1744d377c61a9f66e00d789feecbd6bb7f4d1f",
    }

    actual = {
        name: hashlib.sha256(getattr(runtime_submission, name).encode("utf-8")).hexdigest()
        for name in expected
    }

    assert actual == expected


def test_canonical_spec_seals_complete_docker_policy() -> None:
    operation_id = UUID("11111111-1111-4111-8111-111111111111")
    deployment_id = UUID("22222222-2222-4222-8222-222222222222")
    operation = {
        "operation_id": operation_id,
        "organization_id": UUID("33333333-3333-4333-8333-333333333333"),
        "deployment_id": deployment_id,
        "crusher_image": "ghcr.io/michaelgold/crusher@sha256:" + "a" * 64,
        "public_domain": "mail.acme.example",
        "database_secret_ref": "secret://local-file/acme/database",
        "object_storage_secret_ref": "secret://vault/acme/object-storage",
    }
    api_name, migration_name = runtime_submission._container_names(deployment_id)

    spec = runtime_submission._canonical_spec(
        operation,
        api_container_name=api_name,
        migration_container_name=migration_name,
        docker_network="fstack-local",
        secret_stage_root=Path("/var/lib/fstack/secrets"),
    )

    assert spec["version"] == "fstack.local-crusher-spec.v1"
    assert spec["source_hashes"] == {
        "api_launcher_v1": "33bb430f5625620c4c7c5b1ffefbe11feb2b97650590b61ffcc4b9e3e82186ca",
        "database_probe_v1": "18fd9cf708c5ad62179182abd3bf93f8e3fca2ac2eb60382cf984bc632f5493e",
        "http_probe_v1": "a6045ff06180fe16a7ca13efcf1744d377c61a9f66e00d789feecbd6bb7f4d1f",
        "migration_launcher_v1": "5cc3ebc8fdbff87500470236433ef65ca72607f9edc821e29fc8b2c5fcf1c592",
    }
    common = spec["docker"]["common"]
    assert common == {
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
        "network": {"aliases": "deterministic-container-name-only", "name": "fstack-local"},
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
    migration = spec["docker"]["migration"]
    assert migration["command"] == [
        "/app/.venv/bin/python",
        "-c",
        runtime_submission.MIGRATION_LAUNCHER_V1,
    ]
    assert migration["healthcheck"] == ["NONE"]
    assert migration["restart_policy"] == {"name": "no", "maximum_retry_count": 0}
    assert migration["mounts"] == [
        {
            "read_only": True,
            "source": "/var/lib/fstack/secrets/22222222222242228222222222222222/owner.json",
            "target": "/run/secrets/fstack/owner.json",
            "type": "bind",
        }
    ]
    api = spec["docker"]["api"]
    assert api["command"] == [
        "/app/.venv/bin/python",
        "-c",
        runtime_submission.API_LAUNCHER_V1,
    ]
    assert api["restart_policy"] == {"name": "unless-stopped", "maximum_retry_count": 0}
    assert api["healthcheck"] == {
        "command": [
            "CMD",
            "/app/.venv/bin/python",
            "-c",
            runtime_submission.HTTP_PROBE_V1,
        ],
        "interval_seconds": 30,
        "retries": 3,
        "start_period_seconds": 10,
        "timeout_seconds": 5,
    }
    assert api["mounts"] == [
        {
            "read_only": True,
            "source": "/var/lib/fstack/secrets/22222222222242228222222222222222/application.json",
            "target": "/run/secrets/fstack/application.json",
            "type": "bind",
        }
    ]
    assert spec["docker"]["labels"] == {
        "io.fstack.managed": "true",
        "io.fstack.operation_id": str(operation_id),
        "io.fstack.organization_id": "33333333-3333-4333-8333-333333333333",
        "io.fstack.deployment_id": str(deployment_id),
        "io.fstack.submission_key": runtime_submission._submission_key(operation_id),
        "io.fstack.spec_fingerprint": "self",
    }

    serialized = json.dumps(spec, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()
    assert runtime_submission._spec_fingerprint(spec) == hashlib.sha256(serialized).hexdigest()
