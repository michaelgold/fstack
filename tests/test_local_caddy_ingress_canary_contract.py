from __future__ import annotations

import hashlib
import io
import json
import subprocess
import sys
import tarfile
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from tools.run_local_caddy_ingress_canary import (  # noqa: E402
    CanaryContractError,
    CanaryInputs,
    CleanupRegistry,
    CommandSpec,
    DockerResourceRunner,
    RecordingRunner,
    ResourceSpec,
    RuntimeSecrets,
    _adopt_caddy_route_command,
    _api_post_command,
    _caddy_routes_command,
    _canary_processes_absent,
    _capture_network_crusher_containers,
    _conflict_fixture,
    _dispatch_generation_command,
    _durable_counts_command,
    _finalize_success_evidence,
    _merge_captured_resources,
    _quiesce_canary_producers,
    _recover_expected_operations,
    _select_exact_managed_route,
    _source_tree_digest,
    _submission_ids_command,
    _task_queue_has_pollers,
    _topology_mismatch_command,
    _verify_executing_harness,
    assert_no_resource_collisions,
    build_live_verification_commands,
    build_maintenance_steps,
    build_plan,
    build_resource_specs,
    build_runtime_commands,
    create_evidence_directory,
    execute_resource,
    main,
    parse_args,
    prepare_runtime_files,
    prepare_source,
    validate_source,
    verify_images,
    write_evidence,
)


def _digest(image: str, marker: str) -> str:
    return f"{image}@sha256:{marker * 64}"


class FakeExecutor:
    def __init__(
        self,
        outputs: dict[tuple[str, ...], str],
        existing: set[tuple[str, ...]] | None = None,
    ) -> None:
        self.outputs = outputs
        self.existing = existing or set()
        self.calls: list[tuple[str, ...]] = []

    def run(self, argv: tuple[str, ...], *, timeout: int = 60) -> str:
        self.calls.append(argv)
        if argv not in self.outputs:
            raise CanaryContractError("unexpected command")
        return self.outputs[argv]

    def exists(self, argv: tuple[str, ...], *, timeout: int = 30) -> bool:
        self.calls.append(argv)
        return argv in self.existing


class SequenceExecutor:
    def __init__(self, outputs: dict[tuple[str, ...], list[str | None]]) -> None:
        self.outputs = {command: list(values) for command, values in outputs.items()}
        self.calls: list[tuple[str, ...]] = []

    def run(self, argv: tuple[str, ...], *, timeout: int = 60) -> str:
        self.calls.append(argv)
        values = self.outputs.get(argv)
        if not values:
            raise CanaryContractError("unexpected command")
        value = values.pop(0)
        if value is None:
            raise CanaryContractError("scripted command failure")
        return value

    def exists(self, argv: tuple[str, ...], *, timeout: int = 30) -> bool:
        self.calls.append(argv)
        values = self.outputs.get(argv)
        if not values:
            return False
        return values.pop(0) == "exists"


def test_requires_exact_immutable_images_and_caddy_v2117(tmp_path: Path) -> None:
    evidence = tmp_path / "evidence"
    inputs = CanaryInputs(
        source_commit="a" * 40,
        source_archive=None,
        source_archive_sha256=None,
        caddy_image=_digest("caddy:2.11.7", "1"),
        postgres_image=_digest("postgres:17-alpine", "2"),
        temporal_image=_digest("temporalio/auto-setup:1.25.2", "3"),
        crusher_image=_digest("ghcr.io/michaelgold/crusher:canary", "4"),
        evidence_dir=evidence,
        nonce="abc123",
    )

    assert inputs.caddy_image.startswith("caddy:2.11.7@sha256:")

    for field, value in {
        "caddy_image": "caddy:2.11.7",
        "postgres_image": "postgres:17-alpine",
        "temporal_image": "temporalio/auto-setup:1.25.2",
        "crusher_image": "ghcr.io/michaelgold/crusher:canary",
    }.items():
        with pytest.raises(CanaryContractError):
            CanaryInputs(**{**inputs.__dict__, field: value})

    with pytest.raises(CanaryContractError):
        CanaryInputs(**{**inputs.__dict__, "caddy_image": _digest("caddy:2.11.6", "1")})


def test_image_verification_pulls_and_inspects_exact_digests_and_caddy_version(
    tmp_path: Path,
) -> None:
    inputs = CanaryInputs(
        source_commit="a" * 40,
        source_archive=None,
        source_archive_sha256=None,
        caddy_image=_digest("caddy:2.11.7", "1"),
        postgres_image=_digest("postgres:17-alpine", "2"),
        temporal_image=_digest("temporalio/auto-setup:1.25.2", "3"),
        crusher_image=_digest("ghcr.io/michaelgold/crusher:canary", "4"),
        evidence_dir=tmp_path / "evidence",
        nonce="abc123",
    )
    outputs: dict[tuple[str, ...], str] = {}
    for image in (
        inputs.caddy_image,
        inputs.postgres_image,
        inputs.temporal_image,
        inputs.crusher_image,
    ):
        outputs[("docker", "pull", image)] = "pulled"
        outputs[("docker", "image", "inspect", "--format", "{{json .RepoDigests}}", image)] = (
            '["example.invalid/image@sha256:' + image.rsplit(":", 1)[-1] + '"]'
        )
    outputs[("docker", "run", "--rm", inputs.caddy_image, "caddy", "version")] = "v2.11.7"
    executor = FakeExecutor(outputs)

    verified = verify_images(inputs, executor)

    assert verified["caddy"] == "sha256:" + "1" * 64
    assert executor.calls[-1] == (
        "docker",
        "run",
        "--rm",
        inputs.caddy_image,
        "caddy",
        "version",
    )


def test_executing_harness_must_match_immutable_prepared_source(tmp_path: Path) -> None:
    executing = tmp_path / "current.py"
    prepared_root = tmp_path / "prepared"
    prepared_harness = prepared_root / "tools" / "run_local_caddy_ingress_canary.py"
    prepared_harness.parent.mkdir(parents=True)
    executing.write_bytes(b"reviewed harness\n")
    prepared_harness.write_bytes(executing.read_bytes())

    _verify_executing_harness(executing, prepared_root)
    baseline = _source_tree_digest(prepared_root)
    prepared_harness.write_bytes(b"changed prepared source\n")
    assert _source_tree_digest(prepared_root) != baseline
    prepared_harness.write_bytes(b"reviewed harness\n")
    executing.write_bytes(b"modified harness\n")
    with pytest.raises(CanaryContractError):
        _verify_executing_harness(executing, prepared_root)


def test_source_commit_requires_exact_clean_head_or_verified_archive(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    evidence = tmp_path / "evidence"
    commit = "a" * 40

    assert (
        validate_source(
            repo,
            source_commit=commit,
            source_archive=None,
            source_archive_sha256=None,
            git_head=commit,
            git_status="",
            evidence_dir=evidence,
        )
        == repo
    )

    with pytest.raises(CanaryContractError):
        validate_source(
            repo,
            source_commit=commit,
            source_archive=None,
            source_archive_sha256=None,
            git_head=commit,
            git_status=" M README.md",
            evidence_dir=evidence,
        )

    archive = tmp_path / "source.tar"
    archive.write_bytes(b"immutable source")
    archive_sha = hashlib.sha256(archive.read_bytes()).hexdigest()
    assert (
        validate_source(
            repo,
            source_commit=None,
            source_archive=archive,
            source_archive_sha256=archive_sha,
            git_head=None,
            git_status=None,
            evidence_dir=evidence,
        )
        == archive
    )

    with pytest.raises(CanaryContractError):
        validate_source(
            repo,
            source_commit=None,
            source_archive=archive,
            source_archive_sha256="0" * 64,
            git_head=None,
            git_status=None,
            evidence_dir=evidence,
        )


def test_plan_is_fully_isolated_and_registers_cleanup_before_create(tmp_path: Path) -> None:
    inputs = CanaryInputs(
        source_commit="a" * 40,
        source_archive=None,
        source_archive_sha256=None,
        caddy_image=_digest("caddy:2.11.7", "1"),
        postgres_image=_digest("postgres:17-alpine", "2"),
        temporal_image=_digest("temporalio/auto-setup:1.25.2", "3"),
        crusher_image=_digest("ghcr.io/michaelgold/crusher:canary", "4"),
        evidence_dir=tmp_path / "evidence",
        nonce="abc123",
    )
    plan = build_plan(inputs)

    assert plan.prefix == "fstack-caddy-canary-abc123"
    assert plan.database_name.endswith("_test")
    assert all(name.startswith(plan.prefix) for name in plan.resource_names)
    assert plan.caddy_ip == "172.30.0.10"
    assert plan.listener_address == "172.30.0.10:8080"
    assert plan.worker_environment["FSTACK_CADDY_CONTAINER_NAME"] == plan.caddy_container
    assert plan.worker_environment["FSTACK_CADDY_PROBE_ADDRESS"] == plan.listener_address
    assert plan.worker_environment["FSTACK_DOCKER_NETWORK"] == plan.network
    assert plan.worker_supplemental_gid == plan.caddy_socket_gid
    assert plan.caddy_socket_entry == "caddy-admin.sock"
    assert plan.caddy_socket_mode == 0o220
    assert plan.control_volume not in plan.network

    runner = RecordingRunner()
    cleanup = CleanupRegistry(runner)
    secrets_bundle = RuntimeSecrets("pg", "operator", "application", "magic")
    specs = build_resource_specs(plan, tmp_path / "source", tmp_path / "runtime", secrets_bundle)
    for spec in specs:
        execute_resource(spec, runner, cleanup)
    for spec in specs:
        assert runner.events.index(("register", spec.name)) < runner.events.index(
            ("create", spec.name)
        )


def test_cleanup_is_verified_after_success_and_failure(tmp_path: Path) -> None:
    runner = RecordingRunner()
    cleanup = CleanupRegistry(runner)
    cleanup.register("fstack-caddy-canary-abc123-network", ("docker", "network", "rm"))
    cleanup.mark_created("fstack-caddy-canary-abc123-network", "network-id")
    cleanup.register("fstack-caddy-canary-abc123-volume", ("docker", "volume", "rm"))
    cleanup.mark_created("fstack-caddy-canary-abc123-volume", "volume-id")

    cleanup.run_and_verify()

    assert runner.events[-2:] == [
        ("verify-absent", "fstack-caddy-canary-abc123-volume"),
        ("verify-absent", "fstack-caddy-canary-abc123-network"),
    ]


def test_failed_create_never_cleans_up_a_preexisting_exact_name() -> None:
    runner = RecordingRunner(fail_create={"fstack-caddy-canary-abc123-network"})
    cleanup = CleanupRegistry(runner)
    cleanup.register("fstack-caddy-canary-abc123-network", ("docker", "network", "rm"))

    with pytest.raises(CanaryContractError):
        runner.create(
            "fstack-caddy-canary-abc123-network",
            ("docker", "network", "create", "fstack-caddy-canary-abc123-network"),
        )
    cleanup.run_and_verify()

    assert ("remove", "fstack-caddy-canary-abc123-network") not in runner.events


def test_execute_resource_registers_cleanup_before_creation_and_records_identity() -> None:
    runner = RecordingRunner()
    cleanup = CleanupRegistry(runner)
    spec = ResourceSpec(
        "fstack-caddy-canary-abc123-network",
        ("docker", "network", "create", "fstack-caddy-canary-abc123-network"),
        ("docker", "network", "rm"),
    )

    execute_resource(spec, runner, cleanup)

    assert runner.events[:2] == [
        ("register", spec.name),
        ("create", spec.name),
    ]
    assert cleanup.created[spec.name] == f"created:{spec.name}"


def test_nonzero_create_that_materialized_owned_resource_is_cleaned() -> None:
    name = "fstack-caddy-canary-abc123-worker"
    create = ("docker", "run", "-d", "--name", name, "image")
    inspect = ("docker", "container", "inspect", "--format", "{{.Id}}", name)
    owner = (
        "docker",
        "container",
        "inspect",
        "--format",
        (
            '{{index .Config.Labels "io.fstack.canary"}}|'
            '{{index .Config.Labels "io.fstack.canary.invocation"}}'
        ),
        name,
    )
    remove = ("docker", "container", "rm", "--force", name)
    executor = SequenceExecutor(
        {
            create: [None],
            inspect: ["c" * 64, "c" * 64],
            owner: [
                "fstack-caddy-canary-abc123|test-invocation",
                "fstack-caddy-canary-abc123|test-invocation",
            ],
            remove: ["removed"],
            ("docker", "container", "inspect", name): [None],
        }
    )
    runner = DockerResourceRunner(executor)
    cleanup = CleanupRegistry(runner)
    spec = ResourceSpec(name, create, ("docker", "container", "rm", "--force"))

    with pytest.raises(CanaryContractError):
        execute_resource(spec, runner, cleanup)
    cleanup.run_and_verify()

    assert executor.calls.count(remove) == 1


def test_nonzero_create_never_adopts_resource_with_wrong_invocation_token() -> None:
    name = "fstack-caddy-canary-abc123-worker"
    create = ("docker", "run", "-d", "--name", name, "image")
    inspect = ("docker", "container", "inspect", "--format", "{{.Id}}", name)
    owner = (
        "docker",
        "container",
        "inspect",
        "--format",
        (
            '{{index .Config.Labels "io.fstack.canary"}}|'
            '{{index .Config.Labels "io.fstack.canary.invocation"}}'
        ),
        name,
    )
    remove = ("docker", "container", "rm", "--force", name)
    executor = SequenceExecutor(
        {
            create: [None],
            inspect: ["c" * 64],
            owner: ["fstack-caddy-canary-abc123|raced-token"],
        }
    )
    runner = DockerResourceRunner(executor)
    cleanup = CleanupRegistry(runner)
    spec = ResourceSpec(name, create, ("docker", "container", "rm", "--force"))

    with pytest.raises(CanaryContractError):
        execute_resource(spec, runner, cleanup)
    with pytest.raises(CanaryContractError):
        cleanup.run_and_verify()
    assert remove not in executor.calls


def test_real_runner_retries_identity_capture_after_successful_create() -> None:
    name = "fstack-caddy-canary-abc123-worker"
    create = ("docker", "run", "-d", "--name", name, "image")
    inspect = ("docker", "container", "inspect", "--format", "{{.Id}}", name)
    executor = SequenceExecutor(
        {
            create: ["container-id"],
            inspect: [None, "container-id"],
        }
    )
    runner = DockerResourceRunner(executor)
    cleanup = CleanupRegistry(runner)
    cleanup.register(name, ("docker", "container", "rm", "--force"))

    identity = runner.create(name, create)

    assert identity == "container-id"
    assert executor.calls.count(inspect) == 2


def test_successful_create_with_delayed_identity_is_recovered_for_cleanup() -> None:
    name = "fstack-caddy-canary-abc123-worker"
    create = ("docker", "run", "-d", "--name", name, "image")
    inspect = ("docker", "container", "inspect", "--format", "{{.Id}}", name)
    owner = (
        "docker",
        "container",
        "inspect",
        "--format",
        (
            '{{index .Config.Labels "io.fstack.canary"}}|'
            '{{index .Config.Labels "io.fstack.canary.invocation"}}'
        ),
        name,
    )
    remove = ("docker", "container", "rm", "--force", name)
    executor = SequenceExecutor(
        {
            create: ["c" * 64],
            inspect: [*[None] * 10, "c" * 64, "c" * 64],
            owner: ["fstack-caddy-canary-abc123|test-invocation"],
            remove: ["removed"],
            ("docker", "container", "inspect", name): [None],
        }
    )
    runner = DockerResourceRunner(executor)
    cleanup = CleanupRegistry(runner)
    spec = ResourceSpec(name, create, ("docker", "container", "rm", "--force"))

    with pytest.raises(CanaryContractError):
        execute_resource(spec, runner, cleanup)
    cleanup.run_and_verify()

    assert executor.calls.count(remove) == 1


def test_real_runner_removes_the_captured_container_exactly_once() -> None:
    name = "fstack-caddy-canary-abc123-worker"
    create = ("docker", "run", "-d", "--name", name, "image")
    inspect = ("docker", "container", "inspect", "--format", "{{.Id}}", name)
    owner = (
        "docker",
        "container",
        "inspect",
        "--format",
        (
            '{{index .Config.Labels "io.fstack.canary"}}|'
            '{{index .Config.Labels "io.fstack.canary.invocation"}}'
        ),
        name,
    )
    remove = ("docker", "container", "rm", "--force", name)
    executor = SequenceExecutor(
        {
            create: ["container-id"],
            inspect: ["container-id", "container-id"],
            owner: ["fstack-caddy-canary-abc123|test-invocation"],
            remove: ["container-id"],
        }
    )
    runner = DockerResourceRunner(executor)
    cleanup = CleanupRegistry(runner)
    cleanup.register(name, ("docker", "container", "rm", "--force"))
    cleanup.mark_created(name, runner.create(name, create))

    cleanup.run_and_verify()

    assert executor.calls.count(remove) == 1


def test_real_runner_rechecks_container_identity_before_cleanup() -> None:
    name = "fstack-caddy-canary-abc123-worker"
    create = ("docker", "run", "-d", "--name", name, "image")
    inspect = ("docker", "container", "inspect", "--format", "{{.Id}}", name)
    executor = SequenceExecutor(
        {
            create: ["container-id"],
            inspect: ["container-id", "replacement-id"],
        }
    )
    runner = DockerResourceRunner(executor)
    cleanup = CleanupRegistry(runner)
    cleanup.register(name, ("docker", "container", "rm", "--force"))
    resource_id = runner.create(name, create)
    cleanup.mark_created(name, resource_id)

    with pytest.raises(CanaryContractError):
        cleanup.run_and_verify()

    assert ("docker", "container", "rm", "--force", name) not in executor.calls


def test_commands_share_one_network_and_preflight_before_provisioning(tmp_path: Path) -> None:
    inputs = CanaryInputs(
        source_commit="a" * 40,
        source_archive=None,
        source_archive_sha256=None,
        caddy_image=_digest("caddy:2.11.7", "1"),
        postgres_image=_digest("postgres:17-alpine", "2"),
        temporal_image=_digest("temporalio/auto-setup:1.25.2", "3"),
        crusher_image=_digest("ghcr.io/michaelgold/crusher:canary", "4"),
        evidence_dir=tmp_path / "evidence",
        nonce="abc123",
    )
    plan = build_plan(inputs)
    secrets_bundle = RuntimeSecrets("pg", "operator", "application", "magic")
    commands = {
        command.name: command
        for command in build_runtime_commands(
            plan, tmp_path / "source", tmp_path / "runtime", secrets_bundle
        )
    }

    for name in ("caddy", "api", "worker", "postgres", "temporal"):
        assert ("--network", plan.network) in list(
            zip(commands[name].argv, commands[name].argv[1:], strict=False)
        )
    socket_mount = f"{plan.control_volume}:/run/fstack-caddy"
    socket_consumers = {name for name, command in commands.items() if socket_mount in command.argv}
    assert socket_consumers == {"caddy", "worker"}
    invocation_label = f"io.fstack.canary.invocation={plan.invocation_token}"
    for command in commands.values():
        assert invocation_label in command.argv
    assert ("--ip", plan.caddy_ip) in list(
        zip(commands["caddy"].argv, commands["caddy"].argv[1:], strict=False)
    )
    for key, value in plan.worker_environment.items():
        assert commands["worker"].environment[key] == value
    assert ("--group-add", str(plan.worker_supplemental_gid)) in list(
        zip(commands["worker"].argv, commands["worker"].argv[1:], strict=False)
    )
    assert f"{plan.control_volume}:/run/fstack-caddy" in commands["caddy"].argv
    assert f"{plan.control_volume}:/run/fstack-caddy" in commands["worker"].argv
    assert "127.0.0.1" not in plan.listener_address
    assert commands["worker"].environment["FSTACK_CADDY_PROBE_ADDRESS"] == plan.listener_address


def test_live_verification_commands_inspect_exact_route_and_probe_both_hops(tmp_path: Path) -> None:
    inputs = CanaryInputs(
        source_commit="a" * 40,
        source_archive=None,
        source_archive_sha256=None,
        caddy_image=_digest("caddy:2.11.7", "1"),
        postgres_image=_digest("postgres:17-alpine", "2"),
        temporal_image=_digest("temporalio/auto-setup:1.25.2", "3"),
        crusher_image=_digest("ghcr.io/michaelgold/crusher:canary", "4"),
        evidence_dir=tmp_path / "evidence",
        nonce="abc123",
    )
    plan = build_plan(inputs)
    deployment_id = "12345678-1234-5678-1234-567812345678"
    commands = build_live_verification_commands(plan, deployment_id)
    by_name = {command.name: command for command in commands}

    assert tuple(by_name) == (
        "inspect-exact-caddy-route",
        "probe-caddy-to-crusher",
        "probe-worker-through-caddy",
    )
    combined = "\n".join(value for command in commands for value in command.argv)
    assert f"fstack-crusher-{deployment_id.replace('-', '')}" in combined
    assert plan.caddy_ip in combined
    assert f"{inputs.nonce}.canary.invalid" in combined
    assert "/var/run/docker.sock" not in combined
    route_script = by_name["inspect-exact-caddy-route"].argv[-1]
    assert "CaddyAdminTransport" in route_script
    assert "routes==[expected]" in route_script
    counts = _durable_counts_command(plan, "12345678-1234-5678-9234-567812345679")
    assert counts[:4] == ("docker", "exec", plan.postgres_container, "psql")
    assert "12345678-1234-5678-9234-567812345679" in counts[-1]
    for sql_command in (
        _dispatch_generation_command(plan, "12345678-1234-5678-9234-567812345679"),
        _submission_ids_command(plan, "12345678-1234-5678-9234-567812345679"),
    ):
        database_index = sql_command.index("--dbname") + 1
        assert sql_command[database_index] == plan.database_name
    get_routes = _caddy_routes_command(plan)
    set_routes = _caddy_routes_command(plan, replacement=[{"@id": "fixture"}])
    adopt = _adopt_caddy_route_command(plan, {"@id": "fixture"})
    conflict_post = _api_post_command(plan, f"{plan.prefix}-unmanaged", scenario="unmanaged")
    for command in (get_routes, set_routes, adopt):
        assert command[:3] == ("docker", "exec", plan.worker_container)
    assert "method='PATCH'" in set_routes[-1]
    assert "CaddyAdminTransport" in get_routes[-1]
    assert "._exchange(request)" in get_routes[-1]
    assert "CaddyAdmin" in adopt[-1]
    assert "scenario='unmanaged'" in conflict_post[-1]
    for scenario in ("wrong-network", "wrong-probe-ip", "missing-alias"):
        topology = _topology_mismatch_command(
            plan,
            operation_id="12345678-1234-5678-9234-567812345679",
            runtime_submission_id="22345678-1234-5678-9234-567812345679",
            ingress_submission_id="32345678-1234-5678-9234-567812345679",
            deployment_id="42345678-1234-5678-9234-567812345679",
            scenario=scenario,
        )
        assert topology[:3] == ("docker", "exec", plan.worker_container)
        assert "preflight_then_reconcile" in topology[-1]
        assert scenario in topology[-1]
        compile(topology[-1], f"<topology-{scenario}>", "exec")

    for command in (
        *commands,
        CommandSpec("get", get_routes),
        CommandSpec("set", set_routes),
        CommandSpec("adopt", adopt),
        CommandSpec("post", conflict_post),
    ):
        if "python" in command.argv and "-c" in command.argv:
            compile(command.argv[-1], f"<{command.name}>", "exec")


def test_adoption_selects_the_only_exact_managed_route() -> None:
    deployment_hex = "a" * 32
    route = {"@id": f"fstack-crusher-ingress-{deployment_hex}"}
    assert _select_exact_managed_route([route], deployment_hex) == route
    with pytest.raises(CanaryContractError):
        _select_exact_managed_route([route, {"@id": "unrelated"}], deployment_hex)


def test_live_conflict_fixtures_cover_every_required_real_caddy_case() -> None:
    managed = [{"@id": "existing", "match": [{"host": ["existing.invalid"]}]}]
    deployment_hex = "a" * 32
    host = "case.abc123.canary.invalid"
    expected = {
        "expected-id-different-content": "caddy_route_id_conflict",
        "expected-id-plus-same-host": "caddy_host_conflict",
        "same-host-other-id": "caddy_host_conflict",
        "wildcard-host": "caddy_host_conflict",
        "catch-all": "caddy_routes_ambiguous",
        "unclassifiable": "caddy_routes_ambiguous",
        "missing-routes": "caddy_prerequisite_missing",
    }

    for scenario, code in expected.items():
        fixture = _conflict_fixture(scenario, deployment_hex, host, managed)
        assert fixture.expected_code == code
        if scenario == "missing-routes":
            assert fixture.routes is None
        else:
            assert fixture.routes is not None
            assert len(fixture.routes) > len(managed)


def test_runtime_commands_are_detached_configured_and_keep_secrets_out_of_argv(
    tmp_path: Path,
) -> None:
    inputs = CanaryInputs(
        source_commit="a" * 40,
        source_archive=None,
        source_archive_sha256=None,
        caddy_image=_digest("caddy:2.11.7", "1"),
        postgres_image=_digest("postgres:17-alpine", "2"),
        temporal_image=_digest("temporalio/auto-setup:1.25.2", "3"),
        crusher_image=_digest("ghcr.io/michaelgold/crusher:canary", "4"),
        evidence_dir=tmp_path / "evidence",
        nonce="abc123",
    )
    plan = build_plan(inputs)
    secrets = RuntimeSecrets(
        postgres_password="pg-secret-marker",
        operator_token="operator-secret-marker",
        application_password="application-secret-marker",
        magic_link_secret="magic-secret-marker",
    )
    commands = build_runtime_commands(plan, tmp_path / "source", tmp_path / "runtime", secrets)
    by_name = {command.name: command for command in commands}

    for name in ("postgres", "temporal", "caddy", "api", "worker"):
        assert "-d" in by_name[name].argv
        assert plan.network in by_name[name].argv
    assert by_name["worker"].argv[-3:] == (
        "python",
        "-m",
        "fstack.temporal_worker",
    )
    assert ("--user", f"{plan.worker_uid}:{plan.worker_gid}") in list(
        zip(by_name["worker"].argv, by_name["worker"].argv[1:], strict=False)
    )
    assert str(plan.docker_socket_gid) in by_name["worker"].argv
    assert "/var/run/docker.sock:/var/run/docker.sock" in by_name["worker"].argv
    assert "/var/run/docker.sock:/var/run/docker.sock" not in by_name["api"].argv
    assert "--adapter json" not in " ".join(by_name["caddy"].argv)
    assert "caddy run --config /etc/caddy/caddy.json" in " ".join(by_name["caddy"].argv)
    assert (
        f"{tmp_path / 'runtime' / 'request.json'}:/run/fstack-canary-request.json:ro"
        in by_name["api"].argv
    )
    assert by_name["worker"].environment["FSTACK_CADDY_ADMIN_ROOT"] == "/run/fstack-caddy"
    assert by_name["worker"].environment["FSTACK_CADDY_ADMIN_UID"] == str(plan.worker_uid)
    assert by_name["worker"].environment["FSTACK_CADDY_ADMIN_GID"] == str(plan.caddy_socket_gid)
    assert by_name["caddy"].environment["CADDY_SOCKET_MODE"] == "0220"
    assert by_name["temporal"].environment["DEFAULT_NAMESPACE"] == plan.temporal_namespace
    assert by_name["temporal"].environment["DBNAME"] == plan.temporal_database_name
    assert (
        by_name["temporal"].environment["VISIBILITY_DBNAME"]
        == plan.temporal_visibility_database_name
    )
    all_argv = "\n".join(argument for command in commands for argument in command.argv)
    assert "pg-secret-marker" not in all_argv
    assert "operator-secret-marker" not in all_argv
    assert "application-secret-marker" not in all_argv
    assert "magic-secret-marker" not in all_argv


def test_resource_specs_create_dependencies_in_order_and_define_exact_cleanup(
    tmp_path: Path,
) -> None:
    inputs = CanaryInputs(
        source_commit="a" * 40,
        source_archive=None,
        source_archive_sha256=None,
        caddy_image=_digest("caddy:2.11.7", "1"),
        postgres_image=_digest("postgres:17-alpine", "2"),
        temporal_image=_digest("temporalio/auto-setup:1.25.2", "3"),
        crusher_image=_digest("ghcr.io/michaelgold/crusher:canary", "4"),
        evidence_dir=tmp_path / "evidence",
        nonce="abc123",
    )
    plan = build_plan(inputs)
    secrets = RuntimeSecrets("pg", "operator", "application", "magic")
    specs = build_resource_specs(plan, tmp_path / "source", tmp_path / "runtime", secrets)
    names = [spec.name for spec in specs]

    assert names[:4] == [
        plan.network,
        plan.control_volume,
        f"{plan.prefix}-fstack-base:source",
        plan.fstack_image,
    ]
    assert names.index(plan.postgres_container) < names.index(plan.temporal_container)
    assert names.index(plan.caddy_container) < names.index(plan.worker_container)
    assert all(spec.name.startswith(plan.prefix) for spec in specs)
    assert all(
        spec.remove[:2]
        in {
            ("docker", "network"),
            ("docker", "volume"),
            ("docker", "image"),
            ("docker", "container"),
        }
        for spec in specs
    )
    assert all(not spec.name.endswith("-crusher") for spec in specs)


def test_cleanup_recovers_accepted_operations_from_durable_canary_state() -> None:
    plan = build_plan(
        CanaryInputs(
            source_commit="a" * 40,
            source_archive=None,
            source_archive_sha256=None,
            caddy_image=_digest("caddy:2.11.7", "b"),
            postgres_image=_digest("postgres:17", "c"),
            temporal_image=_digest("temporalio/auto-setup:1.27", "d"),
            crusher_image=_digest("crusher:canary", "e"),
            evidence_dir=Path("/evidence"),
            nonce="abc123",
        )
    )
    operation_id = "22345678-1234-5678-9234-567812345678"
    deployment_id = "12345678-1234-5678-9234-567812345678"
    conflict_operation_id = "32345678-1234-5678-9234-567812345678"
    conflict_deployment_id = "42345678-1234-5678-9234-567812345678"
    scenarios = (
        "expected-id-different-content",
        "expected-id-plus-same-host",
        "same-host-other-id",
        "wildcard-host",
        "catch-all",
        "unclassifiable",
        "missing-routes",
    )
    slugs = ("canary-abc123", *(f"canary-abc123-{scenario}" for scenario in scenarios))
    sql = (
        "SELECT id::text || '|' || deployment_id::text "
        "FROM provisioning_operations "
        f"WHERE organization_slug IN ({','.join(repr(slug) for slug in slugs)}) ORDER BY id"
    )
    command = (
        "docker",
        "exec",
        plan.postgres_container,
        "psql",
        "--username",
        "fstack",
        "--dbname",
        plan.database_name,
        "--tuples-only",
        "--no-align",
        "--command",
        sql,
    )
    executor = FakeExecutor(
        {
            command: (
                f"{operation_id}|{deployment_id}\n"
                f"{conflict_operation_id}|{conflict_deployment_id}\n"
            )
        }
    )
    assert _recover_expected_operations(executor, plan) == {
        operation_id: deployment_id,
        conflict_operation_id: conflict_deployment_id,
    }


def test_cleanup_quiesces_api_and_worker_before_dynamic_inventory() -> None:
    plan = build_plan(
        CanaryInputs(
            source_commit="a" * 40,
            source_archive=None,
            source_archive_sha256=None,
            caddy_image=_digest("caddy:2.11.7", "b"),
            postgres_image=_digest("postgres:17", "c"),
            temporal_image=_digest("temporalio/auto-setup:1.27", "d"),
            crusher_image=_digest("crusher:canary", "e"),
            evidence_dir=Path("/evidence"),
            nonce="abc123",
        )
    )
    api_inspect = ("docker", "container", "inspect", plan.api_container)
    worker_inspect = ("docker", "container", "inspect", plan.worker_container)
    stop = (
        "docker",
        "container",
        "stop",
        "--time",
        "10",
        plan.api_container,
        plan.worker_container,
    )
    running = ("docker", "container", "ls", "--format", "{{.Names}}")
    executor = FakeExecutor(
        {stop: "stopped", running: "unrelated\n"},
        existing={api_inspect, worker_inspect},
    )
    _quiesce_canary_producers(executor, plan)
    assert executor.calls.index(stop) < executor.calls.index(running)


def test_dynamic_crusher_cleanup_capture_uses_owned_network_not_api_response(
    tmp_path: Path,
) -> None:
    inputs = CanaryInputs(
        source_commit="a" * 40,
        source_archive=None,
        source_archive_sha256=None,
        caddy_image=_digest("caddy:2.11.7", "1"),
        postgres_image=_digest("postgres:17-alpine", "2"),
        temporal_image=_digest("temporalio/auto-setup:1.25.2", "3"),
        crusher_image=_digest("ghcr.io/michaelgold/crusher:canary", "4"),
        evidence_dir=tmp_path / "evidence",
        nonce="abc123",
    )
    plan = build_plan(inputs)
    deployment_id = "12345678-1234-5678-9234-567812345678"
    deployment_hex = deployment_id.replace("-", "")
    operation_id = "22345678-1234-5678-9234-567812345678"
    name = f"fstack-crusher-{deployment_hex}"
    list_command = (
        "docker",
        "container",
        "ls",
        "--all",
        "--filter",
        f"network={plan.network}",
        "--format",
        "{{.Names}}",
    )
    inspect_command = (
        "docker",
        "container",
        "inspect",
        "--format",
        "{{json .}}",
        name,
    )
    image_id_command = (
        "docker",
        "image",
        "inspect",
        "--format",
        "{{.Id}}",
        plan.inputs.crusher_image,
    )
    executor = FakeExecutor(
        {
            list_command: name + "\n",
            image_id_command: "sha256:" + "e" * 64,
            inspect_command: json.dumps(
                [
                    {
                        "Id": "d" * 64,
                        "Image": "sha256:" + "e" * 64,
                        "Config": {
                            "Image": plan.inputs.crusher_image,
                            "Labels": {
                                "io.fstack.managed": "true",
                                "io.fstack.operation_id": operation_id,
                                "io.fstack.deployment_id": deployment_id,
                            },
                        },
                        "NetworkSettings": {"Networks": {plan.network: {}}},
                    }
                ]
            ),
        },
        existing={("docker", "network", "inspect", plan.network)},
    )

    expected_operations = {operation_id: deployment_id}
    assert _capture_network_crusher_containers(executor, plan, expected_operations) == [
        (name, "d" * 64, operation_id, deployment_id)
    ]
    global_list_command = (
        "docker",
        "container",
        "ls",
        "--all",
        "--filter",
        "label=io.fstack.managed=true",
        "--format",
        "{{.Names}}",
    )
    disconnected = json.loads(executor.outputs[inspect_command])
    disconnected[0]["NetworkSettings"]["Networks"] = {}
    global_executor = FakeExecutor(
        {
            global_list_command: name + "\n",
            image_id_command: "sha256:" + "e" * 64,
            inspect_command: json.dumps(disconnected),
        }
    )
    assert _capture_network_crusher_containers(
        global_executor,
        plan,
        expected_operations,
        require_network=False,
    ) == [(name, "d" * 64, operation_id, deployment_id)]
    wrong_image = json.loads(executor.outputs[inspect_command])
    wrong_image[0]["Config"]["Image"] = "mutable:latest"
    executor.outputs[inspect_command] = json.dumps(wrong_image)
    with pytest.raises(CanaryContractError):
        _capture_network_crusher_containers(executor, plan, expected_operations)


def test_dynamic_capture_rejects_same_name_replacement_identity() -> None:
    original = [("fstack-crusher-" + "a" * 32, "1" * 64, "op", "deployment")]
    replacement = [("fstack-crusher-" + "a" * 32, "2" * 64, "op", "deployment")]

    with pytest.raises(CanaryContractError):
        _merge_captured_resources(original, replacement)
    assert _merge_captured_resources(original, list(original)) == original


def test_full_resource_failure_cleans_every_previously_captured_identity(tmp_path: Path) -> None:
    inputs = CanaryInputs(
        source_commit="a" * 40,
        source_archive=None,
        source_archive_sha256=None,
        caddy_image=_digest("caddy:2.11.7", "1"),
        postgres_image=_digest("postgres:17-alpine", "2"),
        temporal_image=_digest("temporalio/auto-setup:1.25.2", "3"),
        crusher_image=_digest("ghcr.io/michaelgold/crusher:canary", "4"),
        evidence_dir=tmp_path / "evidence",
        nonce="abc123",
    )
    plan = build_plan(inputs)
    specs = build_resource_specs(
        plan,
        tmp_path / "source",
        tmp_path / "runtime",
        RuntimeSecrets("pg", "operator", "application", "magic"),
    )
    runner = RecordingRunner(fail_create={specs[-1].name})
    cleanup = CleanupRegistry(runner)
    with pytest.raises(CanaryContractError):
        for spec in specs:
            execute_resource(spec, runner, cleanup)
    cleanup.run_and_verify()

    created = [
        name for event, name in runner.events if event == "create" and name != specs[-1].name
    ]
    assert [name for event, name in runner.events if event == "remove"] == list(reversed(created))
    assert [name for event, name in runner.events if event == "verify-absent"] == list(
        reversed(created)
    )


def test_runtime_files_define_unix_admin_socket_and_private_secret_material(tmp_path: Path) -> None:
    inputs = CanaryInputs(
        source_commit="a" * 40,
        source_archive=None,
        source_archive_sha256=None,
        caddy_image=_digest("caddy:2.11.7", "1"),
        postgres_image=_digest("postgres:17-alpine", "2"),
        temporal_image=_digest("temporalio/auto-setup:1.25.2", "3"),
        crusher_image=_digest("ghcr.io/michaelgold/crusher:canary", "4"),
        evidence_dir=tmp_path / "evidence",
        nonce="abc123",
    )
    plan = build_plan(inputs)
    secrets = RuntimeSecrets(
        postgres_password="pg-secret-marker",
        operator_token="operator-secret-marker",
        application_password="application-secret-marker",
        magic_link_secret="magic-secret-marker",
    )
    runtime = tmp_path / "runtime"

    prepare_runtime_files(plan, runtime, secrets)

    caddy = json.loads((runtime / "caddy.json").read_text(encoding="utf-8"))
    assert caddy["admin"]["listen"] == "unix//run/fstack-caddy/caddy-admin.sock"
    assert caddy["admin"]["config"]["persist"] is False
    assert caddy["apps"]["http"]["servers"]["fstack"] == {
        "listen": [":8080"],
        "routes": [],
    }
    assert "tcp/" not in json.dumps(caddy)
    build_context = runtime / "build-context"
    assert "docker.io" in (build_context / "Dockerfile").read_text(encoding="utf-8")
    runtime_commands = build_runtime_commands(plan, tmp_path / "source", runtime, secrets)
    build_command = {command.name: command for command in runtime_commands}["build-fstack-runtime"]
    assert build_command.argv[-1] == str(build_context)
    request = json.loads((runtime / "request.json").read_text(encoding="utf-8"))
    assert request == {
        "organization_slug": "canary-abc123",
        "organization_name": "Fstack canary abc123",
        "business_slug": "mail",
        "business_name": "Canary mail",
        "environment_slug": "production",
        "public_domain": "abc123.canary.invalid",
        "crusher_image": inputs.crusher_image,
        "database_secret_ref": "secret://local-file/canary/database",
        "object_storage_secret_ref": "secret://local-file/canary/database",
    }
    for filename in ("postgres.env", "temporal.env", "api.env", "worker.env"):
        assert (runtime / filename).stat().st_mode & 0o777 == 0o600
    source_bundle = runtime / "source-secrets" / "canary" / "database"
    assert (
        json.loads((source_bundle / "owner.json").read_text(encoding="utf-8"))[
            "application_password"
        ]
        == "application-secret-marker"
    )
    assert (source_bundle / "owner.json").stat().st_mode & 0o777 == 0o600


def test_collision_preflight_rejects_any_exact_existing_resource(tmp_path: Path) -> None:
    inputs = CanaryInputs(
        source_commit="a" * 40,
        source_archive=None,
        source_archive_sha256=None,
        caddy_image=_digest("caddy:2.11.7", "1"),
        postgres_image=_digest("postgres:17-alpine", "2"),
        temporal_image=_digest("temporalio/auto-setup:1.25.2", "3"),
        crusher_image=_digest("ghcr.io/michaelgold/crusher:canary", "4"),
        evidence_dir=tmp_path / "evidence",
        nonce="abc123",
    )
    plan = build_plan(inputs)
    container_names = (
        plan.postgres_container,
        plan.temporal_container,
        plan.caddy_container,
        plan.api_container,
        plan.worker_container,
        f"{plan.prefix}-migration-20261006_0003",
        f"{plan.prefix}-migration-20261013_0004",
        f"{plan.prefix}-migration-20261015_0005",
    )
    for name in container_names:
        collision = ("docker", "container", "inspect", name)
        executor = FakeExecutor({}, existing={collision})
        with pytest.raises(CanaryContractError):
            assert_no_resource_collisions(plan, executor)


def test_process_exit_proof_ignores_unrelated_containers_but_fails_on_canary_names(
    tmp_path: Path,
) -> None:
    inputs = CanaryInputs(
        source_commit="a" * 40,
        source_archive=None,
        source_archive_sha256=None,
        caddy_image=_digest("caddy:2.11.7", "1"),
        postgres_image=_digest("postgres:17-alpine", "2"),
        temporal_image=_digest("temporalio/auto-setup:1.25.2", "3"),
        crusher_image=_digest("ghcr.io/michaelgold/crusher:canary", "4"),
        evidence_dir=tmp_path / "evidence",
        nonce="abc123",
    )
    plan = build_plan(inputs)

    assert _canary_processes_absent("unrelated-one\nunrelated-two\n", plan)
    assert not _canary_processes_absent(f"unrelated\n{plan.worker_container}\n", plan)
    assert not _canary_processes_absent(f"{plan.api_container}\n", plan)


def test_maintenance_sequence_keeps_api_off_until_v2_polling_is_proved(tmp_path: Path) -> None:
    inputs = CanaryInputs(
        source_commit="a" * 40,
        source_archive=None,
        source_archive_sha256=None,
        caddy_image=_digest("caddy:2.11.7", "1"),
        postgres_image=_digest("postgres:17-alpine", "2"),
        temporal_image=_digest("temporalio/auto-setup:1.25.2", "3"),
        crusher_image=_digest("ghcr.io/michaelgold/crusher:canary", "4"),
        evidence_dir=tmp_path / "evidence",
        nonce="abc123",
    )
    steps = build_plan(inputs).maintenance_steps

    assert steps == (
        "bootstrap-20261006-0003",
        "stop-api-dispatcher-worker",
        "verify-process-exit",
        "verify-no-old-poller",
        "upgrade-20261013-0004",
        "upgrade-20261015-0005",
        "start-v2-worker-dispatcher",
        "verify-v1-v2-registration",
        "verify-new-poller",
        "enable-api",
        "insert-v2-command",
    )
    assert steps.index("enable-api") > steps.index("verify-new-poller")
    assert steps.index("insert-v2-command") > steps.index("upgrade-20261015-0005")

    commands = build_maintenance_steps(
        build_plan(inputs),
        tmp_path / "source",
        tmp_path / "runtime",
        RuntimeSecrets("pg", "op", "app", "magic"),
    )
    assert tuple(command.name for command in commands) == steps
    by_name = {command.name: command for command in commands}
    assert by_name["stop-api-dispatcher-worker"].argv[:3] == (
        "docker",
        "container",
        "stop",
    )
    assert by_name["verify-process-exit"].argv != by_name["stop-api-dispatcher-worker"].argv
    for name, revision in (
        ("bootstrap-20261006-0003", "20261006_0003"),
        ("upgrade-20261013-0004", "20261013_0004"),
        ("upgrade-20261015-0005", "20261015_0005"),
    ):
        command = by_name[name]
        assert command.argv[0:2] == ("docker", "create")
        assert command.argv[-3:] == ("alembic", "upgrade", revision)
        assert "--env-file" in command.argv
    assert plan_secret_markers(commands) == set()


def test_temporal_poller_evidence_requires_a_nonempty_workflow_or_activity_poller() -> None:
    assert _task_queue_has_pollers('{"pollers":[]}') is False
    assert (
        _task_queue_has_pollers('{"workflowTaskQueuePollers":[],"activityTaskQueuePollers":[]}')
        is False
    )
    assert (
        _task_queue_has_pollers('{"workflowTaskQueuePollers":[{"identity":"worker@canary"}]}')
        is True
    )
    with pytest.raises(CanaryContractError):
        _task_queue_has_pollers("not-json")
    with pytest.raises(CanaryContractError):
        _task_queue_has_pollers("{}")
    with pytest.raises(CanaryContractError):
        _task_queue_has_pollers('{"unknown":[]}')


def plan_secret_markers(commands: tuple[CommandSpec, ...]) -> set[str]:
    argv = "\n".join(value for command in commands for value in command.argv)
    return {marker for marker in ("pg", "op", "app", "magic") if marker in argv.split("\n")}


def test_cli_requires_exact_task21_inputs(tmp_path: Path) -> None:
    args = parse_args(
        [
            "--source-commit",
            "a" * 40,
            "--caddy-image-digest",
            _digest("caddy:2.11.7", "1"),
            "--postgres-image-digest",
            _digest("postgres:17-alpine", "2"),
            "--temporal-image-digest",
            _digest("temporalio/auto-setup:1.25.2", "3"),
            "--crusher-image-digest",
            _digest("ghcr.io/michaelgold/crusher:canary", "4"),
            "--evidence-dir",
            str(tmp_path / "evidence"),
        ]
    )

    assert args.source_commit == "a" * 40
    assert args.evidence_dir == tmp_path / "evidence"
    assert args.nonce is None


def test_main_constructs_validated_inputs_and_invokes_the_live_runner(tmp_path: Path) -> None:
    received: list[CanaryInputs] = []

    def runner(inputs: CanaryInputs) -> None:
        received.append(inputs)

    result = main(
        [
            "--source-commit",
            "a" * 40,
            "--caddy-image-digest",
            _digest("caddy:2.11.7", "1"),
            "--postgres-image-digest",
            _digest("postgres:17-alpine", "2"),
            "--temporal-image-digest",
            _digest("temporalio/auto-setup:1.25.2", "3"),
            "--crusher-image-digest",
            _digest("ghcr.io/michaelgold/crusher:canary", "4"),
            "--evidence-dir",
            str(tmp_path / "evidence"),
            "--nonce",
            "abc123",
        ],
        runner=runner,
    )

    assert result == 0
    assert len(received) == 1
    assert received[0].nonce == "abc123"
    assert received[0].evidence_dir == tmp_path / "evidence"


def test_evidence_directory_must_be_new_external_and_private(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    evidence = tmp_path / "evidence"

    descriptor = create_evidence_directory(repo, evidence)
    try:
        assert evidence.stat().st_mode & 0o777 == 0o700
    finally:
        descriptor.close()

    with pytest.raises(CanaryContractError):
        create_evidence_directory(repo, evidence)
    with pytest.raises(CanaryContractError):
        create_evidence_directory(repo, repo / "evidence")


def test_evidence_is_descriptor_relative_private_sanitized_and_immutable(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    evidence = create_evidence_directory(repo, tmp_path / "evidence")
    try:
        write_evidence(
            evidence,
            "summary.json",
            {"source": "a" * 40, "status": "succeeded"},
            forbidden_values={"secret-marker"},
        )
        written = tmp_path / "evidence" / "summary.json"
        assert written.stat().st_mode & 0o777 == 0o600
        assert json.loads(written.read_text(encoding="utf-8"))["status"] == "succeeded"
        with pytest.raises(CanaryContractError):
            write_evidence(
                evidence,
                "summary.json",
                {"status": "replacement"},
                forbidden_values=set(),
            )
        with pytest.raises(CanaryContractError):
            write_evidence(
                evidence,
                "unsafe.json",
                {"status": "secret-marker"},
                forbidden_values={"secret-marker"},
            )
    finally:
        evidence.close()


def test_success_evidence_is_written_only_after_error_free_cleanup(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    blocked = create_evidence_directory(repo, tmp_path / "blocked-evidence")
    try:
        _finalize_success_evidence(
            blocked,
            {"status": "succeeded"},
            forbidden_values=set(),
            primary_error=None,
            cleanup_error=CanaryContractError("cleanup failed"),
        )
        assert not (blocked.path / "summary.json").exists()
    finally:
        blocked.close()

    complete = create_evidence_directory(repo, tmp_path / "complete-evidence")
    try:
        _finalize_success_evidence(
            complete,
            {"status": "succeeded"},
            forbidden_values=set(),
            primary_error=None,
            cleanup_error=None,
        )
        assert json.loads((complete.path / "summary.json").read_text())["status"] == "succeeded"
    finally:
        complete.close()


def test_prepare_source_queries_git_and_rejects_dirty_or_mismatched_head(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    subprocess.run(["git", "init", "-q", str(repo)], check=True)
    subprocess.run(
        ["git", "-C", str(repo), "config", "user.email", "canary@example.invalid"], check=True
    )
    subprocess.run(["git", "-C", str(repo), "config", "user.name", "Canary"], check=True)
    (repo / "tracked.txt").write_text("clean", encoding="utf-8")
    subprocess.run(["git", "-C", str(repo), "add", "tracked.txt"], check=True)
    subprocess.run(["git", "-C", str(repo), "commit", "-qm", "fixture"], check=True)
    commit = subprocess.check_output(
        ["git", "-C", str(repo), "rev-parse", "HEAD"], text=True
    ).strip()

    prepared = prepare_source(
        repo,
        source_commit=commit,
        source_archive=None,
        source_archive_sha256=None,
        workspace=tmp_path / "workspace",
    )
    assert prepared.root == (tmp_path / "workspace").resolve()
    assert (prepared.root / "tracked.txt").read_text(encoding="utf-8") == "clean"
    assert prepared.identity == commit

    (repo / "untracked.txt").write_text("dirty", encoding="utf-8")
    with pytest.raises(CanaryContractError):
        prepare_source(
            repo,
            source_commit=commit,
            source_archive=None,
            source_archive_sha256=None,
            workspace=tmp_path / "workspace-dirty",
        )
    with pytest.raises(CanaryContractError):
        prepare_source(
            repo,
            source_commit="0" * 40,
            source_archive=None,
            source_archive_sha256=None,
            workspace=tmp_path / "workspace-wrong",
        )


def test_prepare_source_extracts_verified_safe_archive_and_rejects_traversal(
    tmp_path: Path,
) -> None:
    archive = tmp_path / "source.tar"
    with tarfile.open(archive, "w") as bundle:
        payload = b"frozen"
        member = tarfile.TarInfo("source/tracked.txt")
        member.size = len(payload)
        bundle.addfile(member, io.BytesIO(payload))
    digest = hashlib.sha256(archive.read_bytes()).hexdigest()

    prepared = prepare_source(
        tmp_path / "unused",
        source_commit=None,
        source_archive=archive,
        source_archive_sha256=digest,
        workspace=tmp_path / "workspace",
    )
    assert (prepared.root / "source" / "tracked.txt").read_bytes() == b"frozen"
    assert prepared.identity == digest

    unsafe = tmp_path / "unsafe.tar"
    with tarfile.open(unsafe, "w") as bundle:
        payload = b"escape"
        member = tarfile.TarInfo("../escape")
        member.size = len(payload)
        bundle.addfile(member, io.BytesIO(payload))
    unsafe_workspace = tmp_path / "unsafe-workspace"
    with pytest.raises(CanaryContractError):
        prepare_source(
            tmp_path / "unused",
            source_commit=None,
            source_archive=unsafe,
            source_archive_sha256=hashlib.sha256(unsafe.read_bytes()).hexdigest(),
            workspace=unsafe_workspace,
        )
    assert not unsafe_workspace.exists()
