"""Regression coverage for PR review; no external services beyond disposable PG."""

import json
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from uuid import UUID, uuid4

from sqlalchemy import text
from test_runner_discovery_postgres import discovery, enroll  # noqa: F401
from test_runner_sessions_postgres import admission


def test_supervisor_exits_cleanly_with_control_pipe_still_open():
    # Real interpreter/thread/pipe shutdown; Docker and Tailcat are deliberately
    # replaced at their boundaries. Native acceptance is a separate test module.
    code = """
import json, pathlib, subprocess
from fstack import local_session_runner as runner
original_popen = subprocess.Popen
runner._validate = lambda *args: 0.2
runner._docker = lambda *args: subprocess.CompletedProcess(args, 0, '', '')
def generate(*args, **kwargs):
    key = pathlib.Path(kwargs['env']['HOME']) / 'tailcat/keys/default.private.json'
    key.parent.mkdir(parents=True)
    key.write_text('{}')
    return subprocess.CompletedProcess(args, 0, b'nonsecret-test-address', b'')
runner.subprocess.run = generate
runner.subprocess.Popen = lambda *args, **kwargs: original_popen(
    ['/usr/bin/sleep', '30'], **kwargs)
raise SystemExit(runner._supervise())
"""
    process = subprocess.Popen(
        [sys.executable, "-c", code],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    assert process.stdin is not None and process.stdout is not None and process.stderr is not None
    try:
        process.stdin.write(
            (
                json.dumps(
                    {
                        "assignment": {
                            "session_id": str(uuid4()),
                            "workspace_id": str(uuid4()),
                            "client_public_key": "nodekey:" + "1" * 64,
                            "cpu_millis": 1000,
                            "memory_mib": 512,
                        },
                        "image": "test-only",
                        "command": ["/test-only"],
                        "tailcat": "test-only",
                    }
                )
                + "\n"
            ).encode()
        )
        process.stdin.flush()
        # Do not close stdin or communicate(): expiration must exit independently.
        result = process.wait(timeout=8)
        assert result == 0, process.stderr.read().decode()
    finally:
        if process.poll() is None:
            process.kill()
            process.wait(timeout=3)
        process.stdin.close()
        process.stdout.close()
        process.stderr.close()


def test_downgrade_cannot_delete_concurrently_admitted_session(discovery):  # noqa: F811
    import importlib.util
    from pathlib import Path

    from alembic.migration import MigrationContext
    from alembic.operations import Operations
    from sqlalchemy.exc import DBAPIError

    engine, client, account, owner, _ = discovery
    runner = enroll(client, account, owner)
    body = admission(runner["runner_id"])
    assert (
        client.post(f"/v1/auth/accounts/{account}/sessions", headers=owner, json=body).status_code
        == 200
    )
    path = (
        Path(__file__).resolve().parents[1] / "migrations/versions/20261119_0009_runner_sessions.py"
    )
    spec = importlib.util.spec_from_file_location("session_revision", path)
    assert spec is not None and spec.loader is not None
    revision = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(revision)

    def downgrade():
        try:
            with engine.begin() as connection:
                connection.execute(text("SET LOCAL statement_timeout='5s'"))
                connection.execute(text("SET LOCAL application_name='session_downgrade_test'"))
                with Operations.context(MigrationContext.configure(connection)):
                    revision.downgrade()
            return "history deleted"
        except DBAPIError as exc:
            return exc.orig.sqlstate

    with engine.connect() as writer:
        writer.execute(text("CREATE TEMP TABLE saved_session AS SELECT * FROM local_sessions"))
        writer.execute(text("DELETE FROM local_sessions"))
        writer.commit()
        writer.execute(text("SELECT id FROM local_runners FOR UPDATE"))
        writer.execute(text("INSERT INTO local_sessions SELECT * FROM saved_session"))
        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(downgrade)
            try:
                deadline = time.monotonic() + 3
                waiting = False
                while not future.done() and time.monotonic() < deadline:
                    with engine.connect() as observer:
                        waiting = observer.execute(
                            text("""SELECT EXISTS (
                            SELECT 1 FROM pg_stat_activity
                            WHERE application_name='session_downgrade_test'
                            AND wait_event_type='Lock')""")
                        ).scalar_one()
                    if waiting:
                        break
                    time.sleep(0.02)
                assert future.done() or waiting, "downgrade never reached the concurrent fence"
            finally:
                writer.commit()
            assert future.result(timeout=6) == "55P03"  # NOWAIT, not delayed destructive DDL.
    with engine.connect() as observer:
        assert observer.execute(text("SELECT id FROM local_sessions")).scalar_one() == UUID(
            body["session_id"]
        )
