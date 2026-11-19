import subprocess
from uuid import uuid4

import pytest


def test_recovery_never_removes_unowned_matching_container(monkeypatch):
    from fstack import local_session_runner as runner

    calls = []
    assignment = {
        "session_id": str(uuid4()),
        "workspace_id": str(uuid4()),
        "image": "example.test/image@sha256:" + "a" * 64,
    }

    def docker(*args):
        calls.append(args)
        if args[0] == "ps":
            return subprocess.CompletedProcess(args, 0, "existing-id\n", "")
        if args[0] == "inspect":
            return subprocess.CompletedProcess(
                args, 0, '[{"Id":"existing-id","Config":{"Labels":{},"Image":"unrelated"}}]', ""
            )
        raise AssertionError("must not mutate unrelated container")

    monkeypatch.setattr(runner, "_docker", docker)
    with pytest.raises(RuntimeError, match="local cleanup unconfirmed"):
        runner._remove_owned(assignment)
    assert all(call[0] != "rm" for call in calls)
