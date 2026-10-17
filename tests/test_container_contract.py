from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_api_container_disables_query_bearing_access_logs() -> None:
    dockerfile = (ROOT / "Dockerfile").read_text(encoding="utf-8")

    assert '"--no-access-log"' in dockerfile
