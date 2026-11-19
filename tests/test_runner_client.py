"""Portable client security checks; real transport acceptance is PostgreSQL-backed."""

import pytest

from fstack.runner_client import report_once


@pytest.mark.parametrize(
    "url",
    [
        "http://example.test",
        "https://user:replace-me@example.test",
        "https://example.test/path",
        "https://example.test?x=1",
        "http://127.0.0.1",
        "https://example.test#fragment",
    ],
)
def test_rejects_unsafe_origin_before_network(url):
    with pytest.raises(ValueError, match="invalid runner configuration"):
        report_once(
            url,
            "00000000-0000-0000-0000-000000000001",
            "fs_r_synthetic",
            sequence=1,
            available_slots=0,
            queue_depth=0,
        )
