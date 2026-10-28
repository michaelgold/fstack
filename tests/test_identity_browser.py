from fastapi.testclient import TestClient

from fstack.api import create_app
from fstack.identity import AuthConfiguration


def test_browser_signin_and_explicit_review_controls(monkeypatch):
    monkeypatch.setenv("FSTACK_DATABASE_URL", "postgresql+psycopg://unused@localhost/unused")
    app = create_app(
        auth_configuration=AuthConfiguration(
            "https://testserver", "synthetic-" + "x" * 40, lambda *_: None
        )
    )
    with TestClient(app, base_url="https://testserver") as browser:
        assert browser.get("/auth/sign-in").status_code == 200
        review = browser.get("/auth/device")
        assert review.status_code == 200
        assert "Review request" in review.text
        assert 'id="approve" disabled' in review.text
        script = browser.get("/auth/device.js")
        assert "textContent" in script.text
        assert "innerHTML" not in script.text
        assert review.headers["Cache-Control"] == "no-store"
        assert review.headers["Referrer-Policy"] == "no-referrer"
