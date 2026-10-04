FROM ghcr.io/astral-sh/uv:0.9.17 AS uv
FROM python:3.12-slim

WORKDIR /app
COPY --from=uv /uv /uvx /usr/local/bin/
COPY pyproject.toml uv.lock README.md alembic.ini ./
COPY migrations ./migrations
COPY src ./src
RUN uv sync --frozen --no-dev --no-editable

ENV PATH="/app/.venv/bin:$PATH"
EXPOSE 8000
CMD ["uvicorn", "fstack.main:app", "--host", "0.0.0.0", "--port", "8000"]
