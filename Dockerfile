# syntax=docker/dockerfile:1.7
FROM python:3.11-slim-bookworm

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never \
    UV_PROJECT_ENVIRONMENT=/app/.venv \
    JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 \
    PATH=/app/.venv/bin:/usr/lib/jvm/java-17-openjdk-amd64/bin:$PATH

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless \
        ca-certificates \
        curl \
        procps \
    && rm -rf /var/lib/apt/lists/*

COPY --from=ghcr.io/astral-sh/uv:0.4.30 /uv /usr/local/bin/uv

WORKDIR /app

# Resolve dependencies first to maximize Docker layer cache hits.
COPY pyproject.toml uv.lock README.md ./
RUN uv sync --frozen --no-install-project --no-dev

# Project source.
COPY src ./src
COPY dagster_project ./dagster_project
COPY dbt ./dbt
COPY gx ./gx
COPY Makefile ./

RUN uv sync --frozen --no-dev

RUN useradd --create-home --uid 1000 lakehouse \
    && chown -R lakehouse:lakehouse /app
USER lakehouse

ENTRYPOINT ["python", "-m"]
CMD ["src.ingestion.run", "--help"]
