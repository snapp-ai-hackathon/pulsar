# syntax=docker/dockerfile:1.7

FROM ghcr.io/astral-sh/uv:python3.11-bookworm AS build
WORKDIR /app

# Copy dependency manifests first for better layer caching
COPY pyproject.toml uv.lock README.md ./
COPY src ./src
COPY scripts ./scripts
COPY datasets ./datasets
COPY config.example.yaml sample_tasks.json ./

# Increase timeout for large package downloads (torch, CUDA packages)
# Set longer timeout and enable concurrent downloads for better performance
ENV UV_HTTP_TIMEOUT=600 \
    UV_CONCURRENT_DOWNLOADS=10
RUN uv sync --frozen --no-dev

FROM python:3.11-slim-bookworm AS runtime
ENV PYTHONUNBUFFERED=1 \
    VIRTUAL_ENV=/app/.venv \
    PATH="/app/.venv/bin:$PATH"
WORKDIR /app

COPY --from=build /app/.venv /app/.venv
COPY . .

EXPOSE 8088
ENTRYPOINT ["pulsar"]
CMD ["--help"]
