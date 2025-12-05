# syntax=docker/dockerfile:1.7

FROM ghcr.io/astral-sh/uv:python3.14-bookworm AS build
WORKDIR /app

# Copy project metadata and source code needed for installation
COPY pyproject.toml uv.lock README.md ./
COPY src ./src

# Increase timeout for large package downloads (torch, CUDA packages)
# Set longer timeout and enable concurrent downloads for better performance
ENV UV_HTTP_TIMEOUT=600 \
  UV_CONCURRENT_DOWNLOADS=10

# Install dependencies and the project itself into the virtual environment
RUN uv sync --frozen --no-dev

# Copy the rest of the application data (doesn't affect installed deps)
COPY scripts ./scripts
COPY datasets ./datasets
COPY config.example.yaml sample_tasks.json ./

FROM python:3.14-slim-bookworm AS runtime

# Set working directory
WORKDIR /app

# Create venv structure (will be populated by the build stage copy)
RUN python -m venv .venv || true

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
  VIRTUAL_ENV=/app/.venv \
  PATH="/app/.venv/bin:$PATH"

# Copy virtual environment from build stage (overwrites the empty venv above)
COPY --from=build /app/.venv /app/.venv

# Copy application files
COPY . .

# Expose the API port
EXPOSE 8088

# Health check for the API service
# Use python from the virtual environment or system python3
HEALTHCHECK --interval=30s --timeout=10s --start-period=40s --retries=3 \
  CMD /app/.venv/bin/python -c "import urllib.request; urllib.request.urlopen('http://localhost:8088/healthz').read()" || exit 1

# Set entrypoint and default command
ENTRYPOINT ["pulsar"]
