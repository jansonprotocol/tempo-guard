# syntax=docker/dockerfile:1
FROM python:3.12-slim

WORKDIR /app

# System deps — cached by BuildKit, only reruns if this layer changes
RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt,sharing=locked \
    apt-get update && apt-get install -y --no-install-recommends \
    build-essential libpq-dev libffi-dev \
    && rm -rf /var/lib/apt/lists/*

# Pip deps — cached by BuildKit, only reruns if requirements.txt changes
COPY backend/requirements.txt .
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install -r requirements.txt

# App code — only this layer reruns on normal code deploys
COPY backend/app ./app

# Proxy env vars
ENV HTTP_PROXY="" \
    HTTPS_PROXY="" \
    ALL_PROXY="" \
    http_proxy="" \
    https_proxy="" \
    all_proxy="" \
    NO_PROXY="*"

# Use the PORT environment variable (set by Render) with a fallback to 8000
CMD uvicorn app.main:app --host 0.0.0.0 --port ${PORT:-8000}
