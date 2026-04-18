#!/usr/bin/env bash
set -euo pipefail

echo "=== ReviewIQ render-start ==="
echo "PYTHON: $(python3 --version 2>/dev/null || echo 'python not found')"
echo "PWD: $(pwd)"
echo "ENV SUMMARY: PORT=${PORT:-unset} WEB_CONCURRENCY=${WEB_CONCURRENCY:-unset} REVIEWIQ_LLM_ONLY=${REVIEWIQ_LLM_ONLY:-unset}"
echo "Listing installed packages (top 30):"
python3 -m pip list --format=columns | sed -n '1,30p' || true

export GUNICORN_CMD_ARGS="--log-level debug --preload"

# Use WEB_CONCURRENCY if set, otherwise default to 1 worker
WORKERS=${WEB_CONCURRENCY:-1}

echo "Changing directory to backend/ and starting gunicorn: workers=$WORKERS"
cd "$(dirname "$0")" || true
cd .. || true
cd backend || true
exec gunicorn app:app --bind 0.0.0.0:${PORT:-5000} --workers "$WORKERS" --threads 2 --log-level debug
