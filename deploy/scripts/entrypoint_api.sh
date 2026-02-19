#!/usr/bin/env sh
set -eu

# FastAPI is behind Nginx. We still enable proxy headers so client IP / scheme work.
exec uvicorn sheep_platform_api:app \
  --host 0.0.0.0 \
  --port ${PORT:-8000} \
  --proxy-headers \
  --forwarded-allow-ips='*'
