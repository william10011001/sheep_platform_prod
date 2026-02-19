#!/bin/sh
set -e

echo "[entrypoint] args: $*"

# If command is provided (e.g. migrate service), run it.
if [ "$#" -gt 0 ]; then
  exec "$@"
fi

: "${SHEEP_API_ROOT_PATH:=/api}"
: "${SHEEP_BIND:=0.0.0.0}"
: "${SHEEP_PORT:=8000}"

echo "[entrypoint] starting uvicorn on ${SHEEP_BIND}:${SHEEP_PORT} root_path=${SHEEP_API_ROOT_PATH}"
exec uvicorn sheep_platform_api:app \
  --host "${SHEEP_BIND}" \
  --port "${SHEEP_PORT}" \
  --proxy-headers \
  --forwarded-allow-ips="*"
