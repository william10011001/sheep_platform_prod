#!/usr/bin/env sh
set -eu

# Streamlit served behind Nginx at /app
exec streamlit run sheep_platform_app.py \
  --server.address 0.0.0.0 \
  --server.port ${PORT:-8501} \
  --server.headless true \
  --server.baseUrlPath app \
  --server.enableCORS false \
  --server.enableXsrfProtection true
