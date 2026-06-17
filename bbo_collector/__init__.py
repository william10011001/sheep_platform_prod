# -*- coding: utf-8 -*-
"""Multi-exchange real-time best-bid/offer (BBO) recorder.

WebSocket-based; one async connection per exchange (sharded if needed); records
canonical BBO snapshots to partitioned zstd-Parquet. See registry.py for the
23 supported spot venues (all WS specs live-verified)."""
