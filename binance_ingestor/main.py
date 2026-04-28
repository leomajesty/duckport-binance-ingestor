#!/usr/bin/env python3
"""
Binance Ingestor — standalone entry point.

Connects to duckport-rs, initialises the schema, then runs the data
fetching loop (REST or WebSocket mode).

Usage:
    python -m binance_ingestor.main
    # or via pyproject.toml script entry:
    binance-ingestor
"""

import os
import sys
import signal
import platform
from datetime import datetime, timezone, timedelta

from binance_ingestor.utils.log_kit import logger, divider

os.environ['TZ'] = 'UTC'


def main():
    if platform.system() == 'Windows':
        import asyncio
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    from binance_ingestor.config import (
        DUCKPORT_ADDR, DUCKPORT_SCHEMA,
        KLINE_INTERVAL, DATA_SOURCES, ENABLE_WS, START_DATE,
    )
    from binance_ingestor.duckport_client import DuckportClient
    from binance_ingestor.data_jobs import (
        RestfulDataJobs, WebsocketsDataJobs, KLINE_MARKETS,
    )

    divider("Binance Ingestor starting")

    client = DuckportClient(addr=DUCKPORT_ADDR, schema=DUCKPORT_SCHEMA, interval=KLINE_INTERVAL)

    def signal_handler(signum, frame):
        logger.critical("Received exit signal, shutting down...")
        client.close()
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        info = client.ping()
        logger.info(f"duckport-rs server: {info}")
    except Exception as e:
        logger.error(f"Cannot reach duckport-rs at {DUCKPORT_ADDR}: {e}")
        sys.exit(1)

    logger.info(f"Initialising schema on duckport-rs (markets={list(DATA_SOURCES)}, interval={KLINE_INTERVAL})")
    client.init_schema(
        markets=list(KLINE_MARKETS),
        interval=KLINE_INTERVAL,
        data_sources=DATA_SOURCES,
        start_date=START_DATE,
    )
    client.verify_kline_interval(KLINE_INTERVAL)

    enabled_markets = [m for m in KLINE_MARKETS if m in DATA_SOURCES]
    watermarks = client.read_watermark_rows(enabled_markets)
    now = datetime.now(tz=timezone.utc)
    stale = []
    for market, wm in watermarks.items():
        ref_time = wm["duck_time"] or wm["start_time"]
        if ref_time is not None and now - ref_time > timedelta(days=7):
            stale.append((market, ref_time, wm["duck_time"] is None))
    if stale:
        for market, ref_time, used_start in stale:
            lag_days = (now - ref_time).days
            field = "start_time" if used_start else "duck_time"
            logger.error(
                f"Market '{market}' is {lag_days} days behind "
                f"({field}={ref_time:%Y-%m-%d %H:%M:%S UTC}). "
                f"Please run 'loadhist' first to catch up historical data."
            )
        client.close()
        sys.exit(1)

    if ENABLE_WS:
        WebsocketsDataJobs(client)
    else:
        RestfulDataJobs(client)

    # DataJobs spawns daemon threads; keep main thread alive
    try:
        signal.pause()
    except AttributeError:
        # Windows: signal.pause() not available
        import threading
        threading.Event().wait()


if __name__ == "__main__":
    main()
