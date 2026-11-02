"""
Historical kline bulk loader — v2

Flow:
  Step 0  ping + init_schema
  Step 1  Download monthly/daily zips → convert to Parquet
  Step 1b Gap fill: days missing from the monthly archives are fetched as daily zips,
          the affected months are rebuilt (hist/gap_filler.py)
  Step 2  Incremental clean (skip unchanged files via .ok marker, atomic write)
  Step 3  Incremental load (boundary = duck_time - BOUNDARY_DAYS, anti-join dedup);
          gap-filled (symbol, day) pairs are loaded even when they lie before the boundary

Assumptions:
  - Data before duck_time is complete and accurate except for the recorded gaps; no other backfill.
  - A 1-day overlap (boundary = duck_time - 1 day) is kept for redundancy.
  - Parquet files are retained after load; a reminder is printed at exit.

Usage:
    python -m binance_ingestor.loadhist
"""

import asyncio
import os
import random
import re
from datetime import datetime, timedelta
from glob import glob
from typing import Dict, List, Optional, Tuple

import aiohttp
import duckdb
import pandas as pd
import pyarrow as pa
from tqdm import tqdm

from binance_ingestor.config import (
    KLINE_INTERVAL, PARQUET_DIR, DATA_SOURCES,
    DUCKPORT_ADDR, DUCKPORT_SCHEMA, RESOURCE_PATH,
    START_DATE, proxy,
)
from binance_ingestor.duckport_client import DuckportClient
from binance_ingestor.hist import (
    need_analyse_set,
    async_get_usdt_symbols, spot_symbols_filter,
    async_get_daily_list, async_get_monthly_list,
    get_download_prefix,
    async_download_file,
    get_local_path, clean_old_daily_zip,
    batch_process_data,
    GapRecord, fill_gaps,
)
from binance_ingestor.utils.log_kit import logger
from binance_ingestor.utils.timer import timer

KLINE_MARKETS = ('usdt_perp', 'usdt_spot')

# Re-load duck_time - BOUNDARY_DAYS as the starting boundary.
# The 1-day overlap is deduplicated by the anti-join in bulk_write_kline (idempotent).
BOUNDARY_DAYS = int(os.getenv("LOADHIST_BOUNDARY_DAYS", "1"))
LOADHIST_BATCH_ROWS = int(os.getenv("LOADHIST_BATCH_ROWS", "50000"))
LOADHIST_DUCKDB_MEMORY_LIMIT = os.getenv("LOADHIST_DUCKDB_MEMORY_LIMIT", "")
LOADHIST_DUCKDB_THREADS = int(os.getenv("LOADHIST_DUCKDB_THREADS", "1"))

_PARQUET_MONTH_RE = re.compile(r"_(\d{4}-\d{2})_\d+M\.parquet$")


def get_enabled_kline_markets():
    return [m for m in KLINE_MARKETS if m in DATA_SOURCES]


def _local_duckdb():
    con = duckdb.connect()
    con.execute(f"SET threads TO {LOADHIST_DUCKDB_THREADS}")
    if LOADHIST_DUCKDB_MEMORY_LIMIT:
        con.execute(f"SET memory_limit = '{LOADHIST_DUCKDB_MEMORY_LIMIT}'")
    return con


# ── Step 0 ────────────────────────────────────────────────────────────

def init_remote_schema(client: DuckportClient):
    """Create business tables on duckport-rs (idempotent)."""
    client.init_schema(
        markets=list(KLINE_MARKETS),
        interval=KLINE_INTERVAL,
        data_sources=DATA_SOURCES,
        start_date=START_DATE,
    )
    client.verify_kline_interval(KLINE_INTERVAL)


# ── Step 1: download ──────────────────────────────────────────────────

async def _ping_binance():
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(
                url='https://fapi.binance.com/fapi/v1/ping',
                proxy=proxy, timeout=aiohttp.ClientTimeout(total=5),
            ) as response:
                if await response.text() == '{}':
                    logger.info('币安接口已连通')
                else:
                    logger.warning('币安接口响应异常，请检查网络')
        except Exception as e:
            logger.warning(f'币安接口无法连接: {e}')


def _download_market(trade_type: str, interval: str) -> GapRecord:
    """Download + convert one market, then fill the days its monthly archives lack."""
    logger.info(f'开始下载 {trade_type} 数据...')
    params = {
        'delimiter': '/',
        'prefix': get_download_prefix(trade_type, 'klines', 'daily', None, interval),
    }
    symbols = async_get_usdt_symbols(params)
    if trade_type == 'usdt_spot':
        symbols = spot_symbols_filter(symbols)
    logger.info(f'{trade_type} 交易对数量: {len(symbols)}')

    if trade_type == 'usdt_perp':
        asyncio.run(_ping_binance())

    daily_list   = async_get_daily_list(RESOURCE_PATH, symbols, trade_type, 'klines', interval)
    monthly_list = async_get_monthly_list(RESOURCE_PATH, symbols, trade_type, 'klines', interval)
    logger.info(f'{trade_type} daily={len(daily_list)}, monthly={len(monthly_list)}')

    all_list = monthly_list + daily_list
    random.shuffle(all_list)

    # Gap-fill zips cover days the monthly archive lacks; they must survive the cleanup.
    clean_old_daily_zip(
        get_local_path(RESOURCE_PATH, trade_type, 'klines', 'daily', None, interval),
        symbols, interval, keep=GapRecord(trade_type, interval).protected_files(),
    )

    error_set: set = set()
    async_download_file(all_list, error_set)
    if error_set:
        logger.warning(f'{trade_type} 下载有 {len(error_set)} 个错误（已重试）')

    _convert_to_parquet(trade_type, interval)
    return _fill_gaps(trade_type, interval)


def _fill_gaps(trade_type: str, interval: str) -> GapRecord:
    """Step 1b: fetch daily zips for missing days and rebuild the months that received any."""
    logger.info(f'--- Step 1b: 补漏 {trade_type} ---')
    con = _local_duckdb()
    try:
        record, months = fill_gaps(trade_type, interval, con)
    finally:
        con.close()
    if months:
        logger.info(f'{trade_type}: 重建 {len(months)} 个月份的 Parquet: {sorted(months)}')
        _convert_to_parquet(trade_type, interval, force_months=months)
    return record


def _convert_to_parquet(trade_type: str, interval: str, force_months=()):
    logger.info(f'转换 {trade_type} zip → Parquet ...')
    try:
        success, errors = batch_process_data(trade_type, interval, force_months=force_months)
        if errors:
            logger.warning(f'{trade_type} 转换完成，{errors} 个文件失败')
        else:
            logger.info(f'{trade_type} 转换完成，{success} 个文件成功')
    except Exception as e:
        logger.error(f'{trade_type} 转换异常: {e}')


def run_download(interval: str) -> Dict[str, GapRecord]:
    """Download every enabled market; returns ``{market: GapRecord}`` for Step 3."""
    start = datetime.now()
    records: Dict[str, GapRecord] = {}
    if 'usdt_perp' in DATA_SOURCES:
        logger.info('=== 下载期货数据 ===')
        records['usdt_perp'] = _download_market('usdt_perp', interval)
    if 'usdt_spot' in DATA_SOURCES:
        logger.info('=== 下载现货数据 ===')
        records['usdt_spot'] = _download_market('usdt_spot', interval)
    logger.info(f'下载完成，耗时 {(datetime.now() - start).seconds / 60:.1f} 分钟')
    return records


# ── Step 2: incremental clean ─────────────────────────────────────────

def _sql_path(p: str) -> str:
    return os.path.abspath(p).replace("\\", "/").replace("'", "''")


def _needs_clean(path: str) -> bool:
    marker = path + ".ok"
    if not os.path.exists(marker):
        return True
    try:
        return os.path.getmtime(path) != float(open(marker).read().strip())
    except Exception:
        return True


def _write_marker(path: str) -> None:
    with open(path + ".ok", "w") as f:
        f.write(str(os.path.getmtime(path)))


def clean_markets(markets: list) -> None:
    """
    Clean each Parquet file once (idempotent via .ok marker):
      - Remove zero-volume symbols
      - Trim rows outside each symbol's first/last candle
      - Write atomically (.tmp → os.replace)
    """
    con = duckdb.connect()
    try:
        for market in markets:
            pqt_dir = os.path.join(PARQUET_DIR, f"{market}_{KLINE_INTERVAL}")
            files   = sorted(glob(os.path.join(pqt_dir, f"{market}_*.parquet")))
            if not files:
                logger.info(f"{market}: 无 Parquet 文件，跳过清洗")
                continue

            glob_esc = _sql_path(os.path.join(pqt_dir, "*.parquet"))

            logger.info(f"{market}: 扫描全目录计算清洗元数据...")
            meta_df = con.execute(f"""
                SELECT
                    symbol,
                    min(CASE WHEN volume > 0 THEN open_time END) AS first_candle,
                    max(CASE WHEN volume > 0 THEN open_time END) AS last_candle,
                    sum(volume)                                   AS total_volume
                FROM read_parquet('{glob_esc}')
                GROUP BY symbol
            """).df()

            for col in ("first_candle", "last_candle"):
                if meta_df[col].dt.tz is not None:
                    meta_df[col] = meta_df[col].dt.tz_localize(None)

            valid_range = meta_df[meta_df["total_volume"] > 0][
                ["symbol", "first_candle", "last_candle"]
            ].copy()
            useless = set(meta_df[meta_df["total_volume"] == 0]["symbol"])

            skipped = cleaned = 0
            for fp in tqdm(files, desc=f"clean {market}"):
                if not _needs_clean(fp):
                    skipped += 1
                    continue

                df = pd.read_parquet(fp)
                if df["open_time"].dt.tz is not None:
                    df["open_time"] = df["open_time"].dt.tz_localize(None)

                df = df.merge(valid_range, on="symbol", how="left")
                if df["first_candle"].dt.tz is not None:
                    df["first_candle"] = df["first_candle"].dt.tz_localize(None)
                    df["last_candle"]  = df["last_candle"].dt.tz_localize(None)
                df = df[df["open_time"].between(df["first_candle"], df["last_candle"])]
                df = df[~df["symbol"].isin(useless)]
                df = df.drop(columns=["first_candle", "last_candle"])

                tmp = fp + ".tmp"
                try:
                    df.to_parquet(tmp, index=False)
                    os.replace(tmp, fp)
                except Exception:
                    if os.path.exists(tmp):
                        os.remove(tmp)
                    raise

                _write_marker(fp)
                cleaned += 1

            logger.info(
                f"{market}: 清洗完成 — 处理 {cleaned} 个，跳过 {skipped} 个（未变化）"
            )
    finally:
        con.close()


# ── Step 3: incremental load ──────────────────────────────────────────

def _gap_predicate(pairs: List[Tuple[str, str]]) -> str:
    """SQL selecting exactly the given (symbol, 'YYYY-MM-DD') pairs."""
    by_symbol: Dict[str, List[str]] = {}
    for symbol, day in pairs:
        by_symbol.setdefault(symbol, []).append(day)
    parts = []
    for symbol, days in sorted(by_symbol.items()):
        sym = symbol.replace("'", "''")
        day_list = ", ".join(f"DATE '{d}'" for d in sorted(set(days)))
        parts.append(f"(symbol = '{sym}' AND CAST(open_time AS DATE) IN ({day_list}))")
    return "(" + " OR ".join(parts) + ")"


def save_to_duckport(
    client: DuckportClient,
    markets: list,
    gap_records: Optional[Dict[str, GapRecord]] = None,
) -> None:
    """
    Incremental load with a 1-day redundancy buffer:

      boundary = duck_time - BOUNDARY_DAYS  (None if first run)

      file_max <  boundary  →  skip (all rows already in hot DB)
      file_max >= boundary  →  WHERE open_time >= boundary
                                the anti-join dedup handles the overlap
      boundary is None      →  full load (first run)

    Gap-filled days (``gap_records[market]``, status ``downloaded``) lie before the boundary.
    Their files are read anyway with a predicate selecting just those (symbol, day) pairs;
    once written they are marked ``loaded``. duck_time only ever moves forward.
    """
    gap_records = gap_records or {}
    for market in markets:
        existing_dt = client.read_duck_time(market)
        ed       = existing_dt.replace(tzinfo=None) if existing_dt else None
        boundary = ed - timedelta(days=BOUNDARY_DAYS) if ed else None

        record  = gap_records.get(market)
        pending = record.pending_by_month() if record else {}

        pqt_dir = os.path.join(PARQUET_DIR, f"{market}_{KLINE_INTERVAL}")
        files   = sorted(glob(os.path.join(pqt_dir, f"{market}_*.parquet")))
        if not files:
            logger.warning(f"{market}: 无 Parquet 文件，跳过导入")
            continue

        logger.info(
            f"{market}: duck_time={ed}  boundary={boundary}  files={len(files)}"
            + (f"  补漏月份={sorted(pending)}" if pending else "")
        )

        local_conn = _local_duckdb()
        new_max    = None
        total_rows = 0
        gap_rows   = 0

        try:
            for fp in files:
                fp_esc = _sql_path(fp)
                m = _PARQUET_MONTH_RE.search(os.path.basename(fp))
                gap_pairs = pending.get(m.group(1), []) if m else []

                row = local_conn.execute(
                    f"SELECT max(open_time) FROM read_parquet('{fp_esc}')"
                ).fetchone()
                if row is None or row[0] is None:
                    continue

                fm = row[0].replace(tzinfo=None)

                if boundary is not None and fm < boundary and not gap_pairs:
                    logger.debug(
                        f"skip {os.path.basename(fp)} "
                        f"(file_max={fm:%Y-%m-%d} < boundary={boundary:%Y-%m-%d})"
                    )
                    continue

                preds = []
                if boundary is not None:
                    preds.append(f"open_time >= '{boundary:%Y-%m-%d %H:%M:%S}'")
                    if gap_pairs:
                        preds.append(_gap_predicate(gap_pairs))
                where = f"WHERE {' OR '.join(preds)}" if preds else ""

                reader = local_conn.execute(f"""
                    SELECT open_time, symbol, open, high, low, close, volume,
                           quote_volume, trade_num,
                           taker_buy_base_asset_volume,
                           taker_buy_quote_asset_volume,
                           avg_price
                    FROM read_parquet('{fp_esc}')
                    {where}
                    ORDER BY open_time, symbol
                """).to_arrow_reader(batch_size=LOADHIST_BATCH_ROWS)

                file_rows = 0
                for batch in reader:
                    if batch.num_rows == 0:
                        continue

                    arrow = pa.Table.from_batches([batch])
                    client.bulk_write_kline(
                        arrow, market, KLINE_INTERVAL, "",
                        chunk_size=LOADHIST_BATCH_ROWS,
                        sync_duck_time=False,
                    )
                    file_rows += len(arrow)
                    total_rows += len(arrow)

                if gap_pairs:
                    if file_rows == 0:
                        logger.warning(
                            f"{market}  {os.path.basename(fp)}: 补漏 {len(gap_pairs)} 天"
                            f"在 Parquet 中仍无数据，保留待处理"
                        )
                    else:
                        for symbol, day in gap_pairs:
                            record.set(symbol, day, "loaded")
                        gap_rows += file_rows
                        logger.info(
                            f"{market}  {os.path.basename(fp)}: 补漏 {len(gap_pairs)} 天 "
                            f"(+{file_rows:,} rows incl. overlap)"
                        )
                        if boundary is not None and fm < boundary:
                            continue

                if file_rows == 0:
                    continue

                if new_max is None or fm > new_max:
                    new_max = fm

                logger.info(
                    f"{market}  {os.path.basename(fp)}: +{file_rows:,} rows"
                    f"  (file_max={fm:%Y-%m-%d})"
                )

        finally:
            local_conn.close()
            if record is not None:
                record.save()

        if new_max is not None and (ed is None or new_max > ed):
            client.replace_duck_time(market, new_max.strftime("%Y-%m-%d %H:%M:%S"))
            logger.info(
                f"{market}: 导入完成，共 {total_rows:,} 行，"
                f"duck_time → {new_max:%Y-%m-%d %H:%M:%S}"
            )
        elif total_rows:
            logger.info(
                f"{market}: 导入 {total_rows:,} 行（补漏 {gap_rows:,} 行），duck_time 保持 {ed}"
            )
        else:
            logger.info(f"{market}: 无新数据，duck_time 保持 {ed}")


# ── Main ──────────────────────────────────────────────────────────────

def main():
    pqt_path = os.path.abspath(PARQUET_DIR)
    logger.info("=" * 60)
    logger.info(f"loadhist — 历史数据批量导入「{pqt_path}」 (duckport-rs)")
    logger.info("=" * 60)

    client = DuckportClient(DUCKPORT_ADDR, DUCKPORT_SCHEMA, interval=KLINE_INTERVAL)
    client.ping()

    logger.info("初始化远程 schema ...")
    init_remote_schema(client)

    # Step 1 (+1b gap fill): download
    logger.info("--- Step 1: 下载 ---")
    gap_records = run_download(KLINE_INTERVAL)

    markets = get_enabled_kline_markets()
    if not markets:
        logger.info("未启用任何市场，退出")
        client.close()
        return

    # Step 2: clean
    logger.info("--- Step 2: 清洗 ---")
    clean_markets(markets)

    # Step 3: load
    logger.info("--- Step 3: 导入 ---")
    save_to_duckport(client, markets, gap_records=gap_records)

    client.close()

    logger.info("\n" + "=" * 60)
    logger.info("loadhist 完成。")
    logger.info(f"Parquet 文件已保留在：{pqt_path}")
    logger.info("如需释放磁盘空间，可执行：")
    logger.info(f"  rm -rf {pqt_path}")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
