"""
Gap filling — find whole days a symbol has no rows for and fetch Binance's daily zips for them.

Binance's monthly archives are occasionally incomplete: most USDT-M symbols from F onward lack
2022-02-26..28 and 2022-04-01..02, BNXUSDT lacks a few scattered days, and so on. The daily
archives for those days do exist. The regular download only lists monthly zips (up to the month
before last) and daily zips (from last month on), so nothing noticed the holes.

After the regular download + conversion, ``fill_gaps`` does:

  1. scan the market's Parquet for calendar days between a symbol's first and last day that have
     no rows (``find_missing_days``);
  2. ask the S3 index which of those days have a daily zip. Days without one are recorded as
     ``nodata`` (delisting / trading halt) and are not asked about again;
  3. download the daily zips into ``daily_klines/<SYMBOL>/`` and record them as ``downloaded``.

The caller then rebuilds the affected monthly Parquet files (``read_symbol_csv`` merges monthly
and daily zips of the same month) and loads exactly those (symbol, day) pairs into duckport,
after which they become ``loaded``.

State lives in ``<RESOURCE_PATH>/<market>_<interval>/gap_fill.json`` so that the gap zips are
protected from ``clean_old_daily_zip`` and ``nodata`` days are not re-probed on every run
(delete the entry from that file to probe a day again).
"""

import asyncio
import datetime as dt
import json
import os
import re
import time
from collections import defaultdict
from glob import glob
from hashlib import sha256
from typing import Dict, List, Optional, Set, Tuple

import aiohttp

from binance_ingestor.config import PARQUET_DIR, RESOURCE_PATH, create_download_semaphore
from binance_ingestor.utils.log_kit import logger
from .data_lister import get_download_prefix
from .downloader import async_download_file
from .file_manager import get_local_path
from .symbol_manager import request_session_4_list

NODATA = "nodata"          # Binance has no daily zip for the day (halt / delisting)
DOWNLOADED = "downloaded"  # daily zip on disk, Parquet rebuilt or pending rebuild, not yet in duckport
LOADED = "loaded"          # rows are in duckport

_DAY_RE = re.compile(r"-(\d{4}-\d{2}-\d{2})\.zip(?:\.CHECKSUM)?$")


class GapRecord:
    """``{symbol: {"YYYY-MM-DD": status}}`` persisted next to the market's zips."""

    def __init__(self, market: str, interval: str):
        self.market = market
        self.interval = interval
        self.path = os.path.join(RESOURCE_PATH, f"{market}_{interval}", "gap_fill.json")
        self._data: Dict[str, Dict[str, str]] = {}
        if os.path.exists(self.path):
            try:
                with open(self.path, encoding="utf-8") as f:
                    self._data = json.load(f)
            except Exception as e:
                logger.warning(f"{self.path} 无法读取，按空记录处理: {e}")
                self._data = {}

    def status(self, symbol: str, day: str) -> Optional[str]:
        return self._data.get(symbol, {}).get(day)

    def set(self, symbol: str, day: str, status: str) -> None:
        self._data.setdefault(symbol, {})[day] = status

    def entries(self, *statuses: str) -> List[Tuple[str, str]]:
        return sorted(
            (symbol, day)
            for symbol, days in self._data.items()
            for day, st in days.items()
            if st in statuses
        )

    def pending_by_month(self) -> Dict[str, List[Tuple[str, str]]]:
        """``downloaded`` entries grouped by ``YYYY-MM`` — the months whose Parquet must be rebuilt/loaded."""
        out: Dict[str, List[Tuple[str, str]]] = defaultdict(list)
        for symbol, day in self.entries(DOWNLOADED):
            out[day[:7]].append((symbol, day))
        return dict(out)

    def protected_files(self) -> Set[str]:
        """Daily zip basenames that ``clean_old_daily_zip`` must keep (with their .CHECKSUM)."""
        names: Set[str] = set()
        for symbol, day in self.entries(DOWNLOADED, LOADED):
            zip_name = f"{symbol}-{self.interval}-{day}.zip"
            names.add(zip_name)
            names.add(zip_name + ".CHECKSUM")
        return names

    def summary(self) -> str:
        counts = defaultdict(int)
        for days in self._data.values():
            for st in days.values():
                counts[st] += 1
        return ", ".join(f"{k}={v}" for k, v in sorted(counts.items())) or "empty"

    def save(self) -> None:
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        tmp = self.path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(self._data, f, indent=1, sort_keys=True)
        os.replace(tmp, self.path)


def _sql_path(p: str) -> str:
    return os.path.abspath(p).replace("\\", "/").replace("'", "''")


def find_missing_days(market: str, interval: str, con) -> Dict[str, List[str]]:
    """Calendar days between each symbol's first and last day that have no rows in the market's Parquet."""
    pqt_dir = os.path.join(PARQUET_DIR, f"{market}_{interval}")
    files = glob(os.path.join(pqt_dir, f"{market}_*.parquet"))
    if not files:
        return {}
    glob_esc = _sql_path(os.path.join(pqt_dir, f"{market}_*.parquet"))
    rows = con.execute(f"""
        WITH d AS (
            SELECT symbol, CAST(open_time AS DATE) AS day
            FROM read_parquet('{glob_esc}')
            GROUP BY 1, 2
        ),
        r AS (
            SELECT symbol FROM d GROUP BY 1
            HAVING date_diff('day', min(day), max(day)) + 1 > count(*)
        )
        SELECT d.symbol, d.day FROM d JOIN r USING (symbol) ORDER BY 1, 2
    """).fetchall()

    present: Dict[str, Set[dt.date]] = defaultdict(set)
    for symbol, day in rows:
        present[symbol].add(day)

    missing: Dict[str, List[str]] = {}
    for symbol, days in present.items():
        lo, hi = min(days), max(days)
        gaps = [
            (lo + dt.timedelta(i)).isoformat()
            for i in range((hi - lo).days + 1)
            if (lo + dt.timedelta(i)) not in days
        ]
        if gaps:
            missing[symbol] = gaps
    return missing


async def _list_daily_days(session, market, interval, symbol, sem) -> Dict[str, Tuple[str, float]]:
    """``{day: (s3 key of the .CHECKSUM, last_modified)}`` for every daily zip Binance has for ``symbol``."""
    prefix = get_download_prefix(market, 'klines', 'daily', symbol, interval)
    async with sem:
        roots = await request_session_4_list(session, {'delimiter': '/', 'prefix': prefix})
    out: Dict[str, Tuple[str, float]] = {}
    for root in roots:
        if getattr(root, 'Contents', None) is None:
            continue
        for item in root.Contents:
            key = item.Key.text
            if not key.endswith('.CHECKSUM'):
                continue
            m = _DAY_RE.search(key)
            if not m:
                continue
            struct_time = time.strptime(item.LastModified.text, '%Y-%m-%dT%H:%M:%S.%fZ')
            out[m.group(1)] = (key, time.mktime(struct_time))
    return out


async def _list_all(market, interval, symbols) -> Dict[str, Dict[str, Tuple[str, float]]]:
    sem = create_download_semaphore()
    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False)) as session:
        results = await asyncio.gather(
            *(_list_daily_days(session, market, interval, s, sem) for s in symbols)
        )
    return dict(zip(symbols, results))


def _zip_verified(zip_path: str) -> bool:
    sum_path = zip_path + ".CHECKSUM"
    if not (os.path.exists(zip_path) and os.path.exists(sum_path)):
        return False
    try:
        with open(sum_path, encoding="utf-8") as f:
            expected = f.readline().split(' ')[0].lower()
        h = sha256()
        with open(zip_path, 'rb') as f:
            h.update(f.read())
        return h.hexdigest().lower() == expected
    except Exception:
        return False


def _describe(pairs: List[Tuple[str, str]], limit: int = 12) -> str:
    by_symbol: Dict[str, List[str]] = defaultdict(list)
    for symbol, day in pairs:
        by_symbol[symbol].append(day)
    parts = [
        f"{s}: {len(d)}d {min(d)}..{max(d)}" for s, d in sorted(by_symbol.items())
    ]
    more = f" …(+{len(parts) - limit} symbols)" if len(parts) > limit else ""
    return "; ".join(parts[:limit]) + more


def fill_gaps(market: str, interval: str, con) -> Tuple[GapRecord, Set[str]]:
    """Detect holes, fetch the daily zips Binance has for them, update the record.

    Returns the record and the set of ``YYYY-MM`` months whose Parquet must be rebuilt
    (every month that still has ``downloaded`` entries, including ones left over from an
    interrupted earlier run).
    """
    record = GapRecord(market, interval)
    t0 = time.time()
    missing = find_missing_days(market, interval, con)
    total_missing = sum(len(v) for v in missing.values())
    logger.info(
        f"{market}: 扫描 Parquet 发现 {len(missing)} 个 symbol 共 {total_missing} 个缺失整天"
        f"（{time.time() - t0:.0f}s）"
    )

    todo: Dict[str, List[str]] = {}
    known_nodata = redo = 0
    for symbol, days in missing.items():
        for day in days:
            st = record.status(symbol, day)
            if st == NODATA:
                known_nodata += 1
                continue
            if st in (DOWNLOADED, LOADED):
                # zip is on disk but the Parquet still lacks the day → rebuild and reload it
                record.set(symbol, day, DOWNLOADED)
                redo += 1
                continue
            todo.setdefault(symbol, []).append(day)

    n_todo = sum(len(v) for v in todo.values())
    if known_nodata or redo:
        logger.info(f"{market}: 已知源站无数据 {known_nodata} 天，需重新转换/导入 {redo} 天")

    if todo:
        logger.info(f"{market}: 向 S3 索引查询 {len(todo)} 个 symbol 的日包（{n_todo} 天）...")
        listings = asyncio.run(_list_all(market, interval, sorted(todo)))

        params = []
        nodata: List[Tuple[str, str]] = []
        for symbol, days in todo.items():
            avail = listings.get(symbol, {})
            local_path = get_local_path(RESOURCE_PATH, market, 'klines', 'daily', symbol, interval)
            for day in days:
                if day in avail:
                    key, last_modified = avail[day]
                    params.append({
                        'key': key, 'last_modified': last_modified,
                        'local_path': local_path, 'interval': interval,
                    })
                else:
                    nodata.append((symbol, day))

        for symbol, day in nodata:
            record.set(symbol, day, NODATA)
        if nodata:
            logger.info(f"{market}: 源站无日包（停牌/下架）{len(nodata)} 天 — {_describe(nodata)}")

        if params:
            fillable = [(p['key'].split('/')[-3], _DAY_RE.search(p['key']).group(1)) for p in params]
            logger.info(f"{market}: 下载补漏日包 {len(params)} 天 — {_describe(fillable)}")
            error_set: set = set()
            async_download_file(params, error_set)
            if error_set:
                logger.warning(f"{market}: 补漏下载有 {len(error_set)} 个错误")
            failed = 0
            for p, (symbol, day) in zip(params, fillable):
                zip_path = os.path.join(p['local_path'], os.path.basename(p['key'])[:-9])
                if _zip_verified(zip_path):
                    record.set(symbol, day, DOWNLOADED)
                else:
                    failed += 1
                    for path in (zip_path, zip_path + ".CHECKSUM"):
                        if os.path.exists(path):
                            os.remove(path)
            if failed:
                logger.warning(f"{market}: {failed} 个补漏日包下载失败或校验不通过，下次运行会重试")

    record.save()
    months = {day[:7] for _, day in record.entries(DOWNLOADED)}
    logger.info(f"{market}: 补漏记录 {record.summary()}；需重建 Parquet 的月份: {sorted(months) or '无'}")
    return record, months
