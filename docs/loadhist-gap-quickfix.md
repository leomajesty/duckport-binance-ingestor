# loadhist 补漏快速修复操作手册

适用场景：duckport 库里的 K 线表存在"整天缺失"的空洞（币安月归档缺天导致），而完整跑一次 `duckport loadhist <实例>` 代价太高。本文的做法只执行 binance-ingestor 0.4.0 新增的补漏步骤和补漏导入，跳过全量下载与转换，实时采集全程不停。

pro.studio 已于 2026-11-02 按此流程执行，耗时 3.2 分钟。背景与审计结果见 http://192.168.9.20:8088/duckport/binance-ingestor-loadhist-gap-audit-2026-10-30.html ，代码改动见 https://github.com/sjmc-quant/duckport-binance-ingestor/pull/2 。

## 1. 原理

0.4.0 的 loadhist 在下载转换之后多了一步 Step 1b（`binance_ingestor/hist/gap_filler.py`）：

1. 用 DuckDB 扫描本地 Parquet，找出每个 symbol 首末日之间没有任何行的整天；
2. 向币安 S3 索引查询这些天有没有日包，没有的记为 `nodata`（停牌、下架），以后不再查；
3. 下载存在的日包到 `daily_klines/<SYMBOL>/`，校验 sha256，记为 `downloaded`；
4. 重建收到日包的那几个月份的 Parquet；
5. Step 3 导入时，对这些月份只读取记录中的 `(symbol, day)` 行，通过 anti-join 写入，写完记为 `loaded`。

状态文件：`<RESOURCE_PATH>/<market>_<interval>/gap_fill.json`。

快速修复就是把这几步从 loadhist 里单独拿出来执行，并把写入改走一张私有 staging 表 `_staging_gapfill_<market>_<interval>`，这样不会和实时采集每分钟 TRUNCATE 的 `_staging_<market>_<interval>` 冲突，采集不用停。

对库的影响：只有 INSERT，没有 DELETE / UPDATE / TRUNCATE 业务表；anti-join 以 `(open_time, symbol)` 去重，已有行不动；duck_time 不变。新行追加为表尾的几个 row group，zone map 按块独立剪枝，对查询性能无可感知影响。

## 2. 前提

| 项 | 要求 | pro.studio 实际值 |
|---|---|---|
| 本地已有的 hist / pqt 目录 | 上一次 loadhist 留下的月包和 Parquet 必须还在，补漏靠它们定位空洞和重建月份 | `/data/duckport/hist`、`/data/duckport/pqt` |
| 实例 config.env 中的 `RESOURCE_PATH` / `PARQUET_DIR` | 必须指向上面的目录。缺省值是 `~/.duckport/data`，若那里不存在，任何 loadhist 都会从 START_DATE 重下全量 | 2026-11-02 已补入 `/opt/duckport/ingestors/bn1m/config.env` |
| 插件代码 ≥ 0.4.0 | 实例 venv 里装的是 0.4.0 可以直接用；否则用仓库 main 的 checkout 加 `PYTHONPATH` | 当时 venv 是 0.3.0，用了 `/root/work/duckport-binance-ingestor` 的 checkout |
| 能访问 data.binance.vision 与 S3 索引 | 走 config.env 的 `PROXY_URL` | `http://localhost:1082` |
| duckport-server 在跑，本机可连 | 脚本从 server.env 继承 `DUCKPORT_LISTEN_ADDR` | `0.0.0.0:8815` |
| 磁盘 | 重建一个 1m 月份的 Parquet 临时需要约 1 GB | 充足 |
| 不要与 loadhist 或 `upgrade --data` 同时跑 | 都会碰 Parquet 目录 | |

## 3. 脚本

保存为 `/root/work/gapfill_once.py`（Mac 上路径按需调整）。

```python
"""One-off gap backfill, bypassing the full loadhist run.

Runs only Step 1b (gap detection + daily-zip download + Parquet rebuild) and the gap part of
Step 3 against the existing hist/pqt directories, and writes through a PRIVATE staging table so
the live ingestor (which truncates _staging_<market>_<interval> every minute) keeps running.
"""
import time

from binance_ingestor import loadhist as lh
from binance_ingestor.config import DATA_SOURCES, KLINE_INTERVAL, PARQUET_DIR, RESOURCE_PATH
from binance_ingestor.duckport_client import DuckportClient
from binance_ingestor.hist import fill_gaps
from binance_ingestor.utils.log_kit import logger


class GapClient(DuckportClient):
    """bulk_write_kline through _staging_gapfill_<target>, never touching the ingestor's staging table."""

    def bulk_write_kline(self, arrow_table, market, interval, duck_time_str,
                         chunk_size=200_000, *, sync_duck_time=True):
        target = f"{self.schema}.{market}_{interval}"
        staging = f"_staging_gapfill_{market}_{interval}"
        self.execute(f"CREATE TABLE IF NOT EXISTS {staging} AS SELECT * FROM {target} LIMIT 0")
        arrow_table = self._prepare(arrow_table)
        total = len(arrow_table)
        n = self._append_max_rows_per_message(arrow_table, chunk_size)
        for off in range(0, total, n):
            chunk = arrow_table.slice(off, min(n, total - off))
            merge = self._merge_sql(target, staging, chunk)
            if merge is None:
                continue
            self.execute(f"TRUNCATE {staging}")
            self.append("main", staging, chunk)
            tx = self.execute_transaction([merge, f"TRUNCATE {staging}"])
            logger.info(f"gapfill {market}: chunk {off}-{off + len(chunk)}/{total} inserted={tx.get('rows_affected')}")
        return {"total_rows": total}


def main():
    logger.info(f"RESOURCE_PATH={RESOURCE_PATH}  PARQUET_DIR={PARQUET_DIR}")
    markets = [m for m in ("usdt_perp", "usdt_spot") if m in DATA_SOURCES]
    t0 = time.time()

    records = {}
    for market in markets:
        con = lh._local_duckdb()
        try:
            record, months = fill_gaps(market, KLINE_INTERVAL, con)
        finally:
            con.close()
        if months:
            logger.info(f"{market}: 重建 {sorted(months)}")
            lh._convert_to_parquet(market, KLINE_INTERVAL, force_months=months)
        records[market] = record
    logger.info(f"Step 1b 完成，耗时 {(time.time() - t0) / 60:.1f} 分钟")

    lh.clean_markets(markets)

    client = GapClient(lh.DUCKPORT_ADDR, "data", interval=KLINE_INTERVAL)
    client.ping()
    before = {m: client.read_duck_time(m) for m in markets}
    lh.save_to_duckport(client, markets, gap_records=records)
    after = {m: client.read_duck_time(m) for m in markets}
    for m in markets:
        client.execute(f"DROP TABLE IF EXISTS _staging_gapfill_{m}_{KLINE_INTERVAL}")
    client.close()

    for m in markets:
        logger.info(f"{m}: duck_time {before[m]} -> {after[m]}; 记录 {records[m].summary()}")
    logger.info(f"全部完成，耗时 {(time.time() - t0) / 60:.1f} 分钟")


if __name__ == "__main__":
    main()
```

## 4. 执行步骤

以 pro.studio 的 bn1m 为例，全部以 root 执行。

**4.1 确认路径配置**

```bash
grep -E "^(RESOURCE_PATH|PARQUET_DIR|PROXY_URL|DATA_SOURCES|KLINE_INTERVAL)=" /opt/duckport/ingestors/bn1m/config.env
ls /data/duckport/hist/usdt_perp_1m/monthly_klines | wc -l     # 应有几百个 symbol 目录
ls /data/duckport/pqt/usdt_perp_1m | grep -c parquet$           # 应有几十个月份文件
df -h /data
```

如果 config.env 里没有 `RESOURCE_PATH` / `PARQUET_DIR`，先补上，否则不要往下走：

```bash
cp -n /opt/duckport/ingestors/bn1m/config.env /opt/duckport/ingestors/bn1m/config.env.bak-$(date +%Y%m%d)
cat >> /opt/duckport/ingestors/bn1m/config.env <<'EOF'

# loadhist 数据目录（不设则默认 ~/.duckport/data 会重下全量）
RESOURCE_PATH=/data/duckport/hist
PARQUET_DIR=/data/duckport/pqt
EOF
```

运行中的采集进程不读这两项，不需要重启。

**4.2 确认代码版本**

```bash
/opt/duckport/ingestors/bn1m/.venv/bin/python -c "import importlib.metadata as m; print(m.version('binance-ingestor'))"
```

输出 ≥ 0.4.0 则 4.3 中不需要 `PYTHONPATH`。否则确保有一份 main 的 checkout：

```bash
cd /root/work/duckport-binance-ingestor && git fetch -q origin && git checkout -q main && git pull -q --ff-only && grep version pyproject.toml
```

**4.3 后台运行**

```bash
cd /root/work
PYTHONPATH=/root/work/duckport-binance-ingestor \
INGESTOR_ENV_FILE=/opt/duckport/ingestors/bn1m/config.env \
LOADHIST_DUCKDB_THREADS=8 LOADHIST_DUCKDB_MEMORY_LIMIT=8GB \
nohup /opt/duckport/ingestors/bn1m/.venv/bin/python -u /root/work/gapfill_once.py \
  > /root/work/gapfill_once.log 2>&1 &
echo $!
```

`LOADHIST_DUCKDB_THREADS` / `LOADHIST_DUCKDB_MEMORY_LIMIT` 只影响本脚本内的本地 DuckDB，按机器大小调整；小内存机器用 `2` 和 `1GB`。

**4.4 观察进度**

```bash
grep -v "^\s*[0-9]*%\|it/s\]\|bulk_write_kline: .* chunk\|转换失败: No objects\|^🔵 跳过 " /root/work/gapfill_once.log | tail -30
```

关键日志依次是：

```
usdt_perp: 扫描 Parquet 发现 52 个 symbol 共 323 个缺失整天（3s）
usdt_perp: 向 S3 索引查询 52 个 symbol 的日包（323 天）...
usdt_perp: 源站无日包（停牌/下架）5 天 — ICPUSDT: 5d 2022-09-22..2022-09-26
usdt_perp: 下载补漏日包 318 天 — ...
usdt_perp: 补漏记录 downloaded=318, nodata=5；需重建 Parquet 的月份: ['2022-02', ...]
usdt_perp  usdt_perp_2022-02_1M.parquet: 补漏 144 天 (+207,360 rows incl. overlap)
usdt_perp: 导入 457,920 行（补漏 457,920 行），duck_time 保持 2026-11-02 03:39:00
全部完成，耗时 3.2 分钟
```

`转换失败: No objects to concatenate` 出现在没有任何 zip 的未来月份（如 2026-07 到 2026-11），是既有行为，可忽略；只要失败的月份不在"需重建"列表里就没问题。

## 5. 核对

**5.1 库内空洞**

保存为 `/root/work/verify_gaps.py`，用实例 venv 运行：

```python
import sys
sys.path.insert(0, "/root/work/duckport-rs/scripts")
from duckport_flight import Duckport
d = Duckport("127.0.0.1:8815")
Q = """
WITH d AS (SELECT symbol, CAST(open_time AS DATE) AS day FROM data.TABLE
           WHERE open_time < TIMESTAMP 'CUTOFF' GROUP BY 1,2),
r AS (SELECT symbol, min(day) lo, max(day) hi, count(*) ndays FROM d GROUP BY 1)
SELECT count(*) symbols, sum(date_diff('day', lo, hi) + 1 - ndays) missing_days,
       count(*) FILTER (WHERE date_diff('day', lo, hi) + 1 > ndays) symbols_with_gaps FROM r"""
for t in ("usdt_perp_1m", "usdt_spot_1m"):
    print(t, d.query(Q.replace("TABLE", t).replace("CUTOFF", "2026-06-13")))
print(d.query("SELECT table_name, duck_time FROM data.watermark ORDER BY 1"))
print(d.query("SELECT table_name FROM information_schema.tables WHERE table_name LIKE '%staging%' ORDER BY 1"))
```

`CUTOFF` 取上一次 loadhist 的结束日。期望：`missing_days` 只剩 `gap_fill.json` 里 `nodata` 的天数；duck_time 与运行前一致；没有 `_staging_gapfill_*` 残留。

注意 `duckport_flight.Duckport.query()` 会把任何 Flight 错误吞成空列表，SQL 写错时表现为"返回 []"。调试时直接用 `d.client.do_get(ticket)`。

**5.2 状态文件**

```bash
python3 -c "import json; d=json.load(open('/data/duckport/hist/usdt_perp_1m/gap_fill.json')); from collections import Counter; print(Counter(s for v in d.values() for s in v.values()))"
```

期望全部为 `loaded` 或 `nodata`，没有 `downloaded`。若有残留的 `downloaded`，说明导入阶段中断，重跑脚本即可，重跑是幂等的。

**5.3 采集健康**

```bash
duckport status
```

## 6. 补回来的数据长什么样

- 币安日包里有真实成交的天：正常 K 线。pro.studio 这次绝大多数属于此类，包括原先被误判为停牌的 BNXUSDT 合约 2023-02-01 至 21。
- 结算 / 停牌期的天：币安日包提供的是成交量为 0、价格不变的占位 K 线（如 TLMUSDT 2023-03、ICPUSDT 2022-09-01 至 21）。loadhist 的 `clean_markets` 只修剪一个 symbol 两头的零成交量 K 线和从未交易过的 symbol，夹在两段真实交易之间的零成交量区间按设计保留；实时采集在停牌期也写同样的 K 线。若不想要，按 symbol 加时间范围加 `volume = 0` 单独 DELETE。
- S3 索引里没有日包的天记为 `nodata`，不会再查。若币安日后补发，从 `gap_fill.json` 删掉对应条目再跑一次。

## 7. 回退

脚本不删不改任何已有行。若要撤销本次写入，按 `gap_fill.json` 里 `loaded` 的 `(symbol, day)` 对 DELETE 即可，例如：

```sql
DELETE FROM data.usdt_perp_1m
WHERE symbol = 'SOLUSDT' AND CAST(open_time AS DATE) IN (DATE '2022-02-26', DATE '2022-02-27', DATE '2022-02-28');
```

然后把这些条目从 `gap_fill.json` 删除，并删除对应的 `daily_klines/<SYMBOL>/<SYMBOL>-1m-<day>.zip*`，否则下次 loadhist 会再补回来。

## 8. 与正常 loadhist 的关系

PR #2 合并后，`duckport update <实例>` 装上 0.4.0，以后每次 `duckport loadhist` 都自带 Step 1b，本文的脚本只在"不想跑完整 loadhist"时使用。两者共用 `gap_fill.json` 与下载好的日包，本文跑过之后再跑 loadhist，补漏步骤会发现无事可做。
