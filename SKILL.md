# Spot Snapshot Scalp Skill（Binance.US，30~60 分钟，高准确度模式）
> 适用：在 **PowerShell 5.1** 里运行 `data.py` 生成现货快照 `snapshot_<SYMBOL>_*.json`，然后只读一次 JSON 做 30~60 分钟决策（LONG/SHORT/WAIT）。  
> 目标：优先提高 long/short 准度（目标 70%+），允许覆盖率下降，但避免机械性全 WAIT。

---

## 硬规则（必须遵守）
1. 禁止写临时 debug 脚本；禁止多次读取同一 JSON。
2. 必须先运行 `.\scripts\data.py` 生成该 SYMBOL 的最新 snapshot。
3. 分析阶段只允许读取一次最新 snapshot JSON（全文一次）。
4. 输出报告必须是 UTF-8，PowerShell 5.1 禁止用 `>`/`>>` 保存报告。
5. 若数据质量不足必须 WAIT；若 `errors_count>0`，置信度上限 50。
6. 所有输出统一写到 `.\logs\`。
7. 若 `diagnostics.self_check.status=FAIL`，本轮直接终止并先修复报错根因。
8. 本流程用于实盘实时分析，禁止调用 `scripts/backtest_random_nodes.py` 或历史时间戳回放。
9. 报告必须使用 ASCII 段落标签（如 `[FINAL_DECISION]`），避免终端中文乱码影响可审计性。
10. 禁止黑盒结论：每条结论后必须跟字段证据（字段路径 + 数值）。

---

## 变量区（只改这里）
```powershell
$SYMBOL  = "BTCUSDT"
$PROFILE = "MED"
$DATA_PY = ".\scripts\data.py"
$GUARD_PY = ".\scripts\decision_guard.py"
$LOG_DIR = ".\logs"
```

---

## Step 0) PowerShell 5.1 编码环境（逐条执行）
```powershell
chcp 65001
$OutputEncoding = New-Object System.Text.UTF8Encoding $false
[Console]::OutputEncoding = New-Object System.Text.UTF8Encoding $false
$env:PYTHONIOENCODING = "utf-8"
$env:PYTHONUTF8 = "1"
$PSDefaultParameterValues['Out-File:Encoding'] = 'utf8'
$PSDefaultParameterValues['Set-Content:Encoding'] = 'utf8'
$PSDefaultParameterValues['Add-Content:Encoding'] = 'utf8'
if (!(Test-Path $LOG_DIR)) { New-Item -ItemType Directory -Path $LOG_DIR -Force | Out-Null }
```

---

## Step 0.5) 会话自检（防重复报错）
每次新会话先跑一次：
```powershell
python .\scripts\self_check.py --symbol $SYMBOL
```
- 若输出 `status=FAIL`：先按 `logs\self_check_*.json` 的 `issues.fail` 修复，再继续。
- 若输出 `status=WARN`：可继续，但必须在报告中写明风险来源。

---

## Step 1) 抓取快照（Binance.US）
### PROFILE=MED（默认）
- intervals：`1m 3m 5m 15m 30m 1h`
- kline_limit：`360`
- hist_period：`15m`
- hist_limit：`192`
- depth：`150`
- include-exchange-info：开启
- include-trades：开启

```powershell
$RUN_ID = [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds()
$OUT_JSON = Join-Path $LOG_DIR ("snapshot_{0}_{1}.json" -f $SYMBOL, $RUN_ID)
python $DATA_PY --symbol $SYMBOL --intervals 1m 3m 5m 15m 30m 1h --kline-limit 360 --hist-period 15m --hist-limit 192 --depth 150 --include-exchange-info --include-trades --trades-limit 1000 --out $OUT_JSON --strict
```
strict 失败只允许降级一次（去掉 `--strict`）。

---

## Step 2) 定位最新 snapshot
```powershell
Get-ChildItem -File -Path $LOG_DIR -Filter "snapshot_$($SYMBOL)_*.json" | Sort-Object LastWriteTime -Descending | Select-Object -First 1 FullName,Name,LastWriteTime
```

---

## Step 3) 只读一次 JSON 并提取关键字段
必须提取：
- `meta.server_time_ms / server_time / generated_at`
- `errors`
- `diagnostics.self_check.status / failed_critical / failed`
- `diagnostics.error_diagnostics`
- `market.ticker_24h / ticker_1h / ticker_4h / book_ticker / avg_price`
- `order_book.bids / asks`
- `features.order_book.spread_bps / imbalance`
- `features.book_ticker.spread_bps / imbalance_top`
- `flow.trade_features.buy_sell_ratio / imbalance_qty / recent_count / agg_count`
- `features.ticker_window.h1/h4.price_change_pct / range_pct`
- `features.by_interval["5m"/"15m"/"30m"/"1h"]`
- `candles.regular["5m"/"15m"/"30m"/"1h"]`

---

## Step 4) 数据质量门槛（高准确度模式）
- K1 `lastPrice` 非空
- K2 orderbook 非空且 spread/imbalance 非空
- K3 candles：`5m>=120` 且 `15m>=96` 且（`30m>=72` 或 `1h>=48`）
- K4 trade_flow：`recent_count + agg_count >= 80`
- K5 ticker_window：`h1/h4 price_change_pct` 都存在
- K6 book_ticker spread 存在
- K7 freshness：快照到分析时刻 <= 20 分钟

判定：
- HIGH：缺失 <=2 且 errors=0
- MED：缺失 3~4 或 errors>0
- LOW：缺失 >=5 或 K1/K3/K7 失败

强制规则：
- LOW => WAIT 且置信度<=35
- errors_count>0 => 置信度<=50
- `diagnostics.self_check.status=FAIL` => WAIT 且必须先修复后再重跑

---

## Step 5) 高准确度模式分析（30~60 分钟）
### 趋势层
- 5m：EMA20/EMA50 + RSI14
- 15m：EMA20/EMA50 + RSI14（主方向）
- 1h：方向过滤（与 15m 冲突则扣分）

### 微观层
- orderbook spread/imbalance
- bookTicker spread（盘口是否骤然变薄）
- trade flow：`buy_sell_ratio`、`imbalance_qty`

### 波动与节奏
- 15m ATR
- h1/h4 range 与 price_change_pct（波动是否足够）

### 行动准则（高准确度）
- LONG：15m 偏多 + 5m 同向 + flow 偏多 + 1h 不明显反向
- SHORT：15m 偏空 + 5m 同向 + flow 偏空 + 1h 不明显反向
- WAIT：仅在多周期冲突明显或流向失真明显时使用
- 约束：若 LONG/SHORT 核心条件齐全且置信度 >= 68，不应直接给 WAIT

### 硬门槛（必须）
- `LONG` 仅在 LONG_CORE 五项全部为 true 时允许；否则必须 WAIT。
- `SHORT` 仅在 SHORT_CORE 五项全部为 true 时允许；否则必须 WAIT。
- 若 `action=SHORT` 且 `rsi_1h<=34` 且 `ret_1h_pct>=0`，必须 WAIT（short_rebound_risk）。
- 若 `flow_ratio<=0.97` 但 `imbalance>=0.08`，或 `flow_ratio>=1.03` 但 `imbalance<=-0.08`，必须 WAIT（flow_depth_divergence）。
- 若 `abs(entry-book_mid)/max(1e-9,atr15) > 0.25`，必须 WAIT（entry_price_mismatch）。

---

## Step 6) 风控与退出
- ATR 可用时：
  - LONG：SL=`price-1.2*ATR`，TP1=`price+0.8*ATR`，TP2=`price+1.6*ATR`
  - SHORT：SL=`price+1.2*ATR`，TP1=`price-0.8*ATR`，TP2=`price-1.6*ATR`
- ATR 缺失：只给条件止损/止盈，不给具体价格
- 超时退出：30~60 分钟未同向扩展（价格+flow 至少一项不确认）=> 降仓或平仓
- 仓位固定 5%，杠杆提示 <=3x（若使用保证金）

---

## Step 7) 输出模板（固定，audit_full_v1）
第一行（固定）：
```text
snapshot_id=<...> | snapshot_server_time_utc=<...> | analysis_time_utc=<...> | analysis_time_local=<...> | horizon=30m~1h | mode=precision | data_quality=<HIGH/MED/LOW>
```

正文模块（按顺序，不得缺失）：
- `[SNAPSHOT_META]`
  - symbol / server_time / generated_at / snapshot_age_sec
  - source_endpoints（本次实际用到）
- `[DATA_QUALITY_GATE]`
  - K1~K7 逐条输出：`Kx=<PASS|FAIL> | value=... | rule=...`
  - 输出 `missing_key_modules=[...]`
- `[FEATURE_TABLE]`
  - lastPrice / ret_1h_pct / ret_4h_pct / range_1h_pct / range_4h_pct
  - spread_bps / imbalance / flow_buy_sell_ratio / flow_imbalance_qty
  - atr15 / rsi_5m / rsi_15m / rsi_1h
- `[MULTI_TIMEFRAME]`
  - 5m/15m/30m/1h 各自输出：EMA20 / EMA50 / RSI / trend_state
  - 输出是否同向：`mtf_alignment=<aligned|partial|conflict>`
- `[FLOW_AND_ORDERBOOK]`
  - recent_count / agg_count / buy_qty / sell_qty / buy_sell_ratio
  - orderbook_top_imbalance / spread_bps / liquidity_comment
- `[SCORING_BREAKDOWN]`
  - long_score / short_score / conflict_penalty / liquidity_penalty
  - final_confidence（给出计算口径）
- `[DECISION_TRACE]`
  - LONG_CORE 条件逐条 true/false
  - SHORT_CORE 条件逐条 true/false
  - 输出 `core_complete=<true|false>` 与 `hard_blockers=[...]`
  - WAIT 若出现，必须写明唯一主因和次因
- `[RISK_PLAN]`
  - entry_rule / entry_source / entry / entry_mid_diff / SL / TP1 / TP2 / position_size / leverage_cap
- `[INVALIDATION_AND_TIMEOUT]`
  - 至少2条 invalidation
  - timeout_exit（30~60m）
- `[COUNTER_SCENARIO]`
  - 给出反方向成立所需条件（至少2条）
- `[FINAL_DECISION]`
  - action / confidence / data_quality / execution_note

质量要求：
- 不能只写“看多/看空”，必须给字段证据与阈值口径。
- 每个模块至少 2 个“字段=数值”条目。
- 若存在错误或字段缺失，必须写入对应模块，不能省略。

---

## Step 8) 保存报告（UTF-8）
```powershell
$outFile = Join-Path $LOG_DIR ("analysis_{0}_{1}.txt" -f $SYMBOL, $meta.server_time_ms)
$report | Set-Content -Encoding utf8 -Path $outFile
```
说明：
- 统一使用 ASCII 段落标签，保证在 PowerShell 5.1 下可读。
- 如终端中文显示异常，不影响报告主体可审计字段（字段名与数值必须可读）。

## Step 8.5) 决策守卫（必须）
写完报告后必须执行：
```powershell
python $GUARD_PY --analysis $outFile --inplace
```
判定：
- `status=PASS`：保持原结论。
- `status=ENFORCED_WAIT`：守卫已将报告 `FINAL_DECISION` 强制改为 WAIT（说明原结论被拦截）。
- `status=FAIL`：本轮中止，先修复格式/字段问题再重跑。

---

## Step 9) 结束确认（两行）
```text
AGENT_PATH = <当前工作目录>
ANALYSIS_PATH = <输出文件完整路径>
```


