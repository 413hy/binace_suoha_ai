# Futures Snapshot Scalp Skill（短线 2~4 小时）
> 适用：在 **PowerShell 5.1** 里运行 `data.py` 生成合约快照 `snapshot_<SYMBOL>_*.json`，然后只读一次 JSON 做 2~4 小时短线决策（LONG/SHORT/WAIT），并把报告保存为 UTF-8 文本。  
> 目标：**防误判**（先做数据充分性门槛 + 多周期一致性 + 微观确认 + 拥挤风险），宁可保守。

---

## 硬规则（必须遵守）
1. **禁止写临时 debug 脚本**；禁止反复探测结构；禁止多次读取同一 JSON。
2. 必须先运行 `data.py` 生成 **该 SYMBOL 的 snapshot_*.json（全量）**。
3. 分析阶段：只允许读取一次最新 `snapshot_<SYMBOL>_*.json`（全文一次）；不要读 `_agent.txt`；不要重复读 JSON。
4. 输出必须保存为 `analysis_<SYMBOL>_<server_time_ms>.txt`，**UTF-8**。PowerShell 5.1 下**禁止**用 `>` / `>>` 重定向保存（常导致中文 `????`）。必须用 `Set-Content/Out-File` 且显式 `-Encoding utf8`。
5. 若数据质量不足（见 Step 5）必须 **WAIT**；宁可保守也不要猜。若 `errors_count>0`，则 **置信度<=50**。

---

## 变量区（只改这里）
```powershell
$SYMBOL  = "DOGEUSDT"   # 例如 DOGEUSDT / SOMIUSDT / BTCUSDT
$PROFILE = "MED"       # MED=更全但还算快（推荐短线）；FULL=更慢更重
```

---

## Step 0) PowerShell 5.1 编码环境（逐条执行）
> 目的：让 **终端输出 + 写文件** 都是 UTF-8，减少 `????`。
```powershell
chcp 65001
$OutputEncoding = New-Object System.Text.UTF8Encoding $false
[Console]::OutputEncoding = New-Object System.Text.UTF8Encoding $false
$env:PYTHONIOENCODING = "utf-8"
$PSDefaultParameterValues['Out-File:Encoding'] = 'utf8'
$PSDefaultParameterValues['Set-Content:Encoding'] = 'utf8'
$PSDefaultParameterValues['Add-Content:Encoding'] = 'utf8'
```

---

## Step 1) 运行 data.py 生成快照（按 PROFILE）
> 仅允许：`ls` 看一眼是否存在 `data.py`。不要写探测脚本。

### PROFILE=MED（推荐：更全且适合短线）
- intervals：`3m 5m 15m 1h 4h 1d`（默认不含 1m，避免噪声/限流/脚本 interval 参数错误）
- kline_limit：`500`
- hist_period：`30m`
- hist_limit：`240`（约 5 天 30m 统计）
- depth：`100`
- include-exchange-info：开启
- include-trades：开启（trades_limit 1000）

严格执行（**先 strict，失败只允许降级一次**）：
```powershell
python data.py --symbol $SYMBOL --intervals 3m 5m 15m 1h 4h 1d --kline-limit 500 --hist-period 30m --hist-limit 240 --depth 100 --include-exchange-info --include-trades --trades-limit 1000 --strict
```
若返回码非 0：只允许降级一次（去掉 `--strict`）：
```powershell
python data.py --symbol $SYMBOL --intervals 3m 5m 15m 1h 4h 1d --kline-limit 500 --hist-period 30m --hist-limit 240 --depth 100 --include-exchange-info --include-trades --trades-limit 1000
```
若仍非 0：停止，打印错误，不要继续分析。

### PROFILE=FULL（更慢更重：只有 MED 数据质量不足才用）
- intervals：`1m 3m 5m 15m 1h 4h 1d`（含 1m）
- kline_limit：`800`
- hist_period：`15m`
- hist_limit：`480`
- depth：`200`
- include-exchange-info：开启
- include-trades：开启（trades_limit 1000）

执行（strict 失败只允许降级一次）：
```powershell
python data.py --symbol $SYMBOL --intervals 1m 3m 5m 15m 1h 4h 1d --kline-limit 800 --hist-period 15m --hist-limit 480 --depth 200 --include-exchange-info --include-trades --trades-limit 1000 --strict
```
降级一次（去掉 `--strict`）后仍失败则停止。

---

## Step 2) 找到最新 snapshot JSON（必须）
必须找到当前目录最新的：
- `snapshot_<SYMBOL>_*.json`

若找不到：停止并列出当前目录所有 `snapshot_*.json` 文件名。

推荐命令：
```powershell
Get-ChildItem -File -Filter "snapshot_$($SYMBOL)_*.json" | Sort-Object LastWriteTime -Descending | Select-Object -First 1 FullName,Name,LastWriteTime
```
把选中的路径记为：`$SNAP_PATH`

---

## Step 3) 只读取一次 JSON（全文一次）
只允许一次：
- `Get-Content -Raw -Encoding utf8 $SNAP_PATH`
- 解析为对象（一次），然后只在内存里使用。

必须提取以下字段路径（缺失记为 NULL/空）：
- `meta.server_time_ms`
- `meta.server_time`
- `meta.generated_at`
- `errors`（字典；`errors_count = 键数量`）
- `market.ticker_24h.lastPrice`
- `market.premium_index.markPrice`
- `market.premium_index.lastFundingRate`
- `market.premium_index.nextFundingTime`
- `funding.current_rate`（若存在）
- `funding.annualized_pct`（若存在）
- `funding.history`（list）
- `open_interest.current`（dict）
- `open_interest.history`（list）
- `open_interest.change_from_prev_pct`
- `flow.taker_buy_sell_volume`（list）
- `sentiment.global_long_short_account_ratio`（list）
- `sentiment.top_long_short_account_ratio`（list）
- `sentiment.top_long_short_position_ratio`（list）
- `basis`（list）
- `order_book.bids` / `order_book.asks`（list）
- `features.last_close / features.rsi14_last / features.atr14_last / features.ema20_last / features.ema50_last`
- `features.order_book.spread_bps / features.order_book.imbalance`
- `candles.regular["15m"]`（list[dict]）
- `candles.regular["1h"]`（list[dict]）
- `candles.regular["4h"]`（list[dict]）
- （若 intervals 含 3m/5m/1m，也同时取对应 `candles.regular[...]`）
可选：
- `trades`（list）

---

## Step 4) 慎重分析流程（不能跳）
A) 数据质量检查（Step 5）→ 不达标直接 **WAIT**  
B) 多周期一致性：15m + 1h + 4h 分别判断趋势/动能，再看是否一致  
C) 微观确认：盘口(imbalance/spread) + taker + long/short ratio  
D) 拥挤与风险：funding（当前+最近8条）+ basis（最近8条）  
E) 交叉验证：若冲突≥2项，下调置信度并倾向 WAIT  
F) 最后输出：方向 + 触发式入场 + 止损止盈（ATR 可用才给数值）

---

## Step 5) 数据充分性门槛（强制防误判）
关键模块门槛（缺任意项要降级）：
- **K1 price**：`market.premium_index.markPrice` 或 `market.ticker_24h.lastPrice` 至少一个非 NULL
- **K2 orderbook**：`order_book.bids/asks` 非空 且 `features.order_book.spread_bps` & `features.order_book.imbalance` 非 NULL
- **K3 candles**：`15m>=120` 且 `1h>=72` 且 `4h>=72`
- **K4 funding**：`funding.history>=50` 且（`funding.current_rate` 或 `premium_index.lastFundingRate`）非 NULL
- **K5 oi**：`open_interest.history>=24` 或 `open_interest.change_from_prev_pct` 非 NULL
- **K6 taker**：`flow.taker_buy_sell_volume>=24`
- **K7 sentiment**：`global_long_short_account_ratio>=24`
- **K8 errors**：`errors_count=0` 最佳；若 >0 必须降置信度上限

data_quality 判定：
- **HIGH**：errors_count=0 且 K1~K7 全部满足
- **MED**：errors_count=0 且缺 1~2 项；或 errors_count>0 但关键模块都在
- **LOW**：缺>=3项；或 price 缺失；或 candles 严重不足

强制规则：
- data_quality=LOW ⇒ 行动必须 **WAIT** 且 **置信度<=40**
- errors_count>0 ⇒ **置信度<=50**
- ATR 缺失（`features.atr14_last=NULL`）⇒ 允许给方向，但 **止损/止盈不得给具体价格**（只能给条件）且 **置信度至少 -10**

---

## Step 6) 分析指标（短线 2~4 小时）
价格口径：
- 若 `market.premium_index.markPrice` 非 NULL：`price_used=markPrice`
- 否则：`price_used=lastPrice`

多周期判断（必须写在理由里）：
- 15m：最近 40 根 close 的变化/动能
- 1h：ema20 vs ema50 + rsi14
- 4h：趋势是否与 1h 一致（不一致要写冲突并降置信度）

微观确认：
- imbalance：>0 偏多；<0 偏空
- spread_bps：越大越不稳（降置信度）
- taker：buySellRatio 与方向一致加分，背离减分
- long/short：与方向一致加分，背离减分

风险/拥挤：
- funding_annualized_pct 绝对值极高 ⇒ 降置信度倾向 WAIT
- basis 最近值异常 ⇒ 降置信度

止损止盈（仅 ATR 存在才给数值）：
- LONG：硬止损 = price - 1.5*ATR；TP1 = price + 1.0*ATR；TP2 = price + 2.0*ATR
- SHORT：硬止损 = price + 1.5*ATR；TP1 = price - 1.0*ATR；TP2 = price - 2.0*ATR
- 达到 TP1 后：剩余 20% 启用移动止损
- ATR 缺失：止损止盈只写条件，不写价格

---

## Step 7) 输出格式（严格固定模板）
第一行：
```
snapshot_id=<文件名或symbol_ms> | snapshot_server_time_utc=<meta.server_time> | analysis_time_utc=<当前UTC> | analysis_time_local=<当前本地> | data_quality=<HIGH/MED/LOW>
```

正文（顺序固定）：
- 【行动】LONG/SHORT/WAIT
- 【置信度】xx/100（必须遵守上限）
- 【关键理由】（1-6：必须“字段路径=数值 -> 含义”；缺失写缺失并说明影响）
  1) `price_used(口径)=...=...; spread_bps=...; imbalance=...`
  2) 15m：动能/方向（引用 candles）
  3) 1h：`ema20_last vs ema50_last; rsi14_last`
  4) 4h：与 1h 是否一致（冲突要写）
  5) OI：`open_interest.change_from_prev_pct=...`
  6) funding：`current_rate/lastFundingRate=...; annualized_pct=...; history最近8条走向=...`
- 【入场】触发条件 + 明确口径(markPrice/lastPrice)
- 【止损】硬止损=价格（ATR缺失则写无法给数值）；软止损=条件
- 【止盈】TP1/TP2 + 20%移动止损（ATR缺失则写无法给数值）
- 【仓位】固定5%，杠杆<=3x
- 【无效条件】至少 2 条
- 【数据健康】errors_count=<...>；missing_key_modules=<未满足的K项>
- 【错误摘要】none 或 列出 errors 前<=5条（key/path/params）

总结区（固定模板，只填内容）：
```
=== 总结 ===
方向：...
口径：markPrice或lastPrice=...
入场：...
止损：...
止盈：...
仓位/杠杆：5% / <=3x
置信度：xx/100
data_quality：...
主要风险：...(最多2条)
========
```

---

## Step 8) 保存到文件（必须 UTF-8；禁止 > / >>）
文件名：
- `analysis_<SYMBOL>_<meta.server_time_ms>.txt`

保存方式：
```powershell
$report | Set-Content -Encoding utf8 -Path $outFile
```

---

## Step 9) 结束确认（输出两行后停止）
```
AGENT_PATH = <当前工作目录>
ANALYSIS_PATH = <输出文件完整路径>
```

---

## 常见缺失/报错的快速判断（给你参考）
- 如果 errors 里出现类似 `interval: "1"` 而不是 `"1m"`，并导致 400：**这是脚本参数/映射问题**（Binance kline interval 通常是 `1m/3m/5m`），不一定是你提示词的问题。  
- 如果中文变 `????`：通常是 **PowerShell 5.1 的重定向保存** 或 **默认编码**；用 Step 0 + Step 8 的写文件方式能显著降低概率。
