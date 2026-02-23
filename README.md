# binace_suoha_ai

## Quick Start (Binance.US Spot)

```powershell
python .\scripts\self_check.py --symbol BTCUSDT
python .\scripts\data.py --symbol BTCUSDT --intervals 1m 3m 5m 15m 30m 1h --kline-limit 360 --hist-period 15m --hist-limit 192 --depth 150 --include-exchange-info --include-trades --trades-limit 1000 --out .\logs\snapshot_BTCUSDT_manual.json --strict
python .\scripts\decision_guard.py --analysis .\logs\analysis_BTCUSDT_xxx.txt --inplace
python .\scripts\backtest_random_nodes.py
python .\scripts\master_prompt_random_eval.py --nodes-per-seed 60 --seeds 20260311,20260312,20260313
python .\scripts\walk_forward_fixed_dataset.py --nodes 120 --folds 5 --repeats 2 --profile precision --target-win-rate 0.70 --min-coverage 0.10
```

## Notes

- Prompt/Skill files are UTF-8 with BOM to avoid PowerShell 5.1 misread/garbled text.
- `scripts/self_check.py` is the first-line guard against repeated session errors.
- `scripts/data.py` now writes `diagnostics.self_check` and `diagnostics.error_diagnostics` into snapshot JSON.
- `scripts/decision_guard.py` is the post-analysis hard guard. It can enforce WAIT automatically (`status=ENFORCED_WAIT`) when core conditions are incomplete.
- `scripts/master_prompt_random_eval.py` runs multi-seed random timestamp historical evaluation for the 0_master workflow and outputs dual-scope metrics (`top_signal` + `watchlist_all_rows`).
- `scripts/walk_forward_fixed_dataset.py` freezes one timestamp batch into a local dataset, then runs expanding walk-forward tuning on the same nodes for reproducible comparison.
