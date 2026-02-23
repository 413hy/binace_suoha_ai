#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Build a fixed historical dataset (frozen timestamps) and run walk-forward tuning
for reproducible 30~60m directional evaluation.

Key goals:
- Freeze one timestamp batch so repeated runs compare on identical nodes.
- Use expanding walk-forward folds for tune/test separation.
- Report LONG/SHORT/WAIT distribution and LONG/SHORT accuracy for:
  1) top_signal
  2) watchlist_all_rows
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import math
import os
import random
import statistics
import subprocess
import sys
from typing import Any, Dict, List, Optional, Tuple

import backtest_random_nodes as brn


ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LOGS_DIR = os.path.join(ROOT, "logs")
SELF_CHECK_PY = os.path.join(ROOT, "scripts", "self_check.py")
PROMPT_0 = os.path.join(ROOT, "prompt", "0_master_prompt.txt")
PROMPT_1 = os.path.join(ROOT, "prompt", "1_all_select.txt")


def now_utc_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z")


def now_tag() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime("%Y%m%d_%H%M%S_%f")


def safe_int(x: Any, default: int = 0) -> int:
    try:
        return int(x)
    except Exception:
        return default


def safe_float(x: Any, default: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return default


def wilson_ci(successes: int, total: int, z: float = 1.96) -> List[float]:
    if total <= 0:
        return [0.0, 0.0]
    p = successes / total
    z2 = z * z
    denom = 1.0 + z2 / total
    center = p + z2 / (2.0 * total)
    margin = z * math.sqrt((p * (1.0 - p) / total) + (z2 / (4.0 * total * total)))
    lo = max(0.0, (center - margin) / denom)
    hi = min(1.0, (center + margin) / denom)
    return [lo, hi]


def pct(v: float) -> str:
    return f"{v * 100:.2f}%"


def fmt_ci(ci: List[float]) -> str:
    return f"[{pct(ci[0])}, {pct(ci[1])}]"


def run_self_check(symbol: str = "BTCUSDT") -> Dict[str, Any]:
    p = subprocess.run(
        [sys.executable, SELF_CHECK_PY, "--symbol", symbol],
        cwd=ROOT,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    out = p.stdout or ""
    err = p.stderr or ""
    return {
        "rc": int(p.returncode),
        "pass": (p.returncode == 0 and "status=PASS" in out),
        "stdout_tail": out[-1200:],
        "stderr_tail": err[-600:],
    }


def kbar_to_dict(k: brn.KBar) -> Dict[str, Any]:
    return {
        "open_time_ms": int(k.open_time_ms),
        "close_time_ms": int(k.close_time_ms),
        "open": float(k.open),
        "high": float(k.high),
        "low": float(k.low),
        "close": float(k.close),
        "volume": float(k.volume),
    }


def kbar_from_dict(d: Dict[str, Any]) -> brn.KBar:
    return brn.KBar(
        open_time_ms=int(d["open_time_ms"]),
        close_time_ms=int(d["close_time_ms"]),
        open=float(d["open"]),
        high=float(d["high"]),
        low=float(d["low"]),
        close=float(d["close"]),
        volume=float(d["volume"]),
    )


def point_to_dict(p: brn.Point) -> Dict[str, Any]:
    return {
        "symbol": str(p.symbol),
        "ts_ms": int(p.ts_ms),
        "entry": float(p.entry),
        "atr15": float(p.atr15),
        "ema20_5": float(p.ema20_5),
        "ema50_5": float(p.ema50_5),
        "ema20_15": float(p.ema20_15),
        "ema50_15": float(p.ema50_15),
        "ema20_1h": float(p.ema20_1h),
        "ema50_1h": float(p.ema50_1h),
        "rsi5": float(p.rsi5),
        "rsi15": float(p.rsi15),
        "atr_rel": float(p.atr_rel),
        "trend_conflict": bool(p.trend_conflict),
        "future_bars_5m": [kbar_to_dict(x) for x in p.future_bars_5m],
    }


def point_from_dict(d: Dict[str, Any]) -> brn.Point:
    future = [kbar_from_dict(x) for x in (d.get("future_bars_5m") or [])]
    return brn.Point(
        symbol=str(d["symbol"]),
        ts_ms=int(d["ts_ms"]),
        entry=float(d["entry"]),
        atr15=float(d["atr15"]),
        ema20_5=float(d["ema20_5"]),
        ema50_5=float(d["ema50_5"]),
        ema20_15=float(d["ema20_15"]),
        ema50_15=float(d["ema50_15"]),
        ema20_1h=float(d["ema20_1h"]),
        ema50_1h=float(d["ema50_1h"]),
        rsi5=float(d["rsi5"]),
        rsi15=float(d["rsi15"]),
        atr_rel=float(d["atr_rel"]),
        trend_conflict=bool(d["trend_conflict"]),
        future_bars_5m=future,
    )


def summarize_scope(counts: Dict[str, int]) -> Dict[str, Any]:
    long_n = int(counts.get("long", 0))
    short_n = int(counts.get("short", 0))
    wait_n = int(counts.get("wait", 0))
    total = long_n + short_n + wait_n
    long_win = int(counts.get("long_win", 0))
    short_win = int(counts.get("short_win", 0))
    actionable = long_n + short_n
    combined_win = long_win + short_win

    return {
        "long_short_wait_ratio": {
            "long": (long_n / total) if total > 0 else 0.0,
            "short": (short_n / total) if total > 0 else 0.0,
            "wait": (wait_n / total) if total > 0 else 0.0,
        },
        "long_accuracy": (long_win / long_n) if long_n > 0 else 0.0,
        "short_accuracy": (short_win / short_n) if short_n > 0 else 0.0,
        "long_short_combined_accuracy": (combined_win / actionable) if actionable > 0 else 0.0,
        "long_n": long_n,
        "short_n": short_n,
        "wait_n": wait_n,
        "actionable_n": actionable,
        "long_win_n": long_win,
        "short_win_n": short_win,
        "combined_win_n": combined_win,
        "long_ci95": wilson_ci(long_win, long_n),
        "short_ci95": wilson_ci(short_win, short_n),
        "combined_ci95": wilson_ci(combined_win, actionable),
    }


def aggregate_counts(folds: List[Dict[str, Any]]) -> Dict[str, int]:
    agg = {
        "top_long": 0,
        "top_short": 0,
        "top_wait": 0,
        "top_long_win": 0,
        "top_short_win": 0,
        "watch_long": 0,
        "watch_short": 0,
        "watch_wait": 0,
        "watch_long_win": 0,
        "watch_short_win": 0,
        "folds_used": 0,
    }
    for f in folds:
        ms = f.get("test_master") or {}
        top = ms.get("top_decision_distribution", {}) or {}
        watch = ms.get("watchlist_action_distribution", {}) or {}

        top_long = safe_int(top.get("long_count"))
        top_short = safe_int(top.get("short_count"))
        top_wait = safe_int(top.get("wait_count"))
        watch_long = safe_int(watch.get("long_count"))
        watch_short = safe_int(watch.get("short_count"))
        watch_wait = safe_int(watch.get("wait_count"))

        top_long_win = int(round(safe_float(ms.get("top_long_win_rate")) * top_long))
        top_short_win = int(round(safe_float(ms.get("top_short_win_rate")) * top_short))
        watch_long_win = int(round(safe_float(ms.get("watchlist_long_win_rate")) * watch_long))
        watch_short_win = int(round(safe_float(ms.get("watchlist_short_win_rate")) * watch_short))

        agg["top_long"] += top_long
        agg["top_short"] += top_short
        agg["top_wait"] += top_wait
        agg["top_long_win"] += top_long_win
        agg["top_short_win"] += top_short_win
        agg["watch_long"] += watch_long
        agg["watch_short"] += watch_short
        agg["watch_wait"] += watch_wait
        agg["watch_long_win"] += watch_long_win
        agg["watch_short_win"] += watch_short_win
        agg["folds_used"] += 1
    return agg


def build_fixed_dataset(args: argparse.Namespace) -> Dict[str, Any]:
    rng = random.Random(args.dataset_seed)
    symbols = brn.fetch_top_symbols(args.top_symbols)
    if not symbols:
        raise RuntimeError("No symbols fetched from Binance.US.")

    by_ts: Dict[int, Dict[str, brn.Point]] = {}
    symbol_stats: Dict[str, Dict[str, int]] = {}
    symbol_errors: Dict[str, str] = {}

    for s in symbols:
        try:
            bars5 = brn.fetch_klines(s, "5m", args.fetch_bars_5m)
            bars15 = brn.fetch_klines(s, "15m", args.fetch_bars_15m)
            bars1h = brn.fetch_klines(s, "1h", args.fetch_bars_1h)
            pts_full = brn.build_points_for_symbol(s, bars5, bars15, bars1h, 1_000_000_000, rng)
            if not pts_full:
                symbol_errors[s] = "no_valid_points_after_feature_checks"
                continue

            symbol_stats[s] = {
                "bars_5m": len(bars5),
                "bars_15m": len(bars15),
                "bars_1h": len(bars1h),
                "points_full": len(pts_full),
            }
            for p in pts_full:
                by_ts.setdefault(p.ts_ms, {})
                by_ts[p.ts_ms][p.symbol] = p
        except Exception as ex:
            symbol_errors[s] = str(ex)
            continue

    eligible_ts = [ts for ts, m in by_ts.items() if len(m) >= args.watchlist_size]
    if not eligible_ts:
        raise RuntimeError("No eligible timestamps for watchlist-size requirement.")

    sampled_ts = sorted(rng.sample(eligible_ts, min(args.nodes, len(eligible_ts))))
    if len(sampled_ts) < (args.min_train_nodes + args.min_test_nodes):
        raise RuntimeError(
            f"Too few sampled timestamps={len(sampled_ts)} for walk-forward "
            f"(need at least {args.min_train_nodes + args.min_test_nodes})."
        )

    fixed_points: List[brn.Point] = []
    ts_symbol_counts: Dict[int, int] = {}
    for ts in sampled_ts:
        row = by_ts.get(ts, {})
        ts_symbol_counts[ts] = len(row)
        for p in row.values():
            fixed_points.append(p)

    dataset = {
        "dataset_created_utc": now_utc_iso(),
        "dataset_seed": args.dataset_seed,
        "mode": "fixed_timestamp_dataset",
        "source": "live_binance_us_api_once_then_frozen",
        "config": {
            "top_symbols": args.top_symbols,
            "watchlist_size": args.watchlist_size,
            "nodes": args.nodes,
            "horizon_minutes": args.horizon_minutes,
            "fetch_bars_5m": args.fetch_bars_5m,
            "fetch_bars_15m": args.fetch_bars_15m,
            "fetch_bars_1h": args.fetch_bars_1h,
        },
        "symbols": symbols,
        "symbol_stats": symbol_stats,
        "symbol_errors": symbol_errors,
        "eligible_timestamps_n": len(eligible_ts),
        "sampled_timestamps_n": len(sampled_ts),
        "fixed_timestamps": sampled_ts,
        "per_timestamp_symbol_count": {str(k): int(v) for k, v in ts_symbol_counts.items()},
        "points_n": len(fixed_points),
        "points": [point_to_dict(p) for p in fixed_points],
    }
    return dataset


def load_dataset(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        d = json.load(f)
    if not isinstance(d.get("fixed_timestamps"), list) or not isinstance(d.get("points"), list):
        raise RuntimeError("Invalid dataset format: fixed_timestamps/points missing.")
    return d


def points_by_ts(points: List[brn.Point]) -> Dict[int, Dict[str, brn.Point]]:
    out: Dict[int, Dict[str, brn.Point]] = {}
    for p in points:
        out.setdefault(p.ts_ms, {})
        out[p.ts_ms][p.symbol] = p
    return out


def make_folds(
    ts_sorted: List[int],
    folds: int,
    min_train_nodes: int,
    min_test_nodes: int,
) -> List[Tuple[List[int], List[int]]]:
    total = len(ts_sorted)
    if total < (min_train_nodes + min_test_nodes):
        raise RuntimeError(
            f"Insufficient timestamps for walk-forward: total={total}, "
            f"required>={min_train_nodes + min_test_nodes}"
        )

    usable = total - min_train_nodes
    step = max(min_test_nodes, usable // max(1, folds))
    out: List[Tuple[List[int], List[int]]] = []

    train_end = min_train_nodes
    while len(out) < folds and (train_end + min_test_nodes) <= total:
        test_end = min(total, train_end + step)
        if (test_end - train_end) < min_test_nodes:
            break
        train_ts = ts_sorted[:train_end]
        test_ts = ts_sorted[train_end:test_end]
        out.append((train_ts, test_ts))
        train_end = test_end
    if not out:
        raise RuntimeError("No valid walk-forward folds built.")
    return out


def subset_points(pb: Dict[int, Dict[str, brn.Point]], ts_list: List[int]) -> List[brn.Point]:
    out: List[brn.Point] = []
    for ts in ts_list:
        row = pb.get(ts, {})
        for p in row.values():
            out.append(p)
    return out


def run_fold(
    fold_id: int,
    train_ts: List[int],
    test_ts: List[int],
    pb: Dict[int, Dict[str, brn.Point]],
    args: argparse.Namespace,
) -> Dict[str, Any]:
    train_points = subset_points(pb, train_ts)
    test_points = subset_points(pb, test_ts)
    if len(train_points) < 40 or len(test_points) < 15:
        raise RuntimeError(
            f"Fold {fold_id}: train/test points too small ({len(train_points)}/{len(test_points)})."
        )

    atr_vals = [p.atr_rel for p in train_points]
    if not atr_vals:
        raise RuntimeError(f"Fold {fold_id}: empty atr values.")
    atr_noise_cut = (
        statistics.quantiles(atr_vals, n=10)[-1] if len(atr_vals) >= 20 else max(atr_vals)
    )

    best_params, best_train = brn.choose_best_params(
        train_points,
        atr_noise_cut,
        profile=args.profile,
        min_coverage=args.min_coverage,
        target_win_rate=args.target_win_rate,
    )

    symbol_gate_info: Dict[str, Any] = {
        "enabled": bool(args.enable_symbol_gate),
        "applied": False,
        "allowed_symbols": [],
    }
    if args.profile.lower() == "precision" and args.enable_symbol_gate:
        allowed_symbols, train_by_symbol = brn.derive_symbol_gate(
            train_points,
            best_params,
            atr_noise_cut,
            min_actionable=args.symbol_gate_min_actionable,
            min_win_rate=args.symbol_gate_min_win_rate,
            min_symbols=args.symbol_gate_min_symbols,
        )
        symbol_gate_info["train_by_symbol"] = train_by_symbol
        if allowed_symbols:
            gated = dict(best_params)
            gated["allowed_symbols"] = allowed_symbols
            gated["scan_use_allowed_only"] = 1.0
            best_params = gated
            best_train = brn.evaluate(train_points, best_params, atr_noise_cut)
            symbol_gate_info["applied"] = True
            symbol_gate_info["allowed_symbols"] = allowed_symbols

    best_params = dict(best_params)
    best_params["master_top_score_margin"] = float(
        best_params.get("master_top_score_margin", args.master_top_score_margin)
    )

    master_refine_info: Dict[str, Any] = {"status": "skipped"}
    if args.profile.lower() == "precision" and args.enable_master_refine:
        base_train_master = brn.evaluate_master_prompt_timestamps(
            train_points,
            best_params,
            random.Random(args.eval_seed + fold_id * 101 + 403),
            num_timestamps=max(20, len(train_ts)),
            watchlist_size=max(3, args.watchlist_size),
        )
        min_top_coverage = max(
            0.10,
            args.master_min_top_count / float(max(1, len(train_ts))),
        )
        refined_params, refine_info = brn.refine_params_for_master(
            train_points_full=train_points,
            base_params=best_params,
            rng_seed=args.eval_seed + fold_id * 101 + 404,
            num_timestamps=max(20, len(train_ts)),
            watchlist_size=max(3, args.watchlist_size),
            min_top_coverage=min_top_coverage,
        )
        master_refine_info = {
            "status": refine_info.get("status"),
            "base_train_master": base_train_master,
            "detail": refine_info,
            "accepted": False,
        }
        if refine_info.get("status") == "ok":
            cand_train = brn.evaluate(train_points, refined_params, atr_noise_cut)
            pre_awr = safe_float(best_train.get("actionable_win_rate"))
            cand_awr = safe_float(cand_train.get("actionable_win_rate"))
            ref_train_master = (refine_info.get("train_master") or {})
            base_top_wr = safe_float(base_train_master.get("top_signal_win_rate"))
            ref_top_wr = safe_float(ref_train_master.get("top_signal_win_rate"))
            base_top_count = safe_int(base_train_master.get("top_signal_count"))
            ref_top_count = safe_int(ref_train_master.get("top_signal_count"))
            improve = (
                (ref_top_wr >= base_top_wr + 0.01 and ref_top_count >= max(args.master_min_top_count, base_top_count))
                or (ref_top_wr >= base_top_wr + 0.02)
            )
            not_hurt = cand_awr + 0.10 >= pre_awr
            if improve and not_hurt:
                best_params = refined_params
                best_train = cand_train
                master_refine_info["accepted"] = True

    point_test = brn.evaluate(test_points, best_params, atr_noise_cut)
    master_test = brn.evaluate_master_prompt_timestamps(
        test_points,
        best_params,
        random.Random(args.eval_seed + fold_id * 101 + 202),
        num_timestamps=len(test_ts),
        watchlist_size=max(3, args.watchlist_size),
    )

    return {
        "fold_id": fold_id,
        "train_nodes": len(train_ts),
        "test_nodes": len(test_ts),
        "train_points": len(train_points),
        "test_points": len(test_points),
        "atr_noise_cut": atr_noise_cut,
        "params": best_params,
        "symbol_gate": symbol_gate_info,
        "master_refine": master_refine_info,
        "train_metrics": {k: v for k, v in best_train.items() if k != "rows"},
        "test_point_metrics": {k: v for k, v in point_test.items() if k != "rows"},
        "test_master": master_test,
    }


def run_walk_forward_once(
    ts_sorted: List[int],
    pb: Dict[int, Dict[str, brn.Point]],
    args: argparse.Namespace,
) -> Dict[str, Any]:
    folds = make_folds(
        ts_sorted=ts_sorted,
        folds=args.folds,
        min_train_nodes=args.min_train_nodes,
        min_test_nodes=args.min_test_nodes,
    )
    fold_results: List[Dict[str, Any]] = []
    for i, (train_ts, test_ts) in enumerate(folds):
        fold_results.append(run_fold(i + 1, train_ts, test_ts, pb, args))

    agg = aggregate_counts(fold_results)
    top_summary = summarize_scope(
        {
            "long": agg["top_long"],
            "short": agg["top_short"],
            "wait": agg["top_wait"],
            "long_win": agg["top_long_win"],
            "short_win": agg["top_short_win"],
        }
    )
    watch_summary = summarize_scope(
        {
            "long": agg["watch_long"],
            "short": agg["watch_short"],
            "wait": agg["watch_wait"],
            "long_win": agg["watch_long_win"],
            "short_win": agg["watch_short_win"],
        }
    )
    return {
        "folds": fold_results,
        "aggregate": agg,
        "summary": {
            "top_signal": top_summary,
            "watchlist_all_rows": watch_summary,
        },
    }


def build_markdown(report: Dict[str, Any], dataset_path: str, out_json: str, out_md: str) -> str:
    cfg = report.get("config", {}) or {}
    summary = report.get("summary", {}) or {}
    top = summary.get("top_signal", {}) or {}
    watch = summary.get("watchlist_all_rows", {}) or {}
    reproducibility = report.get("reproducibility", {}) or {}
    lines: List[str] = []
    lines.append(f"# Fixed Dataset Walk-Forward Report ({report.get('run_time_utc')})")
    lines.append("")
    lines.append("## Config")
    lines.append(f"- mode: `{report.get('mode')}`")
    lines.append(f"- workflow: `{report.get('workflow')}`")
    lines.append(f"- dataset_path: `{dataset_path}`")
    lines.append(f"- profile: `{cfg.get('profile')}`")
    lines.append(f"- folds: {cfg.get('folds')}")
    lines.append(f"- min_train_nodes: {cfg.get('min_train_nodes')}")
    lines.append(f"- min_test_nodes: {cfg.get('min_test_nodes')}")
    lines.append(f"- watchlist_size: {cfg.get('watchlist_size')}")
    lines.append(f"- target_win_rate: {cfg.get('target_win_rate')}")
    lines.append(f"- min_coverage: {cfg.get('min_coverage')}")
    lines.append(f"- repeats: {cfg.get('repeats')}")
    lines.append("")
    lines.append("## Top Signal")
    lines.append("| metric | value |")
    lines.append("|---|---:|")
    lines.append(f"| LONG/SHORT/WAIT n | {top.get('long_n',0)}/{top.get('short_n',0)}/{top.get('wait_n',0)} |")
    lines.append(
        f"| LONG/SHORT/WAIT ratio | {pct((top.get('long_short_wait_ratio') or {}).get('long',0.0))} / "
        f"{pct((top.get('long_short_wait_ratio') or {}).get('short',0.0))} / "
        f"{pct((top.get('long_short_wait_ratio') or {}).get('wait',0.0))} |"
    )
    lines.append(f"| LONG accuracy | {pct(top.get('long_accuracy',0.0))} |")
    lines.append(f"| SHORT accuracy | {pct(top.get('short_accuracy',0.0))} |")
    lines.append(f"| LONG+SHORT combined accuracy | {pct(top.get('long_short_combined_accuracy',0.0))} |")
    lines.append(f"| LONG CI95 | {fmt_ci(top.get('long_ci95') or [0.0,0.0])} |")
    lines.append(f"| SHORT CI95 | {fmt_ci(top.get('short_ci95') or [0.0,0.0])} |")
    lines.append(f"| COMBINED CI95 | {fmt_ci(top.get('combined_ci95') or [0.0,0.0])} |")
    lines.append("")
    lines.append("## Watchlist All Rows")
    lines.append("| metric | value |")
    lines.append("|---|---:|")
    lines.append(f"| LONG/SHORT/WAIT n | {watch.get('long_n',0)}/{watch.get('short_n',0)}/{watch.get('wait_n',0)} |")
    lines.append(
        f"| LONG/SHORT/WAIT ratio | {pct((watch.get('long_short_wait_ratio') or {}).get('long',0.0))} / "
        f"{pct((watch.get('long_short_wait_ratio') or {}).get('short',0.0))} / "
        f"{pct((watch.get('long_short_wait_ratio') or {}).get('wait',0.0))} |"
    )
    lines.append(f"| LONG accuracy | {pct(watch.get('long_accuracy',0.0))} |")
    lines.append(f"| SHORT accuracy | {pct(watch.get('short_accuracy',0.0))} |")
    lines.append(f"| LONG+SHORT combined accuracy | {pct(watch.get('long_short_combined_accuracy',0.0))} |")
    lines.append(f"| LONG CI95 | {fmt_ci(watch.get('long_ci95') or [0.0,0.0])} |")
    lines.append(f"| SHORT CI95 | {fmt_ci(watch.get('short_ci95') or [0.0,0.0])} |")
    lines.append(f"| COMBINED CI95 | {fmt_ci(watch.get('combined_ci95') or [0.0,0.0])} |")
    lines.append("")
    lines.append("## Reproducibility")
    lines.append(f"- repeat_consistent: {reproducibility.get('repeat_consistent')}")
    lines.append(f"- repeats: {reproducibility.get('repeats')}")
    lines.append("")
    lines.append("## Files")
    lines.append(f"- json: `{out_json}`")
    lines.append(f"- md: `{out_md}`")
    return "\n".join(lines) + "\n"


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Build fixed timestamp dataset and run walk-forward tuning."
    )
    ap.add_argument("--dataset-path", type=str, default="")
    ap.add_argument("--nodes", type=int, default=120)
    ap.add_argument("--dataset-seed", type=int, default=20260321)
    ap.add_argument("--eval-seed", type=int, default=20260331)
    ap.add_argument("--top-symbols", type=int, default=10)
    ap.add_argument("--watchlist-size", type=int, default=5)
    ap.add_argument("--horizon-minutes", type=int, default=60)
    ap.add_argument("--fetch-bars-5m", type=int, default=3000)
    ap.add_argument("--fetch-bars-15m", type=int, default=1600)
    ap.add_argument("--fetch-bars-1h", type=int, default=900)
    ap.add_argument("--profile", type=str, default="precision")
    ap.add_argument("--target-win-rate", type=float, default=0.70)
    ap.add_argument("--min-coverage", type=float, default=0.10)
    ap.add_argument("--folds", type=int, default=5)
    ap.add_argument("--min-train-nodes", type=int, default=40)
    ap.add_argument("--min-test-nodes", type=int, default=15)
    ap.add_argument("--repeats", type=int, default=2)
    ap.add_argument("--enable-symbol-gate", type=int, default=1)
    ap.add_argument("--symbol-gate-min-actionable", type=int, default=8)
    ap.add_argument("--symbol-gate-min-win-rate", type=float, default=0.62)
    ap.add_argument("--symbol-gate-min-symbols", type=int, default=3)
    ap.add_argument("--enable-master-refine", type=int, default=0)
    ap.add_argument("--master-min-top-count", type=int, default=12)
    ap.add_argument("--master-top-score-margin", type=float, default=0.20)
    args = ap.parse_args()

    if args.nodes <= 0:
        raise SystemExit("--nodes must be > 0")
    if args.folds <= 0:
        raise SystemExit("--folds must be > 0")
    if args.repeats <= 0:
        raise SystemExit("--repeats must be > 0")

    os.makedirs(LOGS_DIR, exist_ok=True)

    # Touch prompts for traceability (do not print full content).
    with open(PROMPT_0, "r", encoding="utf-8") as _:
        pass
    with open(PROMPT_1, "r", encoding="utf-8") as _:
        pass

    self_check = run_self_check("BTCUSDT")

    dataset_path = args.dataset_path.strip()
    if dataset_path:
        if not os.path.isabs(dataset_path):
            dataset_path = os.path.normpath(os.path.join(ROOT, dataset_path))
    else:
        dataset_path = os.path.join(LOGS_DIR, f"fixed_dataset_{now_tag()}.json")

    if os.path.exists(dataset_path):
        dataset_obj = load_dataset(dataset_path)
        dataset_mode = "loaded_existing"
    else:
        dataset_obj = build_fixed_dataset(args)
        with open(dataset_path, "w", encoding="utf-8") as f:
            json.dump(dataset_obj, f, ensure_ascii=False, indent=2)
        dataset_mode = "built_new"

    points = [point_from_dict(d) for d in (dataset_obj.get("points") or [])]
    fixed_ts = sorted(int(x) for x in (dataset_obj.get("fixed_timestamps") or []))
    pb = points_by_ts(points)

    run_results: List[Dict[str, Any]] = []
    aggregates: List[Dict[str, Any]] = []
    for i in range(args.repeats):
        once = run_walk_forward_once(fixed_ts, pb, args)
        run_results.append(once)
        aggregates.append(once["aggregate"])

    repeat_consistent = True
    if len(aggregates) >= 2:
        first = aggregates[0]
        for x in aggregates[1:]:
            if x != first:
                repeat_consistent = False
                break

    final = run_results[0]
    summary = final["summary"]
    aggregate = final["aggregate"]
    top_comb = safe_float((summary.get("top_signal") or {}).get("long_short_combined_accuracy"))
    watch_comb = safe_float((summary.get("watchlist_all_rows") or {}).get("long_short_combined_accuracy"))

    report = {
        "run_time_utc": now_utc_iso(),
        "mode": "fixed_dataset_walk_forward_tuning",
        "workflow": "fixed_dataset_build_or_load -> expanding_walk_forward_tune -> fold_test_aggregate",
        "dataset": {
            "path": os.path.relpath(dataset_path, ROOT).replace("\\", "/"),
            "mode": dataset_mode,
            "created_utc": dataset_obj.get("dataset_created_utc"),
            "sampled_timestamps_n": safe_int(dataset_obj.get("sampled_timestamps_n")),
            "eligible_timestamps_n": safe_int(dataset_obj.get("eligible_timestamps_n")),
            "points_n": safe_int(dataset_obj.get("points_n")),
            "symbols": dataset_obj.get("symbols") or [],
        },
        "config": {
            "profile": args.profile,
            "nodes": args.nodes,
            "folds": args.folds,
            "min_train_nodes": args.min_train_nodes,
            "min_test_nodes": args.min_test_nodes,
            "watchlist_size": args.watchlist_size,
            "target_win_rate": args.target_win_rate,
            "min_coverage": args.min_coverage,
            "repeats": args.repeats,
            "enable_symbol_gate": bool(args.enable_symbol_gate),
            "enable_master_refine": bool(args.enable_master_refine),
            "master_top_score_margin": args.master_top_score_margin,
        },
        "precheck": self_check,
        "folds": final["folds"],
        "aggregate": aggregate,
        "summary": summary,
        "reproducibility": {
            "repeats": args.repeats,
            "repeat_consistent": repeat_consistent,
        },
        "target_reached": {
            "top_signal_combined_ge_70pct": bool(top_comb >= 0.70),
            "watchlist_combined_ge_70pct": bool(watch_comb >= 0.70),
        },
        "all_repeat_aggregates": aggregates,
    }

    out_json = os.path.join(LOGS_DIR, f"walk_forward_fixed_dataset_{now_tag()}.json")
    out_md = os.path.join(LOGS_DIR, f"walk_forward_fixed_dataset_{now_tag()}.md")
    with open(out_json, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    md = build_markdown(report, dataset_path, out_json, out_md)
    with open(out_md, "w", encoding="utf-8") as f:
        f.write(md)

    top = summary["top_signal"]
    watch = summary["watchlist_all_rows"]
    print(
        "TOP_SIGNAL LONG_n={0} / SHORT_n={1} / WAIT_n={2}".format(
            top["long_n"], top["short_n"], top["wait_n"]
        )
    )
    print(
        "TOP_SIGNAL LONG_wr={0} / SHORT_wr={1} / COMBINED_wr={2}".format(
            pct(top["long_accuracy"]),
            pct(top["short_accuracy"]),
            pct(top["long_short_combined_accuracy"]),
        )
    )
    print(
        "WATCHLIST_ALL_ROWS LONG_n={0} / SHORT_n={1} / WAIT_n={2}".format(
            watch["long_n"], watch["short_n"], watch["wait_n"]
        )
    )
    print(
        "WATCHLIST_ALL_ROWS LONG_wr={0} / SHORT_wr={1} / COMBINED_wr={2}".format(
            pct(watch["long_accuracy"]),
            pct(watch["short_accuracy"]),
            pct(watch["long_short_combined_accuracy"]),
        )
    )
    print(f"DATASET_PATH={os.path.relpath(dataset_path, ROOT).replace(os.sep, '/')}")
    print(f"REPEAT_CONSISTENT={str(repeat_consistent).lower()}")
    print(f"OUTPUT_JSON={os.path.relpath(out_json, ROOT).replace(os.sep, '/')}")
    print(f"OUTPUT_MD={os.path.relpath(out_md, ROOT).replace(os.sep, '/')}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
