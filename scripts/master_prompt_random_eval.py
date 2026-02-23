#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Historical random-timestamp evaluation for 0_master prompt flow.

Workflow:
1) random timestamps
2) stageA scan watchlist
3) stageB per-symbol analysis
4) aggregate LONG/SHORT/WAIT ratio + LONG/SHORT accuracy

This script reuses scripts/backtest_random_nodes.py and aggregates multi-seed
results in a single auditable report.
"""

from __future__ import annotations

import argparse
import datetime as dt
import glob
import json
import math
import os
import pathlib
import re
import statistics
import subprocess
import sys
from typing import Any, Dict, List, Optional, Tuple


ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SCRIPTS_DIR = os.path.join(ROOT, "scripts")
LOGS_DIR = os.path.join(ROOT, "logs")
BACKTEST_PY = os.path.join(SCRIPTS_DIR, "backtest_random_nodes.py")
SELF_CHECK_PY = os.path.join(SCRIPTS_DIR, "self_check.py")
PROMPT_0 = os.path.join(ROOT, "prompt", "0_master_prompt.txt")
PROMPT_1 = os.path.join(ROOT, "prompt", "1_all_select.txt")


def now_utc_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z")


def parse_seed_list(raw: str) -> List[int]:
    out: List[int] = []
    seen = set()
    for tok in (raw or "").replace(" ", "").split(","):
        if not tok:
            continue
        v = int(tok)
        if v in seen:
            continue
        seen.add(v)
        out.append(v)
    if not out:
        raise ValueError("seeds is empty")
    return out


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


def read_utf8(path: str) -> str:
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


def run_cmd(cmd: List[str], env: Optional[Dict[str, str]] = None) -> Tuple[int, str, str]:
    p = subprocess.run(
        cmd,
        cwd=ROOT,
        env=env,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    return p.returncode, p.stdout or "", p.stderr or ""


def parse_output_path(stdout: str) -> Optional[str]:
    # backtest_random_nodes.py prints:
    # OK: logs/backtest_random_nodes_*.json
    m = re.findall(r"^OK:\s*(.+?\.json)\s*$", stdout, flags=re.MULTILINE)
    if not m:
        return None
    rel = m[-1].strip().replace("/", os.sep).replace("\\", os.sep)
    return os.path.normpath(os.path.join(ROOT, rel))


def detect_no_valid_params(stdout: str, stderr: str) -> bool:
    text = (stdout or "") + "\n" + (stderr or "")
    return "No valid parameter set found" in text


def build_env(
    seed: int,
    args: argparse.Namespace,
    relaxed: bool = False,
) -> Dict[str, str]:
    env = dict(os.environ)
    env.update(
        {
            "BACKTEST_PROFILE": args.profile,
            "BACKTEST_MASTER_TS_NODES": str(args.nodes_per_seed),
            "BACKTEST_MASTER_WATCHLIST_SIZE": str(args.watchlist_size),
            "BACKTEST_TOP_SYMBOLS": str(args.top_symbols),
            "BACKTEST_POINTS_PER_SYMBOL": str(args.points_per_symbol),
            "BACKTEST_HORIZON_MINUTES": str(args.horizon_minutes),
            "BACKTEST_SEED": str(seed),
            "BACKTEST_ENABLE_PER_SYMBOL_OPT": "0",
            "BACKTEST_ENABLE_MASTER_REFINE": "0",
            # Explicitly pin defaults for reproducibility.
            "BACKTEST_MIN_COVERAGE": "0.10",
            "BACKTEST_TARGET_WIN_RATE": "0.70",
            "BACKTEST_SYMBOL_GATE_MIN_ACTIONABLE": "8",
            "BACKTEST_SYMBOL_GATE_MIN_WIN_RATE": "0.62",
        }
    )
    if relaxed:
        env.update(
            {
                "BACKTEST_MIN_COVERAGE": "0.02",
                "BACKTEST_TARGET_WIN_RATE": "0.55",
                "BACKTEST_SYMBOL_GATE_MIN_ACTIONABLE": "6",
                "BACKTEST_SYMBOL_GATE_MIN_WIN_RATE": "0.58",
            }
        )
    return env


def run_one_seed(seed: int, args: argparse.Namespace) -> Dict[str, Any]:
    cmd = [sys.executable, BACKTEST_PY]
    primary_env = build_env(seed=seed, args=args, relaxed=False)
    rc1, out1, err1 = run_cmd(cmd, env=primary_env)
    if rc1 == 0:
        path = parse_output_path(out1)
        return {
            "seed": seed,
            "status": "ok",
            "attempt": "primary",
            "output_json_path": path,
            "stdout": out1,
            "stderr": err1,
        }

    if detect_no_valid_params(out1, err1):
        relaxed_env = build_env(seed=seed, args=args, relaxed=True)
        rc2, out2, err2 = run_cmd(cmd, env=relaxed_env)
        if rc2 == 0:
            path = parse_output_path(out2)
            return {
                "seed": seed,
                "status": "ok",
                "attempt": "relaxed_after_no_valid_params",
                "output_json_path": path,
                "stdout": out2,
                "stderr": err2,
                "primary_error": (out1 + "\n" + err1).strip(),
            }
        return {
            "seed": seed,
            "status": "failed",
            "attempt": "relaxed_after_no_valid_params",
            "output_json_path": None,
            "stdout": out2,
            "stderr": err2,
            "primary_error": (out1 + "\n" + err1).strip(),
        }

    return {
        "seed": seed,
        "status": "failed",
        "attempt": "primary",
        "output_json_path": None,
        "stdout": out1,
        "stderr": err1,
    }


def load_json(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def extract_scope_counts(master_sim: Dict[str, Any]) -> Dict[str, Dict[str, int]]:
    top = master_sim.get("top_decision_distribution", {}) or {}
    watch = master_sim.get("watchlist_action_distribution", {}) or {}
    top_long = safe_int(top.get("long_count"))
    top_short = safe_int(top.get("short_count"))
    top_wait = safe_int(top.get("wait_count"))
    watch_long = safe_int(watch.get("long_count"))
    watch_short = safe_int(watch.get("short_count"))
    watch_wait = safe_int(watch.get("wait_count"))

    top_long_wr = safe_float(master_sim.get("top_long_win_rate"))
    top_short_wr = safe_float(master_sim.get("top_short_win_rate"))
    watch_long_wr = safe_float(master_sim.get("watchlist_long_win_rate"))
    watch_short_wr = safe_float(master_sim.get("watchlist_short_win_rate"))

    top_long_win = int(round(top_long_wr * top_long))
    top_short_win = int(round(top_short_wr * top_short))
    watch_long_win = int(round(watch_long_wr * watch_long))
    watch_short_win = int(round(watch_short_wr * watch_short))

    return {
        "top_signal": {
            "long": top_long,
            "short": top_short,
            "wait": top_wait,
            "long_win": top_long_win,
            "short_win": top_short_win,
        },
        "watchlist_all_rows": {
            "long": watch_long,
            "short": watch_short,
            "wait": watch_wait,
            "long_win": watch_long_win,
            "short_win": watch_short_win,
        },
    }


def summarize_scope(c: Dict[str, int]) -> Dict[str, Any]:
    long_n = int(c.get("long", 0))
    short_n = int(c.get("short", 0))
    wait_n = int(c.get("wait", 0))
    total = long_n + short_n + wait_n
    long_win = int(c.get("long_win", 0))
    short_win = int(c.get("short_win", 0))
    actionable = long_n + short_n
    combined_win = long_win + short_win

    long_acc = (long_win / long_n) if long_n > 0 else 0.0
    short_acc = (short_win / short_n) if short_n > 0 else 0.0
    combined_acc = (combined_win / actionable) if actionable > 0 else 0.0

    return {
        "long_short_wait_ratio": {
            "long": (long_n / total) if total > 0 else 0.0,
            "short": (short_n / total) if total > 0 else 0.0,
            "wait": (wait_n / total) if total > 0 else 0.0,
        },
        "long_accuracy": long_acc,
        "short_accuracy": short_acc,
        "long_short_combined_accuracy": combined_acc,
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


def ci_overlap(a: List[float], b: List[float]) -> bool:
    return not (a[1] < b[0] or b[1] < a[0])


def normalize_historical_entry(path: str, obj: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    # New format:
    # summary.top_signal.long_short_combined_accuracy
    if isinstance(obj.get("summary"), dict):
        s = obj.get("summary") or {}
        top = s.get("top_signal")
        watch = s.get("watchlist_all_rows")
        if isinstance(top, dict) and isinstance(watch, dict):
            return {
                "path": path,
                "format": "multi",
                "top_signal": top,
                "watchlist_all_rows": watch,
            }
    # Legacy format:
    # summary_top_signal / summary_watchlist_all_rows
    if isinstance(obj.get("summary_top_signal"), dict):
        return {
            "path": path,
            "format": "legacy",
            "top_signal": obj.get("summary_top_signal") or {},
            "watchlist_all_rows": obj.get("summary_watchlist_all_rows") or {},
        }
    return None


def limit_assessment(
    current_summary: Dict[str, Any],
    current_output_json: str,
    actionable_min: int = 40,
) -> Dict[str, Any]:
    cand_paths = sorted(
        set(glob.glob(os.path.join(LOGS_DIR, "master_prompt_random_ts_eval*.json")))
    )
    current_abs = os.path.abspath(current_output_json)
    entries: List[Dict[str, Any]] = []
    for p in cand_paths:
        ap = os.path.abspath(p)
        if ap == current_abs:
            continue
        try:
            obj = load_json(ap)
            e = normalize_historical_entry(ap, obj)
            if e is None:
                continue
            entries.append(e)
        except Exception:
            continue

    def best_for_scope(scope: str) -> Optional[Dict[str, Any]]:
        filtered: List[Dict[str, Any]] = []
        for e in entries:
            s = e.get(scope, {}) or {}
            if safe_int(s.get("actionable_n")) < actionable_min:
                continue
            filtered.append(e)
        if not filtered:
            return None
        return max(
            filtered,
            key=lambda e: safe_float((e.get(scope, {}) or {}).get("long_short_combined_accuracy")),
        )

    out: Dict[str, Any] = {
        "actionable_n_min_for_baseline": actionable_min,
        "candidates_n": len(entries),
    }

    overall_near = True
    for scope in ("top_signal", "watchlist_all_rows"):
        cur = current_summary.get(scope, {}) or {}
        cur_acc = safe_float(cur.get("long_short_combined_accuracy"))
        cur_ci = cur.get("combined_ci95") or [0.0, 0.0]
        best = best_for_scope(scope)
        if best is None:
            out[scope] = {
                "status": "insufficient_baseline",
                "current_combined_accuracy": cur_acc,
                "current_combined_ci95": cur_ci,
                "current_vs_best_gap_pp": 0.0,
                "near_current_limit": False,
            }
            overall_near = False
            continue

        best_s = best.get(scope, {}) or {}
        best_acc = safe_float(best_s.get("long_short_combined_accuracy"))
        best_ci = best_s.get("combined_ci95") or [0.0, 0.0]
        gap_pp = (best_acc - cur_acc) * 100.0
        near = (gap_pp <= 2.0) and ci_overlap(cur_ci, best_ci)
        out[scope] = {
            "status": "ok",
            "baseline_path": best.get("path"),
            "baseline_format": best.get("format"),
            "baseline_combined_accuracy": best_acc,
            "baseline_combined_ci95": best_ci,
            "current_combined_accuracy": cur_acc,
            "current_combined_ci95": cur_ci,
            "current_vs_best_gap_pp": gap_pp,
            "near_current_limit": near,
        }
        overall_near = overall_near and near

    out["overall_near_current_limit"] = overall_near
    return out


def pct(v: float) -> str:
    return f"{(v * 100.0):.2f}%"


def fmt_ci(ci: List[float]) -> str:
    return f"[{pct(ci[0])}, {pct(ci[1])}]"


def build_markdown(
    report: Dict[str, Any],
    out_json: str,
    out_md: str,
) -> str:
    cfg = report.get("config", {}) or {}
    summary = report.get("summary", {}) or {}
    top = summary.get("top_signal", {}) or {}
    watch = summary.get("watchlist_all_rows", {}) or {}
    limit = report.get("limit_assessment", {}) or {}
    lt = limit.get("top_signal", {}) or {}
    lw = limit.get("watchlist_all_rows", {}) or {}

    lines: List[str] = []
    lines.append(f"# Master Prompt Random Timestamp Eval ({report.get('run_time_utc')})")
    lines.append("")
    lines.append("## Config")
    lines.append(f"- mode: `{report.get('mode')}`")
    lines.append(f"- workflow: `{report.get('workflow')}`")
    lines.append(f"- win_mode: `{cfg.get('win_mode')}`")
    lines.append(f"- profile: `{cfg.get('profile')}`")
    lines.append(f"- nodes_per_seed: {cfg.get('nodes_per_seed')}")
    lines.append(f"- seeds: {', '.join(str(x) for x in (cfg.get('seeds') or []))}")
    lines.append(f"- watchlist_size: {cfg.get('watchlist_size')}")
    lines.append(f"- top_symbols: {cfg.get('top_symbols')}")
    lines.append(f"- points_per_symbol: {cfg.get('points_per_symbol')}")
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
    lines.append("## Limit Assessment")
    lines.append(f"- top_signal.near_current_limit: {lt.get('near_current_limit')}")
    lines.append(f"- top_signal.current_vs_best_gap_pp: {lt.get('current_vs_best_gap_pp')}")
    lines.append(f"- watchlist_all_rows.near_current_limit: {lw.get('near_current_limit')}")
    lines.append(f"- watchlist_all_rows.current_vs_best_gap_pp: {lw.get('current_vs_best_gap_pp')}")
    lines.append(f"- overall_near_current_limit: {limit.get('overall_near_current_limit')}")
    lines.append("")
    lines.append("## Failed Seeds")
    failed = report.get("failed_seeds") or []
    if not failed:
        lines.append("- none")
    else:
        for fs in failed:
            lines.append(f"- seed={fs.get('seed')} attempt={fs.get('attempt')} reason={fs.get('reason')}")
    lines.append("")
    lines.append("## Files")
    lines.append(f"- json: `{out_json}`")
    lines.append(f"- md: `{out_md}`")
    return "\n".join(lines) + "\n"


def main() -> int:
    ap = argparse.ArgumentParser(
        description="0_master random timestamp multi-seed historical evaluation"
    )
    ap.add_argument("--nodes-per-seed", type=int, default=60)
    ap.add_argument("--seeds", type=str, default="20260311,20260312,20260313")
    ap.add_argument("--watchlist-size", type=int, default=5)
    ap.add_argument("--top-symbols", type=int, default=10)
    ap.add_argument("--points-per-symbol", type=int, default=80)
    ap.add_argument("--profile", type=str, default="precision")
    ap.add_argument("--horizon-minutes", type=int, default=60)
    ap.add_argument("--win-mode", type=str, default="direction_consistency")
    args = ap.parse_args()

    if args.win_mode != "direction_consistency":
        raise SystemExit("Only win-mode=direction_consistency is supported currently.")
    if args.nodes_per_seed <= 0:
        raise SystemExit("--nodes-per-seed must be > 0")
    if args.watchlist_size < 1:
        raise SystemExit("--watchlist-size must be >= 1")

    os.makedirs(LOGS_DIR, exist_ok=True)

    # Precheck: read prompt files for rule alignment and run self-check once.
    prompt0_text = read_utf8(PROMPT_0)
    prompt1_text = read_utf8(PROMPT_1)
    _ = len(prompt0_text) + len(prompt1_text)

    rc_sc, out_sc, err_sc = run_cmd([sys.executable, SELF_CHECK_PY, "--symbol", "BTCUSDT"])
    self_check_ok = (rc_sc == 0) and ("status=PASS" in out_sc)

    seeds = parse_seed_list(args.seeds)
    runs: List[Dict[str, Any]] = []
    failed_seeds: List[Dict[str, Any]] = []

    base_dataset: Optional[Dict[str, Any]] = None
    aggregate = {
        "top_total_ts": 0,
        "top_long": 0,
        "top_short": 0,
        "top_wait": 0,
        "top_long_win": 0,
        "top_short_win": 0,
        "top_actionable": 0,
        "top_actionable_win": 0,
        "watch_total_rows": 0,
        "watch_long": 0,
        "watch_short": 0,
        "watch_wait": 0,
        "watch_long_win": 0,
        "watch_short_win": 0,
        "watch_actionable": 0,
        "watch_actionable_win": 0,
    }

    for seed in seeds:
        res = run_one_seed(seed, args)
        if res.get("status") != "ok":
            failed_seeds.append(
                {
                    "seed": seed,
                    "attempt": res.get("attempt"),
                    "reason": "runner_failed",
                    "stderr_tail": (res.get("stderr") or "")[-600:],
                }
            )
            runs.append(
                {
                    "seed": seed,
                    "status": "failed",
                    "attempt": res.get("attempt"),
                }
            )
            continue

        out_json_path = res.get("output_json_path")
        if not out_json_path or not os.path.exists(out_json_path):
            failed_seeds.append(
                {
                    "seed": seed,
                    "attempt": res.get("attempt"),
                    "reason": "missing_output_json",
                }
            )
            runs.append(
                {
                    "seed": seed,
                    "status": "failed",
                    "attempt": res.get("attempt"),
                }
            )
            continue

        bt = load_json(out_json_path)
        if base_dataset is None:
            base_dataset = {
                "symbols": bt.get("symbols") or [],
                "symbol_errors": bt.get("symbol_errors") or {},
                "train_points_n": safe_int(((bt.get("sampling") or {}).get("train_points"))),
                "test_points_n": safe_int(((bt.get("sampling") or {}).get("test_points"))),
                "test_points_full_n": safe_int(((bt.get("sampling") or {}).get("test_points_full"))),
                "optimized_params": bt.get("optimized_params") or {},
                "params_source": "choose_best_params",
                "train_metrics": bt.get("optimized_train") or {},
            }

        ms = bt.get("master_prompt_simulation") or {}
        counts = extract_scope_counts(ms)
        top_c = counts["top_signal"]
        watch_c = counts["watchlist_all_rows"]
        top_total = top_c["long"] + top_c["short"] + top_c["wait"]
        watch_total = watch_c["long"] + watch_c["short"] + watch_c["wait"]
        top_actionable = top_c["long"] + top_c["short"]
        watch_actionable = watch_c["long"] + watch_c["short"]
        top_actionable_win = top_c["long_win"] + top_c["short_win"]
        watch_actionable_win = watch_c["long_win"] + watch_c["short_win"]

        aggregate["top_total_ts"] += top_total
        aggregate["top_long"] += top_c["long"]
        aggregate["top_short"] += top_c["short"]
        aggregate["top_wait"] += top_c["wait"]
        aggregate["top_long_win"] += top_c["long_win"]
        aggregate["top_short_win"] += top_c["short_win"]
        aggregate["top_actionable"] += top_actionable
        aggregate["top_actionable_win"] += top_actionable_win

        aggregate["watch_total_rows"] += watch_total
        aggregate["watch_long"] += watch_c["long"]
        aggregate["watch_short"] += watch_c["short"]
        aggregate["watch_wait"] += watch_c["wait"]
        aggregate["watch_long_win"] += watch_c["long_win"]
        aggregate["watch_short_win"] += watch_c["short_win"]
        aggregate["watch_actionable"] += watch_actionable
        aggregate["watch_actionable_win"] += watch_actionable_win

        runs.append(
            {
                "seed": seed,
                "status": "ok",
                "attempt": res.get("attempt"),
                "nodes": args.nodes_per_seed,
                "source_backtest_json": os.path.relpath(out_json_path, ROOT).replace("\\", "/"),
                "top": {
                    "long_n": top_c["long"],
                    "short_n": top_c["short"],
                    "wait_n": top_c["wait"],
                    "long_win_n": top_c["long_win"],
                    "short_win_n": top_c["short_win"],
                    "actionable_n": top_actionable,
                    "combined_win_n": top_actionable_win,
                },
                "watch": {
                    "long_n": watch_c["long"],
                    "short_n": watch_c["short"],
                    "wait_n": watch_c["wait"],
                    "long_win_n": watch_c["long_win"],
                    "short_win_n": watch_c["short_win"],
                    "actionable_n": watch_actionable,
                    "combined_win_n": watch_actionable_win,
                },
                "sample_nodes": ms.get("sample_nodes") or [],
            }
        )

    top_summary = summarize_scope(
        {
            "long": aggregate["top_long"],
            "short": aggregate["top_short"],
            "wait": aggregate["top_wait"],
            "long_win": aggregate["top_long_win"],
            "short_win": aggregate["top_short_win"],
        }
    )
    watch_summary = summarize_scope(
        {
            "long": aggregate["watch_long"],
            "short": aggregate["watch_short"],
            "wait": aggregate["watch_wait"],
            "long_win": aggregate["watch_long_win"],
            "short_win": aggregate["watch_short_win"],
        }
    )

    report: Dict[str, Any] = {
        "run_time_utc": now_utc_iso(),
        "mode": "historical_0_master_prompt_random_timestamp_eval",
        "workflow": "random_timestamps -> stageA_scan_watchlist -> stageB_per_symbol_analysis",
        "config": {
            "nodes_per_seed": args.nodes_per_seed,
            "seeds": seeds,
            "watchlist_size": args.watchlist_size,
            "top_symbols": args.top_symbols,
            "points_per_symbol": args.points_per_symbol,
            "profile": args.profile,
            "horizon_minutes": args.horizon_minutes,
            "win_mode": args.win_mode,
            "historical_eval_mode": True,
            "realtime_constraint_bypassed_for_backtest": True,
        },
        "precheck": {
            "prompt_0_path": os.path.relpath(PROMPT_0, ROOT).replace("\\", "/"),
            "prompt_1_path": os.path.relpath(PROMPT_1, ROOT).replace("\\", "/"),
            "prompt_0_has_watchlist_line": "WATCHLIST_SYMBOLS" in prompt0_text,
            "prompt_1_has_watchlist_line": "WATCHLIST_SYMBOLS" in prompt1_text,
            "self_check_pass": self_check_ok,
            "self_check_stdout_tail": out_sc[-1200:],
            "self_check_stderr_tail": err_sc[-600:],
        },
        "base_dataset": base_dataset or {},
        "runs": runs,
        "failed_seeds": failed_seeds,
        "aggregate": aggregate,
        "summary": {
            "top_signal": top_summary,
            "watchlist_all_rows": watch_summary,
        },
    }

    now_tag = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%d_%H%M%S_%f")
    out_json = os.path.join(LOGS_DIR, f"master_prompt_random_ts_eval_{now_tag}.json")
    out_md = os.path.join(LOGS_DIR, f"master_prompt_random_ts_eval_{now_tag}.md")

    # Must write json before limit assessment to include this run path exclusion.
    with open(out_json, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)

    report["limit_assessment"] = limit_assessment(report["summary"], out_json, actionable_min=40)
    with open(out_json, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)

    md = build_markdown(report, out_json=out_json, out_md=out_md)
    with open(out_md, "w", encoding="utf-8") as f:
        f.write(md)

    top = report["summary"]["top_signal"]
    watch = report["summary"]["watchlist_all_rows"]
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
    print(f"OUTPUT_PATH_JSON={os.path.relpath(out_json, ROOT).replace(os.sep, '/')}")
    print(f"OUTPUT_PATH_MD={os.path.relpath(out_md, ROOT).replace(os.sep, '/')}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
