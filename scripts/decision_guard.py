#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Deterministic post-analysis guard for 30~60m Binance.US spot decisions.

Purpose:
- Prevent repeat false LONG/SHORT when core conditions are incomplete.
- Enforce explicit risk blockers (short rebound, flow-depth divergence, entry mismatch).

Usage examples:
1) Validate latest BTC analysis:
   python .\scripts\decision_guard.py --symbol BTCUSDT

2) Validate a specific analysis file and enforce WAIT in-place when blocked:
   python .\scripts\decision_guard.py --analysis .\logs\analysis_BTCUSDT_xxx.txt --inplace
"""

from __future__ import annotations

import argparse
import datetime as dt
import glob
import json
import os
import re
import sys
from typing import Any, Dict, List, Optional, Tuple


ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
LOG_DIR = os.path.join(ROOT, "logs")

FLOW_LONG_MIN = 1.03
FLOW_SHORT_MAX = 0.97
DEPTH_DIVERGENCE_TH = 0.08
SHORT_REBOUND_RSI1H_MAX = 34.0
ENTRY_MISMATCH_ATR_MULT = 0.25


def utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z")


def safe_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        return float(x)
    except Exception:
        return None


def deep_get(obj: Any, path: str, default: Any = None) -> Any:
    cur = obj
    for part in path.split("."):
        if isinstance(cur, dict) and part in cur:
            cur = cur[part]
        else:
            return default
    return cur


def parse_key_val_lines(text: str) -> Dict[str, List[str]]:
    out: Dict[str, List[str]] = {}
    for line in text.splitlines():
        if "=" not in line:
            continue
        left, right = line.split("=", 1)
        key = left.strip().lstrip("\ufeff")
        val = right.strip()
        if not key:
            continue
        out.setdefault(key, []).append(val)
    return out


def latest_analysis_by_symbol(symbol: str) -> Optional[str]:
    pattern = os.path.join(LOG_DIR, f"analysis_{symbol.upper()}_*.txt")
    files = glob.glob(pattern)
    if not files:
        return None
    files.sort(key=lambda p: os.path.getmtime(p), reverse=True)
    return files[0]


def parse_analysis(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        text = f.read()

    kv = parse_key_val_lines(text)
    head = text.splitlines()[0] if text.splitlines() else ""
    head_kv = parse_key_val_lines(head.replace("|", "\n"))

    def first(d: Dict[str, List[str]], key: str) -> Optional[str]:
        vals = d.get(key)
        return vals[0] if vals else None

    def last(d: Dict[str, List[str]], key: str) -> Optional[str]:
        vals = d.get(key)
        return vals[-1] if vals else None

    snapshot_id = first(head_kv, "snapshot_id") or first(kv, "snapshot_id")
    symbol = last(kv, "symbol")
    action = (last(kv, "action") or "").upper()

    rec = {
        "path": os.path.abspath(path),
        "text": text,
        "snapshot_id": snapshot_id,
        "symbol": symbol,
        "action": action,
        "confidence": safe_float(last(kv, "confidence")),
        "data_quality": last(kv, "data_quality"),
        "entry": safe_float(last(kv, "entry")),
        "sl": safe_float(last(kv, "sl")),
        "tp1": safe_float(last(kv, "tp1")),
        "tp2": safe_float(last(kv, "tp2")),
        "snapshot_server_time_utc": first(head_kv, "snapshot_server_time_utc"),
        "analysis_time_utc": first(head_kv, "analysis_time_utc"),
    }
    return rec


def trend_state(ema20: Optional[float], ema50: Optional[float]) -> str:
    if ema20 is None or ema50 is None:
        return "UNKNOWN"
    if ema20 > ema50:
        return "UP"
    if ema20 < ema50:
        return "DOWN"
    return "SIDE"


def compute_guard(rec: Dict[str, Any], snapshot: Dict[str, Any]) -> Dict[str, Any]:
    symbol = str(rec.get("symbol") or deep_get(snapshot, "meta.symbol") or "")
    action = str(rec.get("action") or "").upper()

    ema20_5 = safe_float(deep_get(snapshot, "features.by_interval.5m.ema20_last"))
    ema50_5 = safe_float(deep_get(snapshot, "features.by_interval.5m.ema50_last"))
    ema20_15 = safe_float(deep_get(snapshot, "features.by_interval.15m.ema20_last"))
    ema50_15 = safe_float(deep_get(snapshot, "features.by_interval.15m.ema50_last"))
    ema20_1h = safe_float(deep_get(snapshot, "features.by_interval.1h.ema20_last"))
    ema50_1h = safe_float(deep_get(snapshot, "features.by_interval.1h.ema50_last"))

    trend_5m = trend_state(ema20_5, ema50_5)
    trend_15m = trend_state(ema20_15, ema50_15)
    trend_1h = trend_state(ema20_1h, ema50_1h)

    flow_ratio = safe_float(deep_get(snapshot, "flow.trade_features.buy_sell_ratio"))
    flow_imb = safe_float(deep_get(snapshot, "flow.trade_features.imbalance_qty"))
    depth_imb = safe_float(deep_get(snapshot, "features.order_book.imbalance"))

    rsi_1h = safe_float(deep_get(snapshot, "features.by_interval.1h.rsi14_last"))
    ret_1h = safe_float(deep_get(snapshot, "features.ticker_window.h1.price_change_pct"))

    last_price = safe_float(deep_get(snapshot, "market.ticker_24h.lastPrice"))
    bid = safe_float(deep_get(snapshot, "market.book_ticker.bidPrice"))
    ask = safe_float(deep_get(snapshot, "market.book_ticker.askPrice"))
    atr15 = safe_float(deep_get(snapshot, "features.by_interval.15m.atr14_last"))
    mid = None
    if bid is not None and ask is not None:
        mid = (bid + ask) / 2.0

    entry = safe_float(rec.get("entry"))
    if entry is None:
        entry = last_price
    entry_mid_diff = None
    entry_mid_diff_atr = None
    if entry is not None and mid is not None:
        entry_mid_diff = entry - mid
    if entry_mid_diff is not None and atr15 not in (None, 0):
        entry_mid_diff_atr = abs(entry_mid_diff) / atr15

    long_core = {
        "LONG_CORE_15m_up": trend_15m == "UP",
        "LONG_CORE_5m_up": trend_5m == "UP",
        "LONG_CORE_flow_ratio_ge_1.03": (flow_ratio is not None and flow_ratio >= FLOW_LONG_MIN),
        "LONG_CORE_flow_imb_positive": (flow_imb is not None and flow_imb > 0),
        "LONG_CORE_1h_not_down": trend_1h in ("UP", "SIDE"),
    }
    short_core = {
        "SHORT_CORE_15m_down": trend_15m == "DOWN",
        "SHORT_CORE_5m_down": trend_5m == "DOWN",
        "SHORT_CORE_flow_ratio_le_0.97": (flow_ratio is not None and flow_ratio <= FLOW_SHORT_MAX),
        "SHORT_CORE_flow_imb_negative": (flow_imb is not None and flow_imb < 0),
        "SHORT_CORE_1h_not_up": trend_1h in ("DOWN", "SIDE"),
    }

    hard_blockers: List[str] = []
    expected_action = action
    core_complete = True

    if action == "LONG":
        missing = [k for k, ok in long_core.items() if not ok]
        if missing:
            core_complete = False
            hard_blockers.append("long_core_incomplete:" + ",".join(missing))
    elif action == "SHORT":
        missing = [k for k, ok in short_core.items() if not ok]
        if missing:
            core_complete = False
            hard_blockers.append("short_core_incomplete:" + ",".join(missing))
    else:
        core_complete = False

    if action == "SHORT":
        if rsi_1h is not None and ret_1h is not None and rsi_1h <= SHORT_REBOUND_RSI1H_MAX and ret_1h >= 0:
            hard_blockers.append("short_rebound_risk")

    if flow_ratio is not None and depth_imb is not None:
        if flow_ratio <= FLOW_SHORT_MAX and depth_imb >= DEPTH_DIVERGENCE_TH:
            hard_blockers.append("flow_depth_divergence_bear_flow_vs_bid_depth")
        if flow_ratio >= FLOW_LONG_MIN and depth_imb <= -DEPTH_DIVERGENCE_TH:
            hard_blockers.append("flow_depth_divergence_bull_flow_vs_ask_depth")

    if entry_mid_diff_atr is not None and entry_mid_diff_atr > ENTRY_MISMATCH_ATR_MULT:
        hard_blockers.append("entry_price_mismatch_vs_book_mid")

    blocked = action in ("LONG", "SHORT") and len(hard_blockers) > 0
    if blocked:
        expected_action = "WAIT"

    return {
        "symbol": symbol,
        "analysis_action": action,
        "guard_action": expected_action,
        "blocked": blocked,
        "hard_blockers": hard_blockers,
        "core_complete": core_complete,
        "metrics": {
            "trend_5m": trend_5m,
            "trend_15m": trend_15m,
            "trend_1h": trend_1h,
            "flow_ratio": flow_ratio,
            "flow_imbalance_qty": flow_imb,
            "depth_imbalance": depth_imb,
            "rsi_1h": rsi_1h,
            "ret_1h_pct": ret_1h,
            "entry": entry,
            "book_mid": mid,
            "entry_mid_diff": entry_mid_diff,
            "entry_mid_diff_atr": entry_mid_diff_atr,
            "atr15": atr15,
        },
        "long_core": long_core,
        "short_core": short_core,
    }


def rewrite_final_decision(text: str, action: str, confidence: int, data_quality: Optional[str], wait_reason: str, note: str) -> str:
    lines = text.splitlines()
    block = [
        "[FINAL_DECISION]",
        f"action={action}",
        f"confidence={confidence}",
        f"data_quality={data_quality or 'UNKNOWN'}",
        f"wait_reason_if_any={wait_reason}",
        f"execution_note={note}",
    ]

    idx = None
    for i, line in enumerate(lines):
        if line.strip() == "[FINAL_DECISION]":
            idx = i
            break

    if idx is None:
        if lines and lines[-1].strip():
            lines.append("")
        lines.extend(block)
        return "\n".join(lines) + "\n"

    j = idx + 1
    while j < len(lines):
        s = lines[j].strip()
        if s.startswith("[") and s.endswith("]"):
            break
        j += 1

    lines = lines[:idx] + block + lines[j:]
    return "\n".join(lines) + "\n"


def main() -> int:
    ap = argparse.ArgumentParser(description="Deterministic post-analysis guard.")
    ap.add_argument("--analysis", default="", help="Path to analysis_*.txt")
    ap.add_argument("--symbol", default="", help="Use latest analysis_<SYMBOL>_*.txt from logs/")
    ap.add_argument("--inplace", action="store_true", help="Rewrite analysis in-place to WAIT when blocked")
    args = ap.parse_args()

    analysis_path = (args.analysis or "").strip()
    if not analysis_path:
        symbol = (args.symbol or "").strip().upper()
        if not symbol:
            print("status=FAIL")
            print("error=missing_analysis_or_symbol")
            return 2
        analysis_path = latest_analysis_by_symbol(symbol) or ""

    if not analysis_path or not os.path.exists(analysis_path):
        print("status=FAIL")
        print(f"error=analysis_not_found:{analysis_path}")
        return 2

    rec = parse_analysis(analysis_path)
    snapshot_id = rec.get("snapshot_id")
    if not snapshot_id:
        print("status=FAIL")
        print("error=missing_snapshot_id_in_analysis")
        return 2

    sid = str(snapshot_id).strip()
    candidates = [
        os.path.join(LOG_DIR, sid),
    ]
    if not sid.endswith(".json"):
        candidates.append(os.path.join(LOG_DIR, sid + ".json"))
    if not sid.startswith("snapshot_"):
        if sid.endswith(".json"):
            candidates.append(os.path.join(LOG_DIR, "snapshot_" + sid))
        else:
            candidates.append(os.path.join(LOG_DIR, "snapshot_" + sid + ".json"))
    if sid.startswith("snapshot_") and not sid.endswith(".json"):
        candidates.append(os.path.join(LOG_DIR, sid + ".json"))

    snapshot_path = ""
    for c in candidates:
        if os.path.exists(c):
            snapshot_path = c
            break

    if not snapshot_path:
        sym = str(rec.get("symbol") or "").upper().strip()
        ts_utc = str(rec.get("snapshot_server_time_utc") or "").strip()
        ts_ms = None
        if ts_utc:
            try:
                ts_ms = int(dt.datetime.fromisoformat(ts_utc.replace("Z", "+00:00")).timestamp() * 1000)
            except Exception:
                ts_ms = None
        if sym and ts_ms is not None:
            patt = os.path.join(LOG_DIR, f"snapshot_{sym}_*.json")
            cands = glob.glob(patt)
            best: Optional[Tuple[int, str]] = None
            for c in cands:
                bn = os.path.basename(c)
                m = re.match(rf"^snapshot_{re.escape(sym)}_(\d+)\.json$", bn)
                if not m:
                    continue
                try:
                    ms = int(m.group(1))
                except Exception:
                    continue
                dist = abs(ms - ts_ms)
                if best is None or dist < best[0]:
                    best = (dist, c)
            # Accept nearest snapshot within 20 minutes.
            if best is not None and best[0] <= 20 * 60 * 1000:
                snapshot_path = best[1]

    if not snapshot_path:
        print("status=FAIL")
        print(f"error=snapshot_not_found:{os.path.join(LOG_DIR, sid)}")
        return 2

    with open(snapshot_path, "r", encoding="utf-8") as f:
        snapshot = json.load(f)

    guard = compute_guard(rec, snapshot)
    status = "PASS"
    note = "guard_pass"

    if guard["blocked"]:
        if args.inplace:
            status = "ENFORCED_WAIT"
            note = "guard_enforced_wait"
        else:
            status = "FAIL"
            note = "guard_blocked_action"

    out_path = os.path.abspath(analysis_path)
    if guard["blocked"] and args.inplace:
        orig_conf = safe_float(rec.get("confidence"))
        forced_conf = min(35, int(orig_conf)) if orig_conf is not None else 35
        wait_reason = "guard_blocked:" + ",".join(guard["hard_blockers"])
        patched = rewrite_final_decision(
            rec["text"],
            action="WAIT",
            confidence=forced_conf,
            data_quality=rec.get("data_quality"),
            wait_reason=wait_reason,
            note=note,
        )
        with open(analysis_path, "w", encoding="utf-8") as f:
            f.write(patched)

    os.makedirs(LOG_DIR, exist_ok=True)
    now = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%d_%H%M%S_%f")
    symbol = guard.get("symbol") or "UNKNOWN"
    guard_report_path = os.path.join(LOG_DIR, f"decision_guard_{symbol}_{now}.json")
    report = {
        "run_time_utc": utc_now(),
        "status": status,
        "analysis_path": out_path,
        "snapshot_path": os.path.abspath(snapshot_path),
        "guard": guard,
    }
    with open(guard_report_path, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)

    print(f"status={status}")
    print(f"analysis_path={out_path}")
    print(f"guard_report={os.path.abspath(guard_report_path)}")
    print(f"analysis_action={guard.get('analysis_action')}")
    print(f"guard_action={guard.get('guard_action')}")
    print(f"blocked={guard.get('blocked')}")
    print(f"hard_blockers={guard.get('hard_blockers')}")

    if status == "FAIL":
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
