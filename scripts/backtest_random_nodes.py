#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Random-node historical backtest for current 30~60m strategy (Binance.US spot).

Outputs:
- logs/backtest_random_nodes_<ts>.json
- logs/backtest_random_nodes_<ts>.md
"""

from __future__ import annotations

import bisect
import dataclasses
import datetime as dt
import itertools
import json
import math
import os
import random
import statistics
import time
from typing import Any, Dict, List, Optional, Tuple

import requests


BASE_URL = "https://api.binance.us"
INTERVAL_MS = {
    "1m": 60_000,
    "3m": 180_000,
    "5m": 300_000,
    "15m": 900_000,
    "30m": 1_800_000,
    "1h": 3_600_000,
}

def env_int(name: str, default: int) -> int:
    v = os.getenv(name)
    if v is None:
        return default
    try:
        return int(v)
    except Exception:
        return default


def env_float(name: str, default: float) -> float:
    v = os.getenv(name)
    if v is None:
        return default
    try:
        return float(v)
    except Exception:
        return default


def env_str(name: str, default: str) -> str:
    v = os.getenv(name)
    if v is None:
        return default
    return str(v).strip()


def parse_symbol_list(s: str) -> List[str]:
    if not s:
        return []
    tokens = [x.strip().upper() for x in s.replace(",", " ").split()]
    seen: set = set()
    out: List[str] = []
    for t in tokens:
        if not t or t in seen:
            continue
        seen.add(t)
        out.append(t)
    return out


# Keep runtime reasonable while still broad enough to be meaningful.
TOP_SYMBOLS = env_int("BACKTEST_TOP_SYMBOLS", 8)
POINTS_PER_SYMBOL = env_int("BACKTEST_POINTS_PER_SYMBOL", 45)
FORCED_SYMBOLS = parse_symbol_list(env_str("BACKTEST_SYMBOLS", ""))

FETCH_BARS_5M = env_int("BACKTEST_FETCH_BARS_5M", 3000)
FETCH_BARS_15M = env_int("BACKTEST_FETCH_BARS_15M", 1600)
FETCH_BARS_1H = env_int("BACKTEST_FETCH_BARS_1H", 900)

HORIZON_MINUTES = env_int("BACKTEST_HORIZON_MINUTES", 60)
HORIZON_BARS_5M = max(1, HORIZON_MINUTES // 5)

RANDOM_SEED = env_int("BACKTEST_SEED", 20260220)
BACKTEST_PROFILE = env_str("BACKTEST_PROFILE", "balanced").lower()
TARGET_WIN_RATE = env_float("BACKTEST_TARGET_WIN_RATE", 0.70 if BACKTEST_PROFILE == "precision" else 0.55)
MIN_COVERAGE = env_float("BACKTEST_MIN_COVERAGE", 0.10 if BACKTEST_PROFILE == "precision" else 0.25)
MASTER_TOP_SCORE_MARGIN = env_float("BACKTEST_MASTER_TOP_SCORE_MARGIN", 0.20 if BACKTEST_PROFILE == "precision" else 0.0)
ENABLE_SYMBOL_GATE = env_int("BACKTEST_ENABLE_SYMBOL_GATE", 1 if BACKTEST_PROFILE == "precision" else 0)
SYMBOL_GATE_MIN_ACTIONABLE = env_int("BACKTEST_SYMBOL_GATE_MIN_ACTIONABLE", 8)
SYMBOL_GATE_MIN_WIN_RATE = env_float("BACKTEST_SYMBOL_GATE_MIN_WIN_RATE", 0.62)
SYMBOL_GATE_MIN_SYMBOLS = env_int("BACKTEST_SYMBOL_GATE_MIN_SYMBOLS", 3)
MASTER_TS_NODES = env_int("BACKTEST_MASTER_TS_NODES", 40)
MASTER_WATCHLIST_SIZE = env_int("BACKTEST_MASTER_WATCHLIST_SIZE", 5)
MASTER_MIN_TOP_COUNT = env_int("BACKTEST_MASTER_MIN_TOP_COUNT", max(10, int(MASTER_TS_NODES * 0.2)))
ENABLE_PER_SYMBOL_OPT = env_int("BACKTEST_ENABLE_PER_SYMBOL_OPT", 0 if BACKTEST_PROFILE == "precision" else 1)
ENABLE_MASTER_REFINE = env_int("BACKTEST_ENABLE_MASTER_REFINE", 0 if BACKTEST_PROFILE == "precision" else 1)
HTTP_TIMEOUT = 20
HTTP_MAX_RETRIES = 4

HTTP_SESSION = requests.Session()
HTTP_SESSION.headers.update(
    {
        "User-Agent": "binance-us-backtest/2.0",
        "Accept": "application/json",
    }
)


@dataclasses.dataclass
class KBar:
    open_time_ms: int
    close_time_ms: int
    open: float
    high: float
    low: float
    close: float
    volume: float


@dataclasses.dataclass
class Point:
    symbol: str
    ts_ms: int
    entry: float
    atr15: float
    ema20_5: float
    ema50_5: float
    ema20_15: float
    ema50_15: float
    ema20_1h: float
    ema50_1h: float
    rsi5: float
    rsi15: float
    atr_rel: float
    trend_conflict: bool
    future_bars_5m: List[KBar]


def utc_iso(ms: int) -> str:
    return dt.datetime.fromtimestamp(ms / 1000.0, tz=dt.timezone.utc).isoformat().replace("+00:00", "Z")


def safe_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        return float(x)
    except Exception:
        return None


def sign(x: float) -> int:
    if x > 0:
        return 1
    if x < 0:
        return -1
    return 0


def ema_last(values: List[float], period: int) -> Optional[float]:
    if period <= 0 or len(values) < period:
        return None
    k = 2.0 / (period + 1.0)
    ema = sum(values[:period]) / period
    for v in values[period:]:
        ema = (v - ema) * k + ema
    return ema


def rsi_last(closes: List[float], period: int = 14) -> Optional[float]:
    if len(closes) < period + 1:
        return None
    gains: List[float] = []
    losses: List[float] = []
    for i in range(1, period + 1):
        ch = closes[i] - closes[i - 1]
        gains.append(max(ch, 0.0))
        losses.append(max(-ch, 0.0))
    avg_gain = sum(gains) / period
    avg_loss = sum(losses) / period
    for i in range(period + 1, len(closes)):
        ch = closes[i] - closes[i - 1]
        gain = max(ch, 0.0)
        loss = max(-ch, 0.0)
        avg_gain = (avg_gain * (period - 1) + gain) / period
        avg_loss = (avg_loss * (period - 1) + loss) / period
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))


def atr_last(highs: List[float], lows: List[float], closes: List[float], period: int = 14) -> Optional[float]:
    if len(highs) != len(lows) or len(lows) != len(closes):
        return None
    n = len(closes)
    if n < period + 1:
        return None
    trs: List[float] = []
    for i in range(1, n):
        trs.append(
            max(
                highs[i] - lows[i],
                abs(highs[i] - closes[i - 1]),
                abs(lows[i] - closes[i - 1]),
            )
        )
    if len(trs) < period:
        return None
    atr = sum(trs[:period]) / period
    for tr in trs[period:]:
        atr = (atr * (period - 1) + tr) / period
    return atr


def wilson_lower_bound(successes: int, total: int, z: float = 1.96) -> float:
    if total <= 0:
        return 0.0
    p = successes / total
    z2 = z * z
    denom = 1.0 + z2 / total
    center = p + z2 / (2.0 * total)
    margin = z * math.sqrt((p * (1.0 - p) / total) + (z2 / (4.0 * total * total)))
    return max(0.0, (center - margin) / denom)


def http_get_json(path: str, params: Dict[str, Any], timeout: int = HTTP_TIMEOUT, max_retries: int = HTTP_MAX_RETRIES) -> Any:
    url = BASE_URL + path
    last_error: Optional[str] = None
    for attempt in range(max_retries + 1):
        try:
            r = HTTP_SESSION.get(url, params=params, timeout=timeout)
            status = r.status_code
            if status in (418, 429, 500, 502, 503, 504):
                last_error = f"HTTP {status}: {(r.text or '')[:200]}"
                if attempt < max_retries:
                    retry_after = safe_float(r.headers.get("Retry-After"))
                    sleep_sec = retry_after if retry_after is not None else (2 ** attempt) * 0.9
                    time.sleep(max(0.5, float(sleep_sec)))
                    continue
            r.raise_for_status()
            return r.json()
        except requests.RequestException as ex:
            last_error = str(ex)
            if attempt < max_retries:
                time.sleep((2 ** attempt) * 0.9)
                continue
    raise RuntimeError(f"HTTP request failed after retries: path={path}, params={params}, err={last_error}")


def fetch_top_symbols(limit: int) -> List[str]:
    if FORCED_SYMBOLS:
        return FORCED_SYMBOLS[: max(1, limit)]

    tickers = http_get_json("/api/v3/ticker/24hr", {})
    if not isinstance(tickers, list):
        raise RuntimeError("Unexpected ticker payload")
    rows: List[Tuple[str, float]] = []
    fallback_rows: List[Tuple[str, float]] = []
    for t in tickers:
        if not isinstance(t, dict):
            continue
        symbol = str(t.get("symbol") or "")
        if not symbol.endswith("USDT"):
            continue
        # Exclude obvious stablecoin pairs / pegged instruments for momentum scalp.
        if symbol in {"USDCUSDT", "BUSDUSDT", "USDTUSD"}:
            continue
        quote_vol = safe_float(t.get("quoteVolume")) or 0.0
        if quote_vol <= 0:
            continue
        fallback_rows.append((symbol, quote_vol))
        last_p = safe_float(t.get("lastPrice"))
        open_p = safe_float(t.get("openPrice"))
        high_p = safe_float(t.get("highPrice"))
        low_p = safe_float(t.get("lowPrice"))
        pcp = abs(safe_float(t.get("priceChangePercent")) or 0.0)
        range_pct = 0.0
        if high_p is not None and low_p is not None and open_p not in (None, 0):
            range_pct = abs((high_p - low_p) / open_p * 100.0)
        # Balanced mode: keep symbols with enough movement and liquidity.
        if pcp < 0.20 and range_pct < 0.50:
            continue
        if quote_vol < 50_000:
            continue
        if last_p is None:
            continue
        rows.append((symbol, quote_vol))
    rows.sort(key=lambda x: x[1], reverse=True)
    chosen = [s for s, _ in rows[:limit]]
    if len(chosen) < limit:
        fallback_rows.sort(key=lambda x: x[1], reverse=True)
        for s, _ in fallback_rows:
            if s in chosen:
                continue
            chosen.append(s)
            if len(chosen) >= limit:
                break
    return chosen[:limit]


def parse_kline_row(row: List[Any]) -> Optional[KBar]:
    if not isinstance(row, list) or len(row) < 7:
        return None
    try:
        return KBar(
            open_time_ms=int(row[0]),
            close_time_ms=int(row[6]),
            open=float(row[1]),
            high=float(row[2]),
            low=float(row[3]),
            close=float(row[4]),
            volume=float(row[5]),
        )
    except Exception:
        return None


def fetch_klines(symbol: str, interval: str, total_bars: int) -> List[KBar]:
    chunks: List[List[KBar]] = []
    end_time: Optional[int] = None
    fetched = 0
    while fetched < total_bars:
        take = min(1000, total_bars - fetched)
        params: Dict[str, Any] = {"symbol": symbol, "interval": interval, "limit": take}
        if end_time is not None:
            params["endTime"] = end_time
        raw = http_get_json("/api/v3/klines", params)
        if not isinstance(raw, list) or not raw:
            break
        bars = [b for b in (parse_kline_row(r) for r in raw) if b is not None]
        if not bars:
            break
        chunks.append(bars)
        fetched += len(bars)
        oldest = min(b.open_time_ms for b in bars)
        end_time = oldest - 1
        if len(bars) < take:
            break

    out: List[KBar] = []
    for chunk in reversed(chunks):
        out.extend(chunk)
    out.sort(key=lambda x: x.open_time_ms)
    dedup: Dict[int, KBar] = {}
    for b in out:
        dedup[b.open_time_ms] = b
    seq = list(sorted(dedup.values(), key=lambda x: x.open_time_ms))
    if len(seq) > total_bars:
        seq = seq[-total_bars:]
    return seq


def index_le(times: List[int], ts: int) -> int:
    i = bisect.bisect_right(times, ts) - 1
    return i


def build_points_for_symbol(symbol: str, bars5: List[KBar], bars15: List[KBar], bars1h: List[KBar], max_points: int, rng: random.Random) -> List[Point]:
    if len(bars5) < 220 or len(bars15) < 180 or len(bars1h) < 90:
        return []

    t5 = [b.close_time_ms for b in bars5]
    t1h = [b.close_time_ms for b in bars1h]

    candidates: List[Point] = []
    min_hist_15 = 120
    for i in range(min_hist_15, len(bars15)):
        ts = bars15[i].close_time_ms
        idx5 = index_le(t5, ts)
        idx1h = index_le(t1h, ts)
        if idx5 < 160 or idx1h < 60:
            continue
        if idx5 + HORIZON_BARS_5M >= len(bars5):
            continue

        c5 = [b.close for b in bars5[: idx5 + 1]]
        h5 = [b.high for b in bars5[: idx5 + 1]]
        l5 = [b.low for b in bars5[: idx5 + 1]]

        c15 = [b.close for b in bars15[: i + 1]]
        h15 = [b.high for b in bars15[: i + 1]]
        l15 = [b.low for b in bars15[: i + 1]]

        c1h = [b.close for b in bars1h[: idx1h + 1]]

        ema20_5 = ema_last(c5, 20)
        ema50_5 = ema_last(c5, 50)
        ema20_15 = ema_last(c15, 20)
        ema50_15 = ema_last(c15, 50)
        ema20_1h = ema_last(c1h, 20)
        ema50_1h = ema_last(c1h, 50)
        rsi5 = rsi_last(c5, 14)
        rsi15 = rsi_last(c15, 14)
        atr15 = atr_last(h15, l15, c15, 14)
        if None in (ema20_5, ema50_5, ema20_15, ema50_15, ema20_1h, ema50_1h, rsi5, rsi15, atr15):
            continue
        if atr15 is None or atr15 <= 0:
            continue

        entry = bars15[i].close
        atr_rel = float(atr15 / entry) if entry > 0 else 0.0

        sig5 = sign(float(ema20_5 - ema50_5))
        sig15 = sign(float(ema20_15 - ema50_15))
        sig1h = sign(float(ema20_1h - ema50_1h))
        trend_conflict = (sig5 != sig15) or (sig1h != sig15)

        future = bars5[idx5 + 1 : idx5 + 1 + HORIZON_BARS_5M]
        if len(future) < HORIZON_BARS_5M:
            continue

        candidates.append(
            Point(
                symbol=symbol,
                ts_ms=ts,
                entry=entry,
                atr15=float(atr15),
                ema20_5=float(ema20_5),
                ema50_5=float(ema50_5),
                ema20_15=float(ema20_15),
                ema50_15=float(ema50_15),
                ema20_1h=float(ema20_1h),
                ema50_1h=float(ema50_1h),
                rsi5=float(rsi5),
                rsi15=float(rsi15),
                atr_rel=atr_rel,
                trend_conflict=trend_conflict,
                future_bars_5m=future,
            )
        )

    if len(candidates) <= max_points:
        return candidates
    return rng.sample(candidates, max_points)


def predict_action(p: Point, params: Dict[str, float]) -> Tuple[str, float, int]:
    s15 = sign(p.ema20_15 - p.ema50_15)
    s5 = sign(p.ema20_5 - p.ema50_5)
    s1h = sign(p.ema20_1h - p.ema50_1h)
    rsi15_term = max(-2.0, min(2.0, (p.rsi15 - 50.0) / 10.0))
    rsi5_term = max(-2.0, min(2.0, (p.rsi5 - 50.0) / 10.0))
    px_term = sign(p.entry - p.ema20_15)

    score = (
        params["w15"] * s15
        + params["w5"] * s5
        + params["w1h"] * s1h
        + params["wrsi15"] * rsi15_term
        + params["wrsi5"] * rsi5_term
        + params["wpx"] * px_term
    )

    th = float(params["threshold"])
    th_long = float(params.get("threshold_long", th))
    th_short = float(params.get("threshold_short", th))
    if score >= th_long:
        action = "LONG"
    elif score <= -th_short:
        action = "SHORT"
    else:
        action = "WAIT"

    allowed_symbols = params.get("allowed_symbols")
    if action != "WAIT" and isinstance(allowed_symbols, (list, tuple, set)) and p.symbol not in allowed_symbols:
        action = "WAIT"

    min_abs_score = params.get("min_abs_score", th)
    if action != "WAIT" and abs(score) < min_abs_score:
        action = "WAIT"

    min_atr_rel = params.get("min_atr_rel", 0.0)
    max_atr_rel = params.get("max_atr_rel", 99.0)
    if action != "WAIT" and (p.atr_rel < min_atr_rel or p.atr_rel > max_atr_rel):
        action = "WAIT"

    entry_stretch = abs(p.entry - p.ema20_15) / max(1e-9, p.atr15)
    max_entry_stretch = params.get("max_entry_stretch", 99.0)
    if action != "WAIT" and entry_stretch > max_entry_stretch:
        action = "WAIT"

    require_mtf_align = params.get("require_mtf_align", 0.0) >= 0.5
    if action != "WAIT" and require_mtf_align:
        mtf_aligned = (s15 != 0) and (s5 == s15) and (s1h == s15)
        if not mtf_aligned:
            action = "WAIT"

    trend_gap_15 = abs(p.ema20_15 - p.ema50_15) / max(1e-9, p.entry)
    trend_gap_1h = abs(p.ema20_1h - p.ema50_1h) / max(1e-9, p.entry)
    if action != "WAIT" and trend_gap_15 < float(params.get("min_trend_gap_15", 0.0)):
        action = "WAIT"
    if action != "WAIT" and trend_gap_1h < float(params.get("min_trend_gap_1h", 0.0)):
        action = "WAIT"

    if action == "LONG":
        if not (
            params.get("long_rsi15_min", 0.0) <= p.rsi15 <= params.get("long_rsi15_max", 100.0)
            and params.get("long_rsi5_min", 0.0) <= p.rsi5 <= params.get("long_rsi5_max", 100.0)
        ):
            action = "WAIT"
    elif action == "SHORT":
        if not (
            params.get("short_rsi15_min", 0.0) <= p.rsi15 <= params.get("short_rsi15_max", 100.0)
            and params.get("short_rsi5_min", 0.0) <= p.rsi5 <= params.get("short_rsi5_max", 100.0)
        ):
            action = "WAIT"

    confidence = int(min(99.0, 50.0 + abs(score) * 10.0))
    return action, score, confidence


def eval_trade(p: Point, action: str, params: Dict[str, float]) -> Dict[str, Any]:
    out: Dict[str, Any] = {"action": action}
    final_close = p.future_bars_5m[-1].close
    out["final_return"] = (final_close - p.entry) / p.entry if p.entry > 0 else 0.0

    if action == "WAIT":
        vol_ref = max(1e-9, p.atr15 / p.entry)
        move = abs(out["final_return"])
        out["outcome"] = "wait_correct" if move <= params["wait_mult"] * vol_ref else "wait_wrong"
        out["stop_hit_bar"] = None
        return out

    stop_mult = params["stop_mult"]
    tp_mult = params["tp_mult"]
    stop_hit_bar: Optional[int] = None
    tp_hit_bar: Optional[int] = None

    if action == "LONG":
        stop = p.entry - stop_mult * p.atr15
        tp = p.entry + tp_mult * p.atr15
        for i, b in enumerate(p.future_bars_5m, start=1):
            both = (b.low <= stop) and (b.high >= tp)
            if both:
                stop_hit_bar = i
                tp_hit_bar = i
                break
            if b.low <= stop:
                stop_hit_bar = i
                break
            if b.high >= tp:
                tp_hit_bar = i
                break
    else:
        stop = p.entry + stop_mult * p.atr15
        tp = p.entry - tp_mult * p.atr15
        for i, b in enumerate(p.future_bars_5m, start=1):
            both = (b.high >= stop) and (b.low <= tp)
            if both:
                stop_hit_bar = i
                tp_hit_bar = i
                break
            if b.high >= stop:
                stop_hit_bar = i
                break
            if b.low <= tp:
                tp_hit_bar = i
                break

    out["stop_hit_bar"] = stop_hit_bar
    out["tp_hit_bar"] = tp_hit_bar

    if stop_hit_bar is not None and tp_hit_bar is not None:
        out["outcome"] = "loss_stop"
    elif stop_hit_bar is not None:
        out["outcome"] = "loss_stop"
    elif tp_hit_bar is not None:
        out["outcome"] = "win_tp"
    else:
        if action == "LONG":
            out["outcome"] = "timeout_win" if out["final_return"] > 0 else "timeout_loss"
        else:
            out["outcome"] = "timeout_win" if out["final_return"] < 0 else "timeout_loss"
    return out


def evaluate(points: List[Point], params: Dict[str, float], atr_noise_cut: float) -> Dict[str, Any]:
    total = len(points)
    actionable = 0
    wins = 0
    losses = 0
    waits = 0
    wait_correct = 0
    directional_correct = 0
    long_actionable = 0
    short_actionable = 0
    long_wins = 0
    short_wins = 0
    reason_counts: Dict[str, int] = {
        "weak_signal": 0,
        "mtf_conflict": 0,
        "overextended_entry": 0,
        "early_stopout": 0,
        "high_noise_regime": 0,
    }
    sample_rows: List[Dict[str, Any]] = []

    for p in points:
        action, score, conf = predict_action(p, params)
        res = eval_trade(p, action, params)
        outcome = res["outcome"]

        row = {
            "symbol": p.symbol,
            "ts_utc": utc_iso(p.ts_ms),
            "action": action,
            "score": round(score, 4),
            "confidence": conf,
            "outcome": outcome,
            "final_return": round(res["final_return"], 6),
            "rsi15": round(p.rsi15, 3),
            "rsi5": round(p.rsi5, 3),
            "atr_rel": round(p.atr_rel, 6),
        }
        sample_rows.append(row)

        if action == "WAIT":
            waits += 1
            if outcome == "wait_correct":
                wait_correct += 1
            continue

        actionable += 1
        if action == "LONG":
            long_actionable += 1
        elif action == "SHORT":
            short_actionable += 1
        dir_ok = (res["final_return"] > 0 and action == "LONG") or (res["final_return"] < 0 and action == "SHORT")
        if dir_ok:
            directional_correct += 1

        is_win = outcome in ("win_tp", "timeout_win")
        if is_win:
            wins += 1
            if action == "LONG":
                long_wins += 1
            elif action == "SHORT":
                short_wins += 1
        else:
            losses += 1
            if abs(score) < (params["threshold"] + 0.75):
                reason_counts["weak_signal"] += 1
            if p.trend_conflict:
                reason_counts["mtf_conflict"] += 1
            if (action == "LONG" and p.rsi15 >= 68) or (action == "SHORT" and p.rsi15 <= 32):
                reason_counts["overextended_entry"] += 1
            stop_hit_bar = res.get("stop_hit_bar")
            if isinstance(stop_hit_bar, int) and stop_hit_bar <= 3:
                reason_counts["early_stopout"] += 1
            if p.atr_rel >= atr_noise_cut:
                reason_counts["high_noise_regime"] += 1

    actionable_win_rate = (wins / actionable) if actionable else 0.0
    long_win_rate = (long_wins / long_actionable) if long_actionable else 0.0
    short_win_rate = (short_wins / short_actionable) if short_actionable else 0.0
    directional_acc = (directional_correct / actionable) if actionable else 0.0
    coverage = actionable / total if total else 0.0
    overall_acc = (wins + wait_correct) / total if total else 0.0
    timeout_share = (
        sum(1 for r in sample_rows if r["outcome"] in ("timeout_win", "timeout_loss")) / actionable if actionable else 0.0
    )

    by_symbol: Dict[str, Dict[str, Any]] = {}
    for r in sample_rows:
        s = r["symbol"]
        if s not in by_symbol:
            by_symbol[s] = {"total": 0, "actionable": 0, "wins": 0, "losses": 0, "waits": 0, "wait_correct": 0}
        b = by_symbol[s]
        b["total"] += 1
        if r["action"] == "WAIT":
            b["waits"] += 1
            if r["outcome"] == "wait_correct":
                b["wait_correct"] += 1
        else:
            b["actionable"] += 1
            if r["outcome"] in ("win_tp", "timeout_win"):
                b["wins"] += 1
            else:
                b["losses"] += 1
    for b in by_symbol.values():
        act = b["actionable"]
        total_s = b["total"]
        b["actionable_win_rate"] = (b["wins"] / act) if act else 0.0
        b["coverage"] = (act / total_s) if total_s else 0.0

    # Binomial normal approximation.
    ci_half = 0.0
    if actionable > 0:
        ci_half = 1.96 * math.sqrt(max(0.0, actionable_win_rate * (1 - actionable_win_rate)) / actionable)
    long_ci_half = 1.96 * math.sqrt(max(0.0, long_win_rate * (1 - long_win_rate)) / long_actionable) if long_actionable > 0 else 0.0
    short_ci_half = 1.96 * math.sqrt(max(0.0, short_win_rate * (1 - short_win_rate)) / short_actionable) if short_actionable > 0 else 0.0

    return {
        "total": total,
        "actionable": actionable,
        "long_actionable": long_actionable,
        "short_actionable": short_actionable,
        "waits": waits,
        "wins": wins,
        "long_wins": long_wins,
        "short_wins": short_wins,
        "losses": losses,
        "wait_correct": wait_correct,
        "coverage": coverage,
        "actionable_win_rate": actionable_win_rate,
        "actionable_win_rate_ci95": [max(0.0, actionable_win_rate - ci_half), min(1.0, actionable_win_rate + ci_half)],
        "long_win_rate": long_win_rate,
        "long_win_rate_ci95": [max(0.0, long_win_rate - long_ci_half), min(1.0, long_win_rate + long_ci_half)],
        "short_win_rate": short_win_rate,
        "short_win_rate_ci95": [max(0.0, short_win_rate - short_ci_half), min(1.0, short_win_rate + short_ci_half)],
        "directional_accuracy": directional_acc,
        "overall_accuracy": overall_acc,
        "timeout_share": timeout_share,
        "loss_reasons": reason_counts,
        "by_symbol": by_symbol,
        "rows": sample_rows,
    }


def point_scan_score(p: Point) -> float:
    s15 = sign(p.ema20_15 - p.ema50_15)
    s5 = sign(p.ema20_5 - p.ema50_5)
    s1h = sign(p.ema20_1h - p.ema50_1h)
    align_bonus = 1.0 if (s15 != 0 and s5 == s15 and s1h == s15) else 0.0
    trend_strength = abs((p.ema20_15 - p.ema50_15) / max(1e-9, p.entry))
    rsi_center_penalty = abs(p.rsi15 - 50.0) / 50.0
    vol_term = min(1.0, p.atr_rel / 0.02)
    score = (
        45.0 * align_bonus
        + 25.0 * min(1.0, trend_strength * 300.0)
        + 20.0 * vol_term
        - 10.0 * rsi_center_penalty
    )
    return float(score)


def evaluate_master_prompt_timestamps(
    points: List[Point],
    params: Dict[str, float],
    rng: random.Random,
    num_timestamps: int,
    watchlist_size: int,
) -> Dict[str, Any]:
    allowed_symbols_raw = params.get("allowed_symbols")
    allowed_symbols: Optional[set] = None
    if isinstance(allowed_symbols_raw, (list, tuple, set)):
        allowed_symbols = {str(x) for x in allowed_symbols_raw}
    scan_use_allowed_only = params.get("scan_use_allowed_only", 0.0) >= 0.5

    by_ts: Dict[int, Dict[str, Point]] = {}
    for p in points:
        by_ts.setdefault(p.ts_ms, {})
        by_ts[p.ts_ms][p.symbol] = p

    if scan_use_allowed_only and allowed_symbols:
        eligible_ts = [ts for ts, m in by_ts.items() if any(sym in allowed_symbols for sym in m.keys())]
    else:
        eligible_ts = [ts for ts, m in by_ts.items() if len(m) >= watchlist_size]
    if not eligible_ts:
        return {
            "total_timestamps": 0,
            "eligible_timestamps": 0,
            "sampled_timestamps": 0,
            "watchlist_size": watchlist_size,
            "top_signal_count": 0,
            "top_signal_wins": 0,
            "top_signal_win_rate": 0.0,
            "top_signal_win_rate_ci95": [0.0, 0.0],
            "top_signal_coverage": 0.0,
            "top_decision_distribution": {
                "total": 0,
                "long_count": 0,
                "short_count": 0,
                "wait_count": 0,
                "long_ratio": 0.0,
                "short_ratio": 0.0,
                "wait_ratio": 0.0,
            },
            "top_long_win_rate": 0.0,
            "top_short_win_rate": 0.0,
            "watchlist_actionable_count": 0,
            "watchlist_actionable_wins": 0,
            "watchlist_actionable_win_rate": 0.0,
            "watchlist_actionable_win_rate_ci95": [0.0, 0.0],
            "watchlist_action_distribution": {
                "total": 0,
                "long_count": 0,
                "short_count": 0,
                "wait_count": 0,
                "long_ratio": 0.0,
                "short_ratio": 0.0,
                "wait_ratio": 0.0,
            },
            "watchlist_long_win_rate": 0.0,
            "watchlist_short_win_rate": 0.0,
            "loss_reasons": {},
            "sample_nodes": [],
        }

    sampled = sorted(rng.sample(eligible_ts, min(num_timestamps, len(eligible_ts))))
    top_count = 0
    top_win = 0
    top_long_count = 0
    top_short_count = 0
    top_long_wins = 0
    top_short_wins = 0
    top_wait_count = 0
    watch_act = 0
    watch_win = 0
    watch_total = 0
    watch_long_count = 0
    watch_short_count = 0
    watch_wait_count = 0
    watch_long_wins = 0
    watch_short_wins = 0
    top_rows: List[Dict[str, Any]] = []
    reason_counts: Dict[str, int] = {
        "weak_signal": 0,
        "mtf_conflict": 0,
        "overextended_entry": 0,
        "early_stopout": 0,
        "high_noise_regime": 0,
        "no_actionable_signal": 0,
    }

    for ts in sampled:
        points_map = by_ts.get(ts, {})
        candidates = list(points_map.values())
        if scan_use_allowed_only and allowed_symbols:
            candidates = [p for p in candidates if p.symbol in allowed_symbols]
        if not candidates:
            reason_counts["no_actionable_signal"] += 1
            continue
        require_regime_align = float(params.get("require_regime_align", 0.0)) >= 0.5
        regime_sign = 0
        if require_regime_align:
            regime_min_gap = float(params.get("regime_min_gap", 0.00025))
            regime_votes: List[int] = []
            for leader in ("BTCUSDT", "ETHUSDT"):
                lp = points_map.get(leader)
                if lp is None:
                    continue
                lv = sign(lp.ema20_15 - lp.ema50_15)
                if lv == 0:
                    continue
                gap = abs(lp.ema20_15 - lp.ema50_15) / max(1e-9, lp.entry)
                if gap >= regime_min_gap:
                    regime_votes.append(lv)
            if len(regime_votes) >= 2 and all(v == regime_votes[0] for v in regime_votes):
                regime_sign = regime_votes[0]
        ranked_points = sorted(candidates, key=point_scan_score, reverse=True)[: max(1, min(watchlist_size, len(candidates)))]
        action_rows: List[Dict[str, Any]] = []
        for p in ranked_points:
            action, score, conf = predict_action(p, params)
            if regime_sign != 0 and action != "WAIT":
                if (regime_sign > 0 and action == "SHORT") or (regime_sign < 0 and action == "LONG"):
                    action = "WAIT"
            res = eval_trade(p, action, params)
            outcome = str(res["outcome"])
            action_rows.append(
                {
                    "symbol": p.symbol,
                    "action": action,
                    "score": float(score),
                    "confidence": int(conf),
                    "outcome": outcome,
                    "final_return": float(res["final_return"]),
                    "rsi15": float(p.rsi15),
                    "rsi5": float(p.rsi5),
                    "atr_rel": float(p.atr_rel),
                    "trend_conflict": bool(p.trend_conflict),
                    "stop_hit_bar": res.get("stop_hit_bar"),
                }
            )

        for r in action_rows:
            watch_total += 1
            action_s = str(r["action"])
            is_win = r["outcome"] in ("win_tp", "timeout_win")
            if action_s == "LONG":
                watch_long_count += 1
                if is_win:
                    watch_long_wins += 1
            elif action_s == "SHORT":
                watch_short_count += 1
                if is_win:
                    watch_short_wins += 1
            else:
                watch_wait_count += 1

            if r["action"] != "WAIT":
                watch_act += 1
                if is_win:
                    watch_win += 1
        score_margin = float(params.get("master_top_score_margin", 0.0))
        top_score_floor = float(params.get("threshold", 3.0)) + max(0.0, score_margin)
        actionable_rows = [
            r for r in action_rows
            if r["action"] != "WAIT" and abs(float(r["score"])) >= top_score_floor
        ]
        if not actionable_rows:
            top_wait_count += 1
            reason_counts["no_actionable_signal"] += 1
            continue

        top = sorted(actionable_rows, key=lambda r: abs(float(r["score"])), reverse=True)[0]
        top_count += 1
        if top["action"] == "LONG":
            top_long_count += 1
        elif top["action"] == "SHORT":
            top_short_count += 1
        top_is_win = top["outcome"] in ("win_tp", "timeout_win")
        if top_is_win:
            top_win += 1
            if top["action"] == "LONG":
                top_long_wins += 1
            elif top["action"] == "SHORT":
                top_short_wins += 1
        else:
            if abs(float(top["score"])) < float(params.get("threshold", 3.0)) + 0.75:
                reason_counts["weak_signal"] += 1
            if bool(top.get("trend_conflict")):
                reason_counts["mtf_conflict"] += 1
            if (top["action"] == "LONG" and float(top["rsi15"]) >= 68.0) or (
                top["action"] == "SHORT" and float(top["rsi15"]) <= 32.0
            ):
                reason_counts["overextended_entry"] += 1
            stop_hit_bar = top.get("stop_hit_bar")
            if isinstance(stop_hit_bar, int) and stop_hit_bar <= 3:
                reason_counts["early_stopout"] += 1
            if float(top["atr_rel"]) >= 0.03:
                reason_counts["high_noise_regime"] += 1

        top_rows.append(
            {
                "ts_utc": utc_iso(ts),
                "watchlist_symbols": [r.symbol for r in ranked_points],
                "top_symbol": top["symbol"],
                "top_action": top["action"],
                "top_score": round(float(top["score"]), 4),
                "top_outcome": top["outcome"],
                "top_final_return": round(float(top["final_return"]), 6),
            }
        )

    sampled_n = len(sampled)
    top_wr = (top_win / top_count) if top_count else 0.0
    top_cov = (top_count / sampled_n) if sampled_n else 0.0
    watch_wr = (watch_win / watch_act) if watch_act else 0.0
    top_long_wr = (top_long_wins / top_long_count) if top_long_count else 0.0
    top_short_wr = (top_short_wins / top_short_count) if top_short_count else 0.0
    watch_long_wr = (watch_long_wins / watch_long_count) if watch_long_count else 0.0
    watch_short_wr = (watch_short_wins / watch_short_count) if watch_short_count else 0.0
    top_ci_half = 1.96 * math.sqrt(max(0.0, top_wr * (1.0 - top_wr)) / top_count) if top_count > 0 else 0.0
    watch_ci_half = 1.96 * math.sqrt(max(0.0, watch_wr * (1.0 - watch_wr)) / watch_act) if watch_act > 0 else 0.0
    return {
        "total_timestamps": len(by_ts),
        "eligible_timestamps": len(eligible_ts),
        "sampled_timestamps": sampled_n,
        "watchlist_size": watchlist_size,
        "top_signal_count": top_count,
        "top_signal_wins": top_win,
        "top_signal_win_rate": top_wr,
        "top_signal_win_rate_ci95": [max(0.0, top_wr - top_ci_half), min(1.0, top_wr + top_ci_half)],
        "top_signal_coverage": top_cov,
        "top_decision_distribution": {
            "total": sampled_n,
            "long_count": top_long_count,
            "short_count": top_short_count,
            "wait_count": top_wait_count,
            "long_ratio": (top_long_count / sampled_n) if sampled_n else 0.0,
            "short_ratio": (top_short_count / sampled_n) if sampled_n else 0.0,
            "wait_ratio": (top_wait_count / sampled_n) if sampled_n else 0.0,
        },
        "top_long_win_rate": top_long_wr,
        "top_short_win_rate": top_short_wr,
        "watchlist_actionable_count": watch_act,
        "watchlist_actionable_wins": watch_win,
        "watchlist_actionable_win_rate": watch_wr,
        "watchlist_actionable_win_rate_ci95": [max(0.0, watch_wr - watch_ci_half), min(1.0, watch_wr + watch_ci_half)],
        "watchlist_action_distribution": {
            "total": watch_total,
            "long_count": watch_long_count,
            "short_count": watch_short_count,
            "wait_count": watch_wait_count,
            "long_ratio": (watch_long_count / watch_total) if watch_total else 0.0,
            "short_ratio": (watch_short_count / watch_total) if watch_total else 0.0,
            "wait_ratio": (watch_wait_count / watch_total) if watch_total else 0.0,
        },
        "watchlist_long_win_rate": watch_long_wr,
        "watchlist_short_win_rate": watch_short_wr,
        "loss_reasons": reason_counts,
        "sample_nodes": top_rows[:20],
    }


def choose_best_params(
    train_points: List[Point],
    atr_noise_cut: float,
    min_actionable: Optional[int] = None,
    profile: str = "balanced",
    min_coverage: float = MIN_COVERAGE,
    target_win_rate: float = TARGET_WIN_RATE,
) -> Tuple[Dict[str, float], Dict[str, Any]]:
    base = {
        "w15": 2.0,
        "w5": 1.0,
        "w1h": 1.0,
        "wrsi15": 0.8,
        "wrsi5": 0.5,
        "wpx": 0.5,
    }
    best_obj = -10_000.0
    best_params: Optional[Dict[str, float]] = None
    best_metrics: Optional[Dict[str, Any]] = None

    use_precision = str(profile or "").lower() == "precision"
    if use_precision:
        required_actionable = min_actionable if isinstance(min_actionable, int) and min_actionable > 0 else max(
            18, int(0.08 * len(train_points))
        )
        threshold_grid = [4.0, 4.5, 5.0, 5.5, 6.0]
        stop_grid = [1.4, 1.6, 1.8, 2.0]
        tp_grid = [0.25, 0.35, 0.45, 0.6]
        wait_grid = [0.3, 0.4, 0.5]
        filter_presets: List[Dict[str, float]] = [
            {
                "require_mtf_align": 1.0,
                "min_abs_score": 4.7,
                "min_atr_rel": 0.0010,
                "max_atr_rel": 0.0350,
                "max_entry_stretch": 1.3,
                "min_trend_gap_15": 0.0,
                "min_trend_gap_1h": 0.0,
                "require_regime_align": 0.0,
                "regime_min_gap": 0.00025,
                "long_rsi15_min": 47.0,
                "long_rsi15_max": 62.0,
                "long_rsi5_min": 44.0,
                "long_rsi5_max": 67.0,
                "short_rsi15_min": 38.0,
                "short_rsi15_max": 53.0,
                "short_rsi5_min": 33.0,
                "short_rsi5_max": 56.0,
            },
            {
                "require_mtf_align": 1.0,
                "min_abs_score": 5.0,
                "min_atr_rel": 0.0012,
                "max_atr_rel": 0.0300,
                "max_entry_stretch": 1.1,
                "min_trend_gap_15": 0.0,
                "min_trend_gap_1h": 0.0,
                "require_regime_align": 0.0,
                "regime_min_gap": 0.00025,
                "long_rsi15_min": 48.0,
                "long_rsi15_max": 60.0,
                "long_rsi5_min": 45.0,
                "long_rsi5_max": 64.0,
                "short_rsi15_min": 40.0,
                "short_rsi15_max": 52.0,
                "short_rsi5_min": 35.0,
                "short_rsi5_max": 55.0,
            },
            {
                "require_mtf_align": 1.0,
                "min_abs_score": 5.2,
                "min_atr_rel": 0.0010,
                "max_atr_rel": 0.0250,
                "max_entry_stretch": 1.0,
                "min_trend_gap_15": 0.0,
                "min_trend_gap_1h": 0.0,
                "require_regime_align": 0.0,
                "regime_min_gap": 0.00025,
                "long_rsi15_min": 49.0,
                "long_rsi15_max": 59.0,
                "long_rsi5_min": 46.0,
                "long_rsi5_max": 63.0,
                "short_rsi15_min": 41.0,
                "short_rsi15_max": 51.0,
                "short_rsi5_min": 36.0,
                "short_rsi5_max": 54.0,
            },
            {
                "require_mtf_align": 1.0,
                "min_abs_score": 5.5,
                "min_atr_rel": 0.0015,
                "max_atr_rel": 0.0220,
                "max_entry_stretch": 0.9,
                "min_trend_gap_15": 0.0,
                "min_trend_gap_1h": 0.0,
                "require_regime_align": 0.0,
                "regime_min_gap": 0.00025,
                "long_rsi15_min": 50.0,
                "long_rsi15_max": 58.0,
                "long_rsi5_min": 47.0,
                "long_rsi5_max": 61.0,
                "short_rsi15_min": 42.0,
                "short_rsi15_max": 50.0,
                "short_rsi5_min": 37.0,
                "short_rsi5_max": 53.0,
            },
            {
                "require_mtf_align": 1.0,
                "min_abs_score": 4.8,
                "min_atr_rel": 0.0010,
                "max_atr_rel": 0.0500,
                "max_entry_stretch": 1.6,
                "min_trend_gap_15": 0.0,
                "min_trend_gap_1h": 0.0,
                "require_regime_align": 0.0,
                "regime_min_gap": 0.00025,
                "long_rsi15_min": 45.0,
                "long_rsi15_max": 65.0,
                "long_rsi5_min": 42.0,
                "long_rsi5_max": 70.0,
                "short_rsi15_min": 35.0,
                "short_rsi15_max": 55.0,
                "short_rsi5_min": 30.0,
                "short_rsi5_max": 58.0,
            },
        ]
    else:
        required_actionable = min_actionable if isinstance(min_actionable, int) and min_actionable > 0 else max(
            25, int(0.15 * len(train_points))
        )
        threshold_grid = [2.5, 3.0, 3.5, 4.0]
        stop_grid = [1.0, 1.2, 1.4]
        tp_grid = [0.6, 0.8, 1.0]
        wait_grid = [0.4, 0.6, 0.8]
        filter_presets = [
            {
                "require_mtf_align": 0.0,
                "min_abs_score": 0.0,
                "min_atr_rel": 0.0,
                "max_atr_rel": 99.0,
                "max_entry_stretch": 99.0,
                "min_trend_gap_15": 0.0,
                "min_trend_gap_1h": 0.0,
                "require_regime_align": 0.0,
                "regime_min_gap": 0.00025,
                "long_rsi15_min": 0.0,
                "long_rsi15_max": 100.0,
                "long_rsi5_min": 0.0,
                "long_rsi5_max": 100.0,
                "short_rsi15_min": 0.0,
                "short_rsi15_max": 100.0,
                "short_rsi5_min": 0.0,
                "short_rsi5_max": 100.0,
            }
        ]

    for threshold, stop_mult, tp_mult, wait_mult, preset in itertools.product(
        threshold_grid,
        stop_grid,
        tp_grid,
        wait_grid,
        filter_presets,
    ):
        params = dict(base)
        params.update(preset)
        params.update(
            {
                "threshold": float(threshold),
                "threshold_long": float(threshold),
                "threshold_short": float(threshold),
                "stop_mult": float(stop_mult),
                "tp_mult": float(tp_mult),
                "wait_mult": float(wait_mult),
            }
        )

        if params["min_abs_score"] <= 0:
            params["min_abs_score"] = params["threshold"]

        m = evaluate(train_points, params, atr_noise_cut)
        if m["actionable"] < required_actionable:
            continue

        if use_precision:
            awr = float(m["actionable_win_rate"])
            cov = float(m["coverage"])
            if cov < max(0.02, min_coverage * 0.60):
                continue
            loss_n = max(1, int(m["losses"]))
            early_stop_ratio = float(m["loss_reasons"].get("early_stopout", 0)) / loss_n
            mtf_conflict_ratio = float(m["loss_reasons"].get("mtf_conflict", 0)) / loss_n
            obj = (
                awr
                - 0.55 * max(0.0, min_coverage - cov)
                - 0.30 * max(0.0, target_win_rate - awr)
                - 0.05 * early_stop_ratio
                - 0.04 * mtf_conflict_ratio
            )
            if awr >= target_win_rate:
                obj += 0.05
        else:
            awr = float(m["actionable_win_rate"])
            cov = float(m["coverage"])
            obj = awr - 0.20 * max(0.0, 0.25 - cov)

        if obj > best_obj:
            best_obj = obj
            best_params = params
            best_metrics = m

    if best_params is None or best_metrics is None:
        raise RuntimeError("No valid parameter set found; sample may be too small.")
    return best_params, best_metrics


def derive_symbol_gate(
    train_points: List[Point],
    params: Dict[str, float],
    atr_noise_cut: float,
    min_actionable: int,
    min_win_rate: float,
    min_symbols: int,
) -> Tuple[List[str], Dict[str, Any]]:
    grouped: Dict[str, List[Point]] = {}
    for p in train_points:
        grouped.setdefault(p.symbol, []).append(p)

    by_symbol: Dict[str, Dict[str, Any]] = {}
    chosen: List[str] = []
    for sym, pts in sorted(grouped.items()):
        m = evaluate(pts, params, atr_noise_cut)
        rec = {
            "total": int(m["total"]),
            "actionable": int(m["actionable"]),
            "wins": int(m["wins"]),
            "losses": int(m["losses"]),
            "actionable_win_rate": float(m["actionable_win_rate"]),
            "coverage": float(m["coverage"]),
            "shrunk_win_rate": float((int(m["wins"]) + 1.0) / (int(m["actionable"]) + 2.0))
        }
        by_symbol[sym] = rec
        if rec["actionable"] >= min_actionable and rec["shrunk_win_rate"] >= min_win_rate:
            chosen.append(sym)

    fallback = sorted(
        by_symbol.items(),
        key=lambda kv: (kv[1]["shrunk_win_rate"], kv[1]["actionable"]),
        reverse=True,
    )
    chosen_set = set(chosen)
    if len(chosen_set) < max(1, min_symbols):
        for s, rec in fallback:
            if s in chosen_set:
                continue
            if rec["actionable"] >= max(4, min_actionable // 2):
                chosen_set.add(s)
            if len(chosen_set) >= max(1, min_symbols):
                break
    chosen = [s for s, _ in fallback if s in chosen_set]

    return chosen, by_symbol


def refine_params_for_master(
    train_points_full: List[Point],
    base_params: Dict[str, float],
    rng_seed: int,
    num_timestamps: int,
    watchlist_size: int,
    min_top_coverage: float,
) -> Tuple[Dict[str, float], Dict[str, Any]]:
    if not train_points_full:
        return dict(base_params), {"status": "skipped", "reason": "empty_train_points_full"}

    base = dict(base_params)
    b_th = float(base.get("threshold", 4.0))
    b_stop = float(base.get("stop_mult", 1.6))
    b_tp = float(base.get("tp_mult", 0.25))
    b_wait = float(base.get("wait_mult", 0.4))
    b_min_abs = float(base.get("min_abs_score", max(3.0, b_th)))
    b_max_atr = float(base.get("max_atr_rel", 0.04))
    b_stretch = float(base.get("max_entry_stretch", 1.2))
    b_top_margin = float(base.get("master_top_score_margin", 0.0))
    b_th_long = float(base.get("threshold_long", b_th))
    b_th_short = float(base.get("threshold_short", b_th))

    threshold_grid = sorted({max(2.5, b_th - 0.5), b_th, b_th + 0.5})
    stop_grid = sorted({max(1.0, b_stop - 0.2), b_stop, b_stop + 0.2})
    tp_grid = sorted({max(0.2, b_tp), min(0.45, b_tp + 0.1)})
    wait_grid = sorted({max(0.2, b_wait - 0.1), b_wait})
    min_abs_grid = sorted({max(3.5, b_min_abs), b_min_abs + 0.3})
    max_atr_grid = sorted({max(0.02, b_max_atr - 0.01), b_max_atr, min(0.06, b_max_atr + 0.01)})
    stretch_grid = sorted({max(0.8, b_stretch - 0.2), b_stretch, min(1.8, b_stretch + 0.2)})
    top_margin_grid = [max(0.0, b_top_margin)]

    best_obj = -10_000.0
    best_params = dict(base)
    best_train_master: Dict[str, Any] = {}

    for th, st, tp, wm, ma, mxatr, estr, tsm in itertools.product(
        threshold_grid,
        stop_grid,
        tp_grid,
        wait_grid,
        min_abs_grid,
        max_atr_grid,
        stretch_grid,
        top_margin_grid,
    ):
        p = dict(base)
        p["threshold"] = float(th)
        th_delta = float(th) - b_th
        p["threshold_long"] = max(2.5, b_th_long + th_delta)
        p["threshold_short"] = max(2.5, b_th_short + th_delta)
        p["stop_mult"] = float(st)
        p["tp_mult"] = float(tp)
        p["wait_mult"] = float(wm)
        p["min_abs_score"] = max(float(ma), min(p["threshold_long"], p["threshold_short"]))
        p["max_atr_rel"] = float(mxatr)
        p["max_entry_stretch"] = float(estr)
        p["master_top_score_margin"] = float(tsm)

        m = evaluate_master_prompt_timestamps(
            train_points_full,
            p,
            random.Random(rng_seed),
            num_timestamps=max(20, num_timestamps),
            watchlist_size=max(3, watchlist_size),
        )
        sampled = int(m.get("sampled_timestamps", 0))
        top_count = int(m.get("top_signal_count", 0))
        if sampled <= 0 or top_count <= 0:
            continue

        top_wr = float(m.get("top_signal_win_rate", 0.0))
        top_cov = float(m.get("top_signal_coverage", 0.0))
        watch_wr = float(m.get("watchlist_actionable_win_rate", 0.0))
        top_count_ratio = top_count / sampled if sampled > 0 else 0.0

        obj = (
            top_wr
            - 0.45 * max(0.0, min_top_coverage - top_cov)
            - 0.15 * max(0.0, 0.55 - watch_wr)
            + 0.06 * min(1.0, top_count_ratio / max(1e-9, min_top_coverage))
        )
        if top_wr >= 0.75:
            obj += 0.06
        if top_wr >= 0.80:
            obj += 0.04

        if obj > best_obj:
            best_obj = obj
            best_params = p
            best_train_master = m

    if not best_train_master:
        return dict(base_params), {"status": "skipped", "reason": "no_valid_master_refine_candidate"}
    return best_params, {
        "status": "ok",
        "objective": best_obj,
        "train_master": best_train_master,
    }


def optimize_per_symbol(
    points: List[Point],
    baseline_params: Dict[str, float],
    atr_noise_cut: float,
    profile: str,
    min_coverage: float,
    target_win_rate: float,
) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    grouped: Dict[str, List[Point]] = {}
    for p in points:
        grouped.setdefault(p.symbol, []).append(p)

    for sym, pts in sorted(grouped.items()):
        pts.sort(key=lambda x: x.ts_ms)
        if len(pts) < 36:
            out[sym] = {
                "status": "skipped",
                "reason": f"sample_too_small({len(pts)})",
            }
            continue
        split = int(len(pts) * 0.7)
        tr = pts[:split]
        te = pts[split:]
        if len(tr) < 24 or len(te) < 10:
            out[sym] = {
                "status": "skipped",
                "reason": f"train_or_test_too_small(train={len(tr)}, test={len(te)})",
            }
            continue

        base_te = evaluate(te, baseline_params, atr_noise_cut)
        try:
            best_params, _ = choose_best_params(
                tr,
                atr_noise_cut,
                min_actionable=max(8, int(0.20 * len(tr))),
                profile=profile,
                min_coverage=max(0.05, min_coverage * 0.8),
                target_win_rate=target_win_rate,
            )
            best_te = evaluate(te, best_params, atr_noise_cut)
            out[sym] = {
                "status": "ok",
                "points_total": len(pts),
                "train_points": len(tr),
                "test_points": len(te),
                "baseline_test": {k: v for k, v in base_te.items() if k != "rows"},
                "optimized_params": best_params,
                "optimized_test": {k: v for k, v in best_te.items() if k != "rows"},
            }
        except Exception as ex:
            out[sym] = {
                "status": "error",
                "reason": str(ex),
                "points_total": len(pts),
                "baseline_test": {k: v for k, v in base_te.items() if k != "rows"},
            }

    return out


def main() -> int:
    rng = random.Random(RANDOM_SEED)
    t0 = time.time()

    symbols = fetch_top_symbols(TOP_SYMBOLS)
    if not symbols:
        raise RuntimeError("No symbols fetched from Binance.US")

    all_points: List[Point] = []
    all_points_full: List[Point] = []
    symbol_stats: Dict[str, Dict[str, int]] = {}
    symbol_errors: Dict[str, str] = {}

    for s in symbols:
        try:
            bars5 = fetch_klines(s, "5m", FETCH_BARS_5M)
            bars15 = fetch_klines(s, "15m", FETCH_BARS_15M)
            bars1h = fetch_klines(s, "1h", FETCH_BARS_1H)
            pts_full = build_points_for_symbol(s, bars5, bars15, bars1h, 1_000_000_000, rng)
            if not pts_full:
                symbol_errors[s] = "no_valid_points_after_feature_checks"
                continue
            pts = pts_full if len(pts_full) <= POINTS_PER_SYMBOL else rng.sample(pts_full, POINTS_PER_SYMBOL)
            all_points.extend(pts)
            all_points_full.extend(pts_full)
            symbol_stats[s] = {
                "bars_5m": len(bars5),
                "bars_15m": len(bars15),
                "bars_1h": len(bars1h),
                "sampled_points": len(pts),
                "full_points": len(pts_full),
            }
        except Exception as ex:
            symbol_errors[s] = str(ex)
            continue

    if len(all_points) < 120:
        raise RuntimeError(f"Insufficient sampled points: {len(all_points)}; symbol_errors={symbol_errors}")

    all_points.sort(key=lambda x: x.ts_ms)
    split = int(len(all_points) * 0.7)
    train_points = all_points[:split]
    test_points = all_points[split:]
    split_ts = test_points[0].ts_ms if test_points else (all_points[-1].ts_ms if all_points else 0)

    all_points_full.sort(key=lambda x: x.ts_ms)
    train_points_full = [p for p in all_points_full if p.ts_ms < split_ts]
    test_points_full = [p for p in all_points_full if p.ts_ms >= split_ts]

    atr_vals = [p.atr_rel for p in all_points]
    atr_noise_cut = statistics.quantiles(atr_vals, n=10)[-1] if len(atr_vals) >= 20 else max(atr_vals)

    baseline_params = {
        "w15": 2.0,
        "w5": 1.0,
        "w1h": 1.0,
        "wrsi15": 0.8,
        "wrsi5": 0.5,
        "wpx": 0.5,
        "threshold": 3.0,
        "stop_mult": 1.2,
        "tp_mult": 0.8,
        "wait_mult": 0.6,
    }

    baseline_train = evaluate(train_points, baseline_params, atr_noise_cut)
    baseline_test = evaluate(test_points, baseline_params, atr_noise_cut)

    best_params, best_train = choose_best_params(
        train_points,
        atr_noise_cut,
        profile=BACKTEST_PROFILE,
        min_coverage=MIN_COVERAGE,
        target_win_rate=TARGET_WIN_RATE,
    )
    best_test = evaluate(test_points, best_params, atr_noise_cut)
    symbol_gate_info: Dict[str, Any] = {
        "enabled": bool(ENABLE_SYMBOL_GATE),
        "applied": False,
        "min_actionable": SYMBOL_GATE_MIN_ACTIONABLE,
        "min_win_rate": SYMBOL_GATE_MIN_WIN_RATE,
        "min_symbols": SYMBOL_GATE_MIN_SYMBOLS,
        "allowed_symbols": [],
        "train_by_symbol": {},
    }
    if BACKTEST_PROFILE == "precision" and ENABLE_SYMBOL_GATE:
        allowed_symbols, train_by_symbol = derive_symbol_gate(
            train_points,
            best_params,
            atr_noise_cut,
            min_actionable=SYMBOL_GATE_MIN_ACTIONABLE,
            min_win_rate=SYMBOL_GATE_MIN_WIN_RATE,
            min_symbols=SYMBOL_GATE_MIN_SYMBOLS,
        )
        if allowed_symbols:
            gated_params = dict(best_params)
            gated_params["allowed_symbols"] = allowed_symbols
            gated_params["scan_use_allowed_only"] = 1.0
            best_params = gated_params
            best_train = evaluate(train_points, best_params, atr_noise_cut)
            best_test = evaluate(test_points, best_params, atr_noise_cut)
            symbol_gate_info["applied"] = True
            symbol_gate_info["allowed_symbols"] = allowed_symbols
        symbol_gate_info["train_by_symbol"] = train_by_symbol

    if BACKTEST_PROFILE == "precision":
        best_params = dict(best_params)
        best_params["master_top_score_margin"] = float(best_params.get("master_top_score_margin", MASTER_TOP_SCORE_MARGIN))

    pre_refine_train = dict(best_train)
    refine_ts = max(MASTER_TS_NODES * 2, 80)
    base_train_master = evaluate_master_prompt_timestamps(
        train_points_full,
        best_params,
        random.Random(RANDOM_SEED + 403),
        num_timestamps=refine_ts,
        watchlist_size=max(3, MASTER_WATCHLIST_SIZE),
    )
    master_refine_info: Dict[str, Any] = {
        "status": "skipped",
        "base_train_master": base_train_master,
    }
    if BACKTEST_PROFILE == "precision" and ENABLE_MASTER_REFINE:
        min_top_coverage = max(0.10, MASTER_MIN_TOP_COUNT / float(max(1, refine_ts)))
        refined_params, refine_info = refine_params_for_master(
            train_points_full=train_points_full,
            base_params=best_params,
            rng_seed=RANDOM_SEED + 404,
            num_timestamps=refine_ts,
            watchlist_size=max(3, MASTER_WATCHLIST_SIZE),
            min_top_coverage=min_top_coverage,
        )
        master_refine_info.update(refine_info)
        if refine_info.get("status") == "ok":
            cand_train = evaluate(train_points, refined_params, atr_noise_cut)
            base_top_wr = float(base_train_master.get("top_signal_win_rate", 0.0))
            base_top_cnt = int(base_train_master.get("top_signal_count", 0))
            ref_train_master = refine_info.get("train_master", {}) or {}
            ref_top_wr = float(ref_train_master.get("top_signal_win_rate", 0.0))
            ref_top_cnt = int(ref_train_master.get("top_signal_count", 0))
            pre_train_awr = float(pre_refine_train.get("actionable_win_rate", 0.0))
            cand_train_awr = float(cand_train.get("actionable_win_rate", 0.0))

            improve_master = (
                (ref_top_wr >= base_top_wr + 0.01 and ref_top_cnt >= max(MASTER_MIN_TOP_COUNT, base_top_cnt))
                or (ref_top_wr >= base_top_wr + 0.02)
            )
            not_hurt_point = cand_train_awr + 0.10 >= pre_train_awr
            if improve_master and not_hurt_point:
                best_params = refined_params
                best_train = cand_train
                best_test = evaluate(test_points, best_params, atr_noise_cut)
                master_refine_info["accepted"] = True
            else:
                master_refine_info["accepted"] = False
                master_refine_info["rejected_reason"] = {
                    "base_top_wr": base_top_wr,
                    "ref_top_wr": ref_top_wr,
                    "base_top_count": base_top_cnt,
                    "ref_top_count": ref_top_cnt,
                    "pre_train_awr": pre_train_awr,
                    "cand_train_awr": cand_train_awr,
                }
    elif BACKTEST_PROFILE == "precision":
        master_refine_info["status"] = "skipped"
        master_refine_info["reason"] = "disabled_by_config"

    master_prompt_sim = evaluate_master_prompt_timestamps(
        test_points_full,
        best_params,
        random.Random(RANDOM_SEED + 202),
        num_timestamps=MASTER_TS_NODES,
        watchlist_size=max(3, MASTER_WATCHLIST_SIZE),
    )

    if ENABLE_PER_SYMBOL_OPT:
        per_symbol_optimized = optimize_per_symbol(
            all_points,
            baseline_params,
            atr_noise_cut,
            profile=BACKTEST_PROFILE,
            min_coverage=MIN_COVERAGE,
            target_win_rate=TARGET_WIN_RATE,
        )
    else:
        per_symbol_optimized = {}

    elapsed = round(time.time() - t0, 2)
    now = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%d_%H%M%S_%f")
    run_tag = f"{now}_{RANDOM_SEED}"

    # Keep a concise random sample for auditability.
    random_nodes = random.Random(RANDOM_SEED + 1).sample(
        best_test["rows"], min(18, len(best_test["rows"]))
    )

    report = {
        "run_time_utc": dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z"),
        "seed": RANDOM_SEED,
        "elapsed_sec": elapsed,
        "run_config": {
            "profile": BACKTEST_PROFILE,
            "target_win_rate": TARGET_WIN_RATE,
            "min_coverage": MIN_COVERAGE,
            "master_top_score_margin": MASTER_TOP_SCORE_MARGIN,
            "enable_symbol_gate": bool(ENABLE_SYMBOL_GATE),
            "symbol_gate_min_actionable": SYMBOL_GATE_MIN_ACTIONABLE,
            "symbol_gate_min_win_rate": SYMBOL_GATE_MIN_WIN_RATE,
            "symbol_gate_min_symbols": SYMBOL_GATE_MIN_SYMBOLS,
            "top_symbols": TOP_SYMBOLS,
            "forced_symbols": FORCED_SYMBOLS,
            "points_per_symbol": POINTS_PER_SYMBOL,
            "fetch_bars_5m": FETCH_BARS_5M,
            "fetch_bars_15m": FETCH_BARS_15M,
            "fetch_bars_1h": FETCH_BARS_1H,
            "horizon_minutes": HORIZON_MINUTES,
            "master_ts_nodes": MASTER_TS_NODES,
            "master_watchlist_size": MASTER_WATCHLIST_SIZE,
            "master_min_top_count": MASTER_MIN_TOP_COUNT,
            "enable_per_symbol_opt": bool(ENABLE_PER_SYMBOL_OPT),
            "enable_master_refine": bool(ENABLE_MASTER_REFINE),
        },
        "symbols": symbols,
        "symbol_stats": symbol_stats,
        "symbol_errors": symbol_errors,
        "sampling": {
            "total_points": len(all_points),
            "total_points_full": len(all_points_full),
            "train_points": len(train_points),
            "test_points": len(test_points),
            "test_points_full": len(test_points_full),
            "horizon_minutes": HORIZON_MINUTES,
            "points_per_symbol_target": POINTS_PER_SYMBOL,
        },
        "baseline_params": baseline_params,
        "baseline_train": {k: v for k, v in baseline_train.items() if k != "rows"},
        "baseline_test": {k: v for k, v in baseline_test.items() if k != "rows"},
        "optimized_params": best_params,
        "optimized_train": {k: v for k, v in best_train.items() if k != "rows"},
        "optimized_test": {k: v for k, v in best_test.items() if k != "rows"},
        "symbol_gate": symbol_gate_info,
        "master_refine": master_refine_info,
        "master_prompt_simulation": master_prompt_sim,
        "per_symbol_optimized": per_symbol_optimized,
        "random_nodes_sample": random_nodes,
    }

    os.makedirs("logs", exist_ok=True)
    out_json = os.path.join("logs", f"backtest_random_nodes_{run_tag}.json")
    out_md = os.path.join("logs", f"backtest_random_nodes_{run_tag}.md")

    with open(out_json, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)

    md = []
    md.append(f"# Backtest Report ({now} UTC)")
    md.append("")
    md.append(f"- symbols: {', '.join(symbols)}")
    md.append(
        f"- total_points(sampled): {len(all_points)} (train={len(train_points)}, test={len(test_points)}) | "
        f"total_points(full)={len(all_points_full)}"
    )
    md.append(f"- horizon: {HORIZON_MINUTES}m")
    md.append(f"- profile: {BACKTEST_PROFILE} (target_win={TARGET_WIN_RATE:.2f}, min_coverage={MIN_COVERAGE:.2f})")
    md.append(f"- master_refine: {master_refine_info.get('status')}")
    if symbol_gate_info.get("applied"):
        md.append(f"- symbol_gate: ON ({', '.join(symbol_gate_info.get('allowed_symbols', []))})")
    elif ENABLE_SYMBOL_GATE and BACKTEST_PROFILE == "precision":
        md.append("- symbol_gate: ON (no eligible symbols, fallback not triggered)")
    if symbol_errors:
        md.append(f"- symbol_errors: {len(symbol_errors)}")
    md.append("")
    md.append("## Baseline (test)")
    md.append(f"- actionable_win_rate: {best_pct(baseline_test['actionable_win_rate'])}")
    md.append(f"- directional_accuracy: {best_pct(baseline_test['directional_accuracy'])}")
    md.append(f"- coverage: {best_pct(baseline_test['coverage'])}")
    md.append(f"- overall_accuracy: {best_pct(baseline_test['overall_accuracy'])}")
    md.append("")
    md.append("## Optimized (test)")
    md.append(f"- actionable_win_rate: {best_pct(best_test['actionable_win_rate'])}")
    md.append(f"- directional_accuracy: {best_pct(best_test['directional_accuracy'])}")
    md.append(f"- coverage: {best_pct(best_test['coverage'])}")
    md.append(f"- overall_accuracy: {best_pct(best_test['overall_accuracy'])}")
    md.append("")
    md.append("## Master Prompt Simulation (random timestamps)")
    md.append(f"- sampled_timestamps: {master_prompt_sim['sampled_timestamps']}")
    md.append(f"- watchlist_size: {master_prompt_sim['watchlist_size']}")
    md.append(
        f"- top_signal_win_rate: {best_pct(master_prompt_sim['top_signal_win_rate'])} "
        f"(coverage={best_pct(master_prompt_sim['top_signal_coverage'])})"
    )
    md.append(
        f"- watchlist_actionable_win_rate: {best_pct(master_prompt_sim['watchlist_actionable_win_rate'])} "
        f"(count={master_prompt_sim['watchlist_actionable_count']})"
    )
    tdist = master_prompt_sim.get("top_decision_distribution", {}) or {}
    wdist = master_prompt_sim.get("watchlist_action_distribution", {}) or {}
    md.append(
        f"- top_decision_ratio: LONG={best_pct(float(tdist.get('long_ratio', 0.0)))} | "
        f"SHORT={best_pct(float(tdist.get('short_ratio', 0.0)))} | WAIT={best_pct(float(tdist.get('wait_ratio', 0.0)))}"
    )
    md.append(
        f"- top_side_win_rate: LONG={best_pct(float(master_prompt_sim.get('top_long_win_rate', 0.0)))} | "
        f"SHORT={best_pct(float(master_prompt_sim.get('top_short_win_rate', 0.0)))}"
    )
    md.append(
        f"- watchlist_action_ratio: LONG={best_pct(float(wdist.get('long_ratio', 0.0)))} | "
        f"SHORT={best_pct(float(wdist.get('short_ratio', 0.0)))} | WAIT={best_pct(float(wdist.get('wait_ratio', 0.0)))}"
    )
    md.append(
        f"- watchlist_side_win_rate: LONG={best_pct(float(master_prompt_sim.get('watchlist_long_win_rate', 0.0)))} | "
        f"SHORT={best_pct(float(master_prompt_sim.get('watchlist_short_win_rate', 0.0)))}"
    )
    md.append("")
    md.append("## Top Loss Reasons (optimized test)")
    for k, v in sorted(best_test["loss_reasons"].items(), key=lambda x: x[1], reverse=True):
        md.append(f"- {k}: {v}")
    md.append("")
    md.append("## By Symbol (optimized test)")
    for s, b in sorted(best_test["by_symbol"].items()):
        md.append(
            f"- {s}: actionable_win_rate={best_pct(b['actionable_win_rate'])}, "
            f"coverage={best_pct(b['coverage'])}, actionable={b['actionable']}, waits={b['waits']}"
        )
    md.append("")
    md.append("## Per-Symbol Optimization")
    for s, rec in sorted(per_symbol_optimized.items()):
        if rec.get("status") != "ok":
            md.append(f"- {s}: {rec.get('status')} ({rec.get('reason')})")
            continue
        b = rec["baseline_test"]
        o = rec["optimized_test"]
        md.append(
            f"- {s}: baseline_win={best_pct(b['actionable_win_rate'])}, "
            f"optimized_win={best_pct(o['actionable_win_rate'])}, "
            f"optimized_cov={best_pct(o['coverage'])}"
        )
    if symbol_errors:
        md.append("")
        md.append("## Symbol Errors")
        for s, err in sorted(symbol_errors.items()):
            md.append(f"- {s}: {err}")
    md.append("")
    md.append("## Random Nodes Sample")
    for r in random_nodes:
        md.append(
            f"- {r['ts_utc']} {r['symbol']} action={r['action']} outcome={r['outcome']} "
            f"score={r['score']} final_return={r['final_return']}"
        )
    md.append("")
    md.append("## Master Prompt Sample Nodes")
    for r in master_prompt_sim.get("sample_nodes", [])[:12]:
        md.append(
            f"- {r['ts_utc']} top={r['top_symbol']} action={r['top_action']} "
            f"outcome={r['top_outcome']} score={r['top_score']}"
        )
    md.append("")
    md.append("## Files")
    md.append(f"- json: `{out_json}`")
    md.append(f"- md: `{out_md}`")

    with open(out_md, "w", encoding="utf-8") as f:
        f.write("\n".join(md) + "\n")

    print(f"OK: {out_json}")
    print(f"OK: {out_md}")
    return 0


def best_pct(x: float) -> str:
    return f"{x * 100:.2f}%"


if __name__ == "__main__":
    raise SystemExit(main())
