#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Realtime 0_master_prompt runner (Binance.US spot, 30~60m).

Flow:
1) Stage A scan -> watchlist
2) Stage B per-symbol self_check + snapshot + audit_full_v1 analysis + guard
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import math
import os
import subprocess
import sys
import time
import xml.etree.ElementTree as ET
from email.utils import parsedate_to_datetime
from typing import Any, Dict, List, Tuple

import requests


ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
LOG_DIR = os.path.join(ROOT, "logs")
DATA_PY = os.path.join(ROOT, "scripts", "data.py")
SELF_CHECK_PY = os.path.join(ROOT, "scripts", "self_check.py")
GUARD_PY = os.path.join(ROOT, "scripts", "decision_guard.py")
BASE_URL = "https://api.binance.us"
NEWS_FEEDS = [
    ("coindesk", "https://www.coindesk.com/arc/outboundfeeds/rss/"),
    ("cointelegraph", "https://cointelegraph.com/rss"),
]
NEWS_POS_WORDS = {
    "surge",
    "rally",
    "record high",
    "bullish",
    "approval",
    "adoption",
    "inflow",
    "breakout",
    "upgrade",
    "partnership",
}
NEWS_NEG_WORDS = {
    "hack",
    "lawsuit",
    "ban",
    "outflow",
    "selloff",
    "bearish",
    "liquidation",
    "crash",
    "exploit",
    "probe",
    "fraud",
}
NEWS_SYMBOL_HINTS = {
    "BTC": ["bitcoin", "btc", "crypto"],
    "ETH": ["ethereum", "eth", "crypto"],
    "SOL": ["solana", "sol"],
    "XRP": ["xrp", "ripple"],
    "BNB": ["bnb", "binance coin", "binance"],
    "DOGE": ["doge", "dogecoin"],
}
NEWS_CACHE: Dict[str, Tuple[float, Dict[str, Any]]] = {}


def utc_now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z")


def local_now_iso() -> str:
    return dt.datetime.now().astimezone().isoformat()


def sf(x: Any) -> float | None:
    try:
        if x is None:
            return None
        return float(x)
    except Exception:
        return None


def g(obj: Any, path: str, default: Any = None) -> Any:
    cur = obj
    for p in path.split("."):
        if isinstance(cur, dict) and p in cur:
            cur = cur[p]
        else:
            return default
    return cur


def b2s(v: bool) -> str:
    return "true" if v else "false"


def trend(ema20: float | None, ema50: float | None) -> str:
    if ema20 is None or ema50 is None:
        return "MIX"
    if ema20 > ema50:
        return "UP"
    if ema20 < ema50:
        return "DOWN"
    return "MIX"


def ema(vals: List[float], period: int) -> float | None:
    if period <= 0 or len(vals) < period:
        return None
    k = 2.0 / (period + 1.0)
    e = sum(vals[:period]) / period
    for v in vals[period:]:
        e = (v - e) * k + e
    return e


def rsi(vals: List[float], period: int = 14) -> float | None:
    if len(vals) < period + 1:
        return None
    gains: List[float] = []
    losses: List[float] = []
    for i in range(1, period + 1):
        ch = vals[i] - vals[i - 1]
        gains.append(max(ch, 0.0))
        losses.append(max(-ch, 0.0))
    avg_gain = sum(gains) / period
    avg_loss = sum(losses) / period
    for i in range(period + 1, len(vals)):
        ch = vals[i] - vals[i - 1]
        gain = max(ch, 0.0)
        loss = max(-ch, 0.0)
        avg_gain = ((period - 1) * avg_gain + gain) / period
        avg_loss = ((period - 1) * avg_loss + loss) / period
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))


def atr(candles: List[Dict[str, Any]], period: int = 14) -> float | None:
    if len(candles) < period + 1:
        return None
    trs: List[float] = []
    prev_close = sf(candles[0].get("close"))
    for c in candles[1:]:
        h = sf(c.get("high"))
        l = sf(c.get("low"))
        close = sf(c.get("close"))
        if h is None or l is None or prev_close is None:
            prev_close = close
            continue
        tr = max(h - l, abs(h - prev_close), abs(l - prev_close))
        trs.append(tr)
        prev_close = close
    if len(trs) < period:
        return None
    return sum(trs[-period:]) / period


def closes_from_klines(kl: Any) -> List[float]:
    out: List[float] = []
    if not isinstance(kl, list):
        return out
    for x in kl:
        if isinstance(x, list) and len(x) > 4:
            v = sf(x[4])
            if v is not None:
                out.append(v)
    return out


def pct_returns(vals: List[float]) -> List[float]:
    out: List[float] = []
    if len(vals) < 2:
        return out
    prev = vals[0]
    for v in vals[1:]:
        if prev > 0:
            out.append((v - prev) / prev)
        prev = v
    return out


def pearson_corr(a: List[float], b: List[float]) -> float | None:
    n = min(len(a), len(b))
    if n < 8:
        return None
    x = a[-n:]
    y = b[-n:]
    mx = sum(x) / n
    my = sum(y) / n
    num = 0.0
    den_x = 0.0
    den_y = 0.0
    for i in range(n):
        dx = x[i] - mx
        dy = y[i] - my
        num += dx * dy
        den_x += dx * dx
        den_y += dy * dy
    den = math.sqrt(den_x * den_y)
    if den <= 1e-12:
        return None
    return num / den


def beta_to_benchmark(asset_ret: List[float], bench_ret: List[float]) -> float | None:
    n = min(len(asset_ret), len(bench_ret))
    if n < 8:
        return None
    x = asset_ret[-n:]
    y = bench_ret[-n:]
    mx = sum(x) / n
    my = sum(y) / n
    cov = 0.0
    var_b = 0.0
    for i in range(n):
        cov += (x[i] - mx) * (y[i] - my)
        var_b += (y[i] - my) * (y[i] - my)
    if var_b <= 1e-12:
        return None
    return cov / var_b


def clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


def parse_pub_time(text: str) -> dt.datetime | None:
    if not text:
        return None
    s = str(text).strip()
    if not s:
        return None
    try:
        return parsedate_to_datetime(s).astimezone(dt.timezone.utc)
    except Exception:
        pass
    try:
        return dt.datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(dt.timezone.utc)
    except Exception:
        return None


def headline_sentiment_score(text: str) -> int:
    t = str(text or "").lower()
    if not t:
        return 0
    score = 0
    for w in NEWS_POS_WORDS:
        if w in t:
            score += 1
    for w in NEWS_NEG_WORDS:
        if w in t:
            score -= 1
    return score


def news_keywords(symbol: str) -> List[str]:
    base = str(symbol or "").upper().replace("USDT", "")
    kws = [base.lower()] if base else []
    kws.extend(NEWS_SYMBOL_HINTS.get(base, []))
    # Keep broad crypto context for majors.
    if base in ("BTC", "ETH"):
        kws.append("crypto")
    dedup: List[str] = []
    seen = set()
    for k in kws:
        kk = str(k).strip().lower()
        if kk and kk not in seen:
            dedup.append(kk)
            seen.add(kk)
    return dedup


def fetch_news_signal(symbol: str, cache_ttl_sec: int = 300, max_age_hours: int = 8) -> Dict[str, Any]:
    sym = str(symbol or "").upper()
    now_ts = time.time()
    cached = NEWS_CACHE.get(sym)
    if cached and (now_ts - cached[0]) <= cache_ttl_sec:
        return cached[1]

    kws = news_keywords(sym)
    now_utc = dt.datetime.now(dt.timezone.utc)
    hits = 0
    pos_hits = 0
    neg_hits = 0
    sum_score = 0.0
    samples: List[str] = []
    src_ok = 0
    src_err = 0

    for _, url in NEWS_FEEDS:
        try:
            r = requests.get(url, timeout=8)
            if not (200 <= r.status_code < 300):
                src_err += 1
                continue
            src_ok += 1
            root = ET.fromstring(r.text)
            items = root.findall(".//item")
            if not items:
                items = root.findall(".//entry")
            for it in items[:150]:
                title = ""
                title_node = it.find("title")
                if title_node is not None and title_node.text:
                    title = title_node.text.strip()
                if not title:
                    continue
                tl = title.lower()
                if kws and not any(k in tl for k in kws):
                    continue

                # Apply recency filter when publish time exists.
                pub_text = ""
                for tag in ("pubDate", "updated", "published"):
                    n = it.find(tag)
                    if n is not None and n.text:
                        pub_text = n.text.strip()
                        break
                pub_dt = parse_pub_time(pub_text) if pub_text else None
                if pub_dt is not None:
                    age_h = (now_utc - pub_dt).total_seconds() / 3600.0
                    if age_h > max_age_hours:
                        continue

                hs = headline_sentiment_score(title)
                hits += 1
                sum_score += hs
                if hs > 0:
                    pos_hits += 1
                elif hs < 0:
                    neg_hits += 1
                if len(samples) < 3:
                    samples.append(title[:180])
        except Exception:
            src_err += 1

    if hits > 0:
        norm_score = clamp(sum_score / max(1.0, hits * 2.0), -1.0, 1.0)
        status = "OK"
    elif src_ok <= 0:
        norm_score = 0.0
        status = "UNAVAILABLE"
    else:
        norm_score = 0.0
        status = "NO_MATCH"

    out = {
        "status": status,
        "hits": hits,
        "positive_hits": pos_hits,
        "negative_hits": neg_hits,
        "score": round(norm_score, 4),
        "sources_ok": src_ok,
        "sources_error": src_err,
        "sample_headlines": samples,
    }
    NEWS_CACHE[sym] = (now_ts, out)
    return out


def get_json(path: str, params: Dict[str, Any] | None = None, retries: int = 1) -> Tuple[Any, Dict[str, Any] | None]:
    url = BASE_URL + path
    last_err: Dict[str, Any] | None = None
    for i in range(retries + 1):
        try:
            r = requests.get(url, params=params, timeout=12)
            if r.status_code in (429, 418) and i < retries:
                time.sleep(1.5 * (2**i))
                continue
            if not (200 <= r.status_code < 300):
                last_err = {
                    "path": path,
                    "params": params or {},
                    "status_code": r.status_code,
                    "cause": (r.text or "")[:220],
                }
                if i < retries and r.status_code in (500, 502, 503, 504, 429, 418):
                    time.sleep(1.2 * (2**i))
                    continue
                return None, last_err
            return r.json(), None
        except Exception as e:
            last_err = {"path": path, "params": params or {}, "status_code": None, "cause": str(e)}
            if i < retries:
                time.sleep(1.2 * (2**i))
                continue
            return None, last_err
    return None, last_err


def run_cmd(args: List[str]) -> subprocess.CompletedProcess:
    return subprocess.run(args, cwd=ROOT, capture_output=True, text=True, encoding="utf-8", errors="ignore")


def parse_final_decision(path: str) -> Dict[str, Any]:
    out: Dict[str, Any] = {"action": "WAIT", "confidence": 0.0, "data_quality": "UNKNOWN"}
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        lines = f.read().splitlines()
    in_final = False
    for line in lines:
        s = line.strip()
        if s == "[FINAL_DECISION]":
            in_final = True
            continue
        if in_final and s.startswith("[") and s.endswith("]"):
            break
        if in_final and "=" in s:
            k, v = s.split("=", 1)
            k = k.strip()
            v = v.strip()
            if k == "action":
                out["action"] = v.upper()
            elif k == "confidence":
                out["confidence"] = sf(v) or 0.0
            elif k == "data_quality":
                out["data_quality"] = v
    return out


def stage_a_scan(watchlist_size: int) -> Tuple[List[str], List[str], str]:
    ts_utc = utc_now_iso()
    ts_tokyo = dt.datetime.now(dt.timezone(dt.timedelta(hours=9))).isoformat()
    rows_24h, err24 = get_json("/api/v3/ticker/24hr")
    books, err_book = get_json("/api/v3/ticker/bookTicker")
    errors: List[Dict[str, Any]] = []
    if err24:
        errors.append(err24)
    if err_book:
        errors.append(err_book)
    if not isinstance(rows_24h, list):
        raise RuntimeError(f"scan failed: ticker24h unavailable: {err24}")
    bmap: Dict[str, Dict[str, Any]] = {}
    if isinstance(books, list):
        for b in books:
            if isinstance(b, dict) and b.get("symbol"):
                bmap[str(b["symbol"])] = b

    usdt = [r for r in rows_24h if isinstance(r, dict) and str(r.get("symbol", "")).endswith("USDT")]
    usdt = [r for r in usdt if (sf(r.get("quoteVolume")) or 0.0) > 0]
    usdt.sort(key=lambda x: sf(x.get("quoteVolume")) or 0.0, reverse=True)
    top60 = usdt[:60]

    qv_sorted = sorted([sf(x.get("quoteVolume")) or 0.0 for x in top60], reverse=True)
    qv_floor = qv_sorted[min(len(qv_sorted) - 1, 35)] if qv_sorted else 0.0
    min_qv = max(150_000.0, min(2_000_000.0, qv_floor))
    qv_soft = max(120_000.0, min_qv * 0.45)
    stable_like = {
        "USDCUSDT",
        "BUSDUSDT",
        "FDUSDUSDT",
        "USDPUSDT",
        "TUSDUSDT",
        "DAIUSDT",
        "PAXGUSDT",
    }
    major_set = {"BTCUSDT", "ETHUSDT", "XRPUSDT"}
    btc_t1, ebtc_t1 = get_json("/api/v3/ticker", {"symbol": "BTCUSDT", "windowSize": "1h"})
    btc_k5, ebtc_k5 = get_json("/api/v3/klines", {"symbol": "BTCUSDT", "interval": "5m", "limit": 120})
    if ebtc_t1:
        errors.append(ebtc_t1)
    if ebtc_k5:
        errors.append(ebtc_k5)
    btc_ret1 = sf(g(btc_t1 or {}, "priceChangePercent")) or 0.0
    btc_close_5m = closes_from_klines(btc_k5 if isinstance(btc_k5, list) else [])
    btc_ret_5m = pct_returns(btc_close_5m)
    corr_penalty_th = 0.82
    corr_hard_th = 0.93
    alpha_corr_th = 0.68
    alpha_rel_strength_th = 0.25
    alpha_beta_adj_th = 0.25
    idio_spread_th = 12.0
    idio_trade_count_th = 320

    preselect: List[Dict[str, Any]] = []
    for r in top60:
        sym = str(r.get("symbol"))
        qv = sf(r.get("quoteVolume")) or 0.0
        if not sym or sym in stable_like or qv < min_qv:
            continue
        b = bmap.get(sym, {})
        bid = sf(b.get("bidPrice")) or 0.0
        ask = sf(b.get("askPrice")) or 0.0
        last = sf(r.get("lastPrice")) or 0.0
        mid = (bid + ask) / 2.0 if bid > 0 and ask > 0 else last
        spread = ((ask - bid) / mid * 10000.0) if mid > 0 and ask >= bid and bid > 0 else 999.0
        if spread <= 0 or spread > 24.0:
            continue
        preselect.append({"row": r, "symbol": sym, "quote_volume": qv, "spread_bps": spread})

    preselect.sort(key=lambda x: (x["quote_volume"], -x["spread_bps"]), reverse=True)
    universe = preselect[:36] if len(preselect) > 36 else preselect
    if len(universe) < 10:
        universe = []
        for r in top60[:36]:
            sym = str(r.get("symbol"))
            qv = sf(r.get("quoteVolume")) or 0.0
            if sym and sym not in stable_like and qv >= qv_soft:
                universe.append(
                    {
                        "row": r,
                        "symbol": sym,
                        "quote_volume": qv,
                        "spread_bps": None,
                    }
                )
    cand: List[Dict[str, Any]] = []
    for item in universe:
        r = item["row"]
        sym = str(item["symbol"])
        qv = sf(item["quote_volume"]) or 0.0
        t1, e1 = get_json("/api/v3/ticker", {"symbol": sym, "windowSize": "1h"})
        t4, e4 = get_json("/api/v3/ticker", {"symbol": sym, "windowSize": "4h"})
        dep, ed = get_json("/api/v3/depth", {"symbol": sym, "limit": 100})
        k5, ek5 = get_json("/api/v3/klines", {"symbol": sym, "interval": "5m", "limit": 120})
        k15, ek15 = get_json("/api/v3/klines", {"symbol": sym, "interval": "15m", "limit": 120})
        k1h, ek1h = get_json("/api/v3/klines", {"symbol": sym, "interval": "1h", "limit": 120})
        tr, etr = get_json("/api/v3/trades", {"symbol": sym, "limit": 600})
        ag, eag = get_json("/api/v3/aggTrades", {"symbol": sym, "limit": 600})
        for e in (e1, e4, ed, ek5, ek15, ek1h, etr, eag):
            if e:
                errors.append(e)
        if not (isinstance(k5, list) and isinstance(k15, list) and isinstance(k1h, list) and isinstance(dep, dict)):
            continue
        ret1 = sf(g(t1 or {}, "priceChangePercent")) or 0.0
        ret4 = sf(g(t4 or {}, "priceChangePercent")) or 0.0
        last = sf(r.get("lastPrice")) or 0.0
        b = bmap.get(sym, {})
        bid = sf(b.get("bidPrice")) or 0.0
        ask = sf(b.get("askPrice")) or 0.0
        mid = (bid + ask) / 2.0 if bid > 0 and ask > 0 else last
        spread = ((ask - bid) / mid * 10000.0) if mid > 0 and ask >= bid and bid > 0 else 999.0
        bids = dep.get("bids", []) if isinstance(dep, dict) else []
        asks = dep.get("asks", []) if isinstance(dep, dict) else []
        bid_qty = sum((sf(x[1]) or 0.0) for x in bids[:20] if isinstance(x, list) and len(x) >= 2)
        ask_qty = sum((sf(x[1]) or 0.0) for x in asks[:20] if isinstance(x, list) and len(x) >= 2)
        den = bid_qty + ask_qty
        imb = (bid_qty - ask_qty) / den if den > 0 else 0.0
        c5, c15, c1h = closes_from_klines(k5), closes_from_klines(k15), closes_from_klines(k1h)
        r5 = pct_returns(c5)
        corr_btc = pearson_corr(r5[-48:], btc_ret_5m[-48:]) if btc_ret_5m and r5 else None
        beta_btc = beta_to_benchmark(r5[-48:], btc_ret_5m[-48:]) if btc_ret_5m and r5 else None
        rel_strength_1h = ret1 - btc_ret1
        beta_use = beta_btc if beta_btc is not None else 1.0
        beta_adj_ret_1h = ret1 - beta_use * btc_ret1
        t5, t15, t1h = trend(ema(c5, 20), ema(c5, 50)), trend(ema(c15, 20), ema(c15, 50)), trend(ema(c1h, 20), ema(c1h, 50))
        rows15 = [{"high": sf(x[2]), "low": sf(x[3]), "close": sf(x[4])} for x in k15 if isinstance(x, list) and len(x) > 4]
        atr15 = atr(rows15, 14) or 0.0
        buy_recent = 0.0
        sell_recent = 0.0
        buy_agg = 0.0
        sell_agg = 0.0
        tr_list = tr if isinstance(tr, list) else []
        ag_list = ag if isinstance(ag, list) else []
        for x in tr_list:
            q = sf(x.get("qty")) or 0.0
            if x.get("isBuyerMaker"):
                sell_recent += q
            else:
                buy_recent += q
        for x in ag_list:
            q = sf(x.get("q")) or 0.0
            if x.get("m"):
                sell_agg += q
            else:
                buy_agg += q
        flow_source = "combined"
        buy = buy_recent + buy_agg
        sell = sell_recent + sell_agg
        if len(ag_list) >= 120 and (buy_agg + sell_agg) > 0:
            flow_source = "agg"
            buy, sell = buy_agg, sell_agg
        elif len(tr_list) >= 120 and (buy_recent + sell_recent) > 0:
            flow_source = "recent"
            buy, sell = buy_recent, sell_recent
        fr = buy / max(1e-9, sell)
        fi = (buy - sell) / max(1e-9, buy + sell)
        flow_count = len(tr_list) + len(ag_list)
        bq = sf(b.get("bidQty")) or 0.0
        aq = sf(b.get("askQty")) or 0.0
        top_imb = ((bq - aq) / max(1e-9, bq + aq)) if (bq + aq) > 0 else 0.0
        if spread > 20.0:
            continue
        if abs(ret1) < 0.05 and atr15 <= max(1e-9, last) * 0.0009:
            continue
        long_ready = (t15 == "UP" and t5 == "UP" and fr >= 1.03 and fi > 0 and t1h in ("UP", "MIX"))
        short_ready = (t15 == "DOWN" and t5 == "DOWN" and fr <= 0.97 and fi < 0 and t1h in ("DOWN", "MIX"))
        core_ready = long_ready or short_ready
        corr_abs = abs(corr_btc) if corr_btc is not None else None
        idio_quality = qv >= qv_soft and spread <= idio_spread_th and flow_count >= idio_trade_count_th
        alpha_ready = idio_quality and (
            (corr_abs is not None and corr_abs <= alpha_corr_th and abs(rel_strength_1h) >= alpha_rel_strength_th)
            or abs(beta_adj_ret_1h) >= alpha_beta_adj_th
        )
        if corr_abs is not None and corr_abs >= corr_hard_th and abs(beta_adj_ret_1h) < 0.10 and not core_ready:
            continue
        flow_aligned = ((t15 == "UP" and fr >= 1.03 and fi > 0) or (t15 == "DOWN" and fr <= 0.97 and fi < 0))
        atr_pct = atr15 / max(1e-9, last)
        score = 0.0
        score += min(22.0, math.log10(max(1.0, qv)) * 2.9)
        score += min(24.0, abs(ret1) * 8.0 + abs(ret4) * 2.0 + atr_pct * 2200.0)
        score += 20.0 if (t5 == t15 and t15 in ("UP", "DOWN")) else 8.0
        score += 14.0 if (t1h == t15) else (10.0 if t1h == "MIX" else 3.0)
        score += 15.0 if flow_aligned else 5.0
        score += 10.0 if core_ready else 0.0
        score += min(12.0, abs(rel_strength_1h) * 8.0 + abs(beta_adj_ret_1h) * 6.0)
        if alpha_ready:
            score += 8.0
        if corr_abs is not None:
            score += max(0.0, (0.78 - corr_abs) * 20.0)
        score -= min(16.0, max(0.0, spread - 5.0) * 1.6)
        if corr_abs is not None and corr_abs >= corr_penalty_th and abs(beta_adj_ret_1h) < 0.22:
            score -= min(18.0, 7.0 + (corr_abs - corr_penalty_th) * 60.0)
        if (fr <= 0.97 and imb >= 0.08) or (fr >= 1.03 and imb <= -0.08):
            score -= 10.0
        if (fr <= 0.97 and top_imb >= 0.20) or (fr >= 1.03 and top_imb <= -0.20):
            score -= 9.0
        if not idio_quality and sym not in major_set:
            score -= 14.0
        if qv < qv_soft and not core_ready:
            continue
        if sym in major_set and not core_ready:
            score -= 10.0
        if sym in major_set and corr_abs is not None and corr_abs >= 0.88 and abs(beta_adj_ret_1h) < 0.22:
            score -= 10.0
        if sym not in major_set and core_ready:
            score += 3.0
        cand.append(
            {
                "symbol": sym,
                "score": round(score, 4),
                "core_ready": core_ready,
                "idio_quality": idio_quality,
                "alpha_ready": alpha_ready,
                "is_major": sym in major_set,
                "quote_volume": round(qv, 4),
                "spread_bps": round(spread, 4),
                "ret_1h_pct": round(ret1, 6),
                "ret_4h_pct": round(ret4, 6),
                "flow_count": flow_count,
                "rel_strength_1h_vs_btc": round(rel_strength_1h, 6),
                "beta_adj_ret_1h_vs_btc": round(beta_adj_ret_1h, 6),
                "corr_5m_vs_btc": None if corr_btc is None else round(corr_btc, 6),
                "beta_5m_vs_btc": None if beta_btc is None else round(beta_btc, 6),
                "flow_source": flow_source,
                "top_imbalance": round(top_imb, 6),
            }
        )

    if not cand:
        cand = []
    cand.sort(
        key=lambda x: (
            1 if x.get("alpha_ready") else 0,
            1 if x.get("core_ready") else 0,
            1 if x.get("idio_quality") else 0,
            x.get("score", 0.0),
            x.get("quote_volume", 0.0),
        ),
        reverse=True,
    )
    target_n = min(max(0, watchlist_size), len(cand))
    max_major = 1
    if target_n >= 5:
        alpha_target = 3
    elif target_n >= 3:
        alpha_target = 2
    elif target_n >= 1:
        alpha_target = 1
    else:
        alpha_target = 0
    alpha_target = min(alpha_target, target_n)
    alpha_pool = [x for x in cand if x.get("alpha_ready") and x.get("idio_quality") and (sf(x.get("score")) or 0.0) >= 62.0]
    if len(alpha_pool) < alpha_target:
        alpha_pool = [x for x in cand if x.get("alpha_ready") and x.get("idio_quality")]
    watch: List[str] = []
    major_count = 0

    def add_from_pool(pool: List[Dict[str, Any]], needed: int, enforce_major_limit: bool = True) -> int:
        nonlocal major_count
        added = 0
        for x in pool:
            if added >= needed:
                break
            sym = str(x.get("symbol"))
            if not sym or sym in watch:
                continue
            is_major = sym in major_set
            if enforce_major_limit and is_major and major_count >= max_major:
                continue
            watch.append(sym)
            if is_major:
                major_count += 1
            added += 1
        return added

    add_from_pool(alpha_pool, alpha_target)
    add_from_pool(cand, max(0, target_n - len(watch)))
    watch = watch[:target_n]
    alpha_selected = [s for s in watch if any(str(x.get("symbol")) == s and x.get("alpha_ready") for x in cand)]
    scan = {
        "scan_time_utc": ts_utc,
        "scan_time_tokyo": ts_tokyo,
        "source": "binance.us.spot",
        "selection_mode": "extreme_precision_v2",
        "run_policy": "single_pass",
        "allow_short_watchlist": True,
        "benchmark": {
            "symbol": "BTCUSDT",
            "ret_1h_pct": round(btc_ret1, 6),
        },
        "preselect_count": len(preselect),
        "universe_count": len(universe),
        "min_quote_volume": min_qv,
        "qv_soft_floor": qv_soft,
        "max_major": max_major,
        "corr_penalty_th": corr_penalty_th,
        "corr_hard_th": corr_hard_th,
        "alpha_corr_th": alpha_corr_th,
        "alpha_rel_strength_th": alpha_rel_strength_th,
        "alpha_beta_adj_th": alpha_beta_adj_th,
        "idio_spread_th": idio_spread_th,
        "idio_trade_count_th": idio_trade_count_th,
        "alpha_target": alpha_target,
        "alpha_selected_count": len(alpha_selected),
        "alpha_selected_symbols": alpha_selected,
        "watchlist_count": len(watch),
        "watchlist_symbols": watch,
        "ranked": cand[: max(watchlist_size, 10)],
        "errors_summary": errors[:30],
    }
    stamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    scan_path = os.path.join(LOG_DIR, f"scan_watchlist_{stamp}.json")
    os.makedirs(LOG_DIR, exist_ok=True)
    ranked_symbols = [str(x.get("symbol")) for x in cand if str(x.get("symbol"))]
    with open(scan_path, "w", encoding="utf-8") as f:
        json.dump(scan, f, ensure_ascii=False, indent=2)
    return watch, ranked_symbols, scan_path


def build_analysis(snapshot_path: str, symbol: str) -> str:
    with open(snapshot_path, "r", encoding="utf-8") as f:
        s = json.load(f)
    now_utc = utc_now_iso()
    now_local = local_now_iso()
    sid = os.path.basename(snapshot_path)
    srv_ms = int(g(s, "meta.server_time_ms", int(time.time() * 1000)))
    srv_utc = str(g(s, "meta.server_time", now_utc))
    gen_utc = str(g(s, "meta.generated_at", now_utc))
    lp = sf(g(s, "market.ticker_24h.lastPrice"))
    ret1 = sf(g(s, "features.ticker_window.h1.price_change_pct"))
    ret4 = sf(g(s, "features.ticker_window.h4.price_change_pct"))
    r1 = sf(g(s, "features.ticker_window.h1.range_pct"))
    r4 = sf(g(s, "features.ticker_window.h4.range_pct"))
    spr = sf(g(s, "features.order_book.spread_bps"))
    imb = sf(g(s, "features.order_book.imbalance"))
    top_imb = sf(g(s, "features.book_ticker.imbalance_top"))
    fr = sf(g(s, "flow.trade_features.buy_sell_ratio"))
    fi = sf(g(s, "flow.trade_features.imbalance_qty"))
    flow_src = str(g(s, "flow.trade_features.flow_source", "combined") or "combined")
    rc = sf(g(s, "flow.trade_features.recent_count")) or 0.0
    ac = sf(g(s, "flow.trade_features.agg_count")) or 0.0
    news = fetch_news_signal(symbol)
    news_status = str(news.get("status", "UNKNOWN"))
    news_hits = int(news.get("hits") or 0)
    news_score = sf(news.get("score")) or 0.0
    e5, e5b, r5 = sf(g(s, "features.by_interval.5m.ema20_last")), sf(g(s, "features.by_interval.5m.ema50_last")), sf(g(s, "features.by_interval.5m.rsi14_last"))
    e15, e15b, r15 = sf(g(s, "features.by_interval.15m.ema20_last")), sf(g(s, "features.by_interval.15m.ema50_last")), sf(g(s, "features.by_interval.15m.rsi14_last"))
    e30, e30b, r30 = sf(g(s, "features.by_interval.30m.ema20_last")), sf(g(s, "features.by_interval.30m.ema50_last")), sf(g(s, "features.by_interval.30m.rsi14_last"))
    e1, e1b, r1h = sf(g(s, "features.by_interval.1h.ema20_last")), sf(g(s, "features.by_interval.1h.ema50_last")), sf(g(s, "features.by_interval.1h.rsi14_last"))
    at = sf(g(s, "features.by_interval.15m.atr14_last"))
    t5, t15, t30, t1h = trend(e5, e5b), trend(e15, e15b), trend(e30, e30b), trend(e1, e1b)
    mtf = "aligned" if t5 == t15 == t1h and t15 in ("UP", "DOWN") else ("partial" if len({t5, t15, t1h} - {"MIX"}) <= 2 else "conflict")
    bid, ask = sf(g(s, "market.book_ticker.bidPrice")), sf(g(s, "market.book_ticker.askPrice"))
    mid = ((bid + ask) / 2.0) if (bid and ask and bid > 0 and ask > 0) else lp
    entry_source = "last_price" if lp is not None else "book_mid_fallback"
    entry = lp if lp is not None else mid
    emd = (entry - mid) if (entry is not None and mid is not None) else 0.0
    emd_atr = abs(emd) / max(1e-9, at or 0.0) if at else 0.0
    srv_dt = dt.datetime.fromisoformat(srv_utc.replace("Z", "+00:00"))
    age = max(0.0, (dt.datetime.now(dt.timezone.utc) - srv_dt).total_seconds())

    c5, c15, c30, c1h = len(g(s, "candles.regular.5m", []) or []), len(g(s, "candles.regular.15m", []) or []), len(g(s, "candles.regular.30m", []) or []), len(g(s, "candles.regular.1h", []) or [])
    k1 = lp is not None
    k2 = (spr is not None) and (imb is not None) and (len(g(s, "order_book.bids", []) or []) > 0) and (len(g(s, "order_book.asks", []) or []) > 0)
    k3 = (c5 >= 120 and c15 >= 96 and (c30 >= 72 or c1h >= 48))
    k4 = (rc + ac) >= 80
    k5 = (ret1 is not None) and (ret4 is not None)
    k6 = sf(g(s, "features.book_ticker.spread_bps")) is not None
    k7 = age <= 1200
    miss = [f"K{i}" for i, ok in enumerate([k1, k2, k3, k4, k5, k6, k7], 1) if not ok]
    err_n = len(g(s, "errors", {}) or {})
    dq = "LOW" if (len(miss) >= 5 or (not k1) or (not k3) or (not k7)) else ("MED" if (3 <= len(miss) <= 4 or err_n > 0) else "HIGH")

    long_core = {"LONG_CORE_15m_up": t15 == "UP", "LONG_CORE_5m_up": t5 == "UP", "LONG_CORE_flow_ratio_ge_1p03": (fr is not None and fr >= 1.03), "LONG_CORE_flow_imb_positive": (fi is not None and fi > 0), "LONG_CORE_1h_not_down": t1h in ("UP", "MIX")}
    short_core = {"SHORT_CORE_15m_down": t15 == "DOWN", "SHORT_CORE_5m_down": t5 == "DOWN", "SHORT_CORE_flow_ratio_le_0p97": (fr is not None and fr <= 0.97), "SHORT_CORE_flow_imb_negative": (fi is not None and fi < 0), "SHORT_CORE_1h_not_up": t1h in ("DOWN", "MIX")}
    lscore = 18 + sum(14 for v in long_core.values() if v) + (4 if (ret1 or 0) > 0 else 0) + (2 if flow_src == "agg" else 0)
    sscore = 18 + sum(14 for v in short_core.values() if v) + (4 if (ret1 or 0) < 0 else 0) + (2 if flow_src == "agg" else 0)
    conflict = (12.0 if ((t15 == "UP" and t1h == "DOWN") or (t15 == "DOWN" and t1h == "UP")) else 0.0)
    if (fr is not None and imb is not None) and ((fr <= 0.97 and imb >= 0.08) or (fr >= 1.03 and imb <= -0.08)):
        conflict += 10.0
    flow_top_div = (fr is not None and top_imb is not None) and ((fr <= 0.97 and top_imb >= 0.20) or (fr >= 1.03 and top_imb <= -0.20))
    if flow_top_div:
        conflict += 10.0
    liq = max(0.0, ((spr or 0.0) - 8.0) * 1.8)
    conf = clamp(max(lscore, sscore) - conflict - liq, 0.0, 99.0)
    action = "WAIT"
    if all(long_core.values()) and lscore >= sscore:
        action = "LONG"
    elif all(short_core.values()) and sscore > lscore:
        action = "SHORT"
    raw_action = action
    core_complete = all(long_core.values()) if raw_action == "LONG" else (all(short_core.values()) if raw_action == "SHORT" else False)
    news_conflict_long = news_hits >= 2 and news_score <= -0.30
    news_conflict_short = news_hits >= 2 and news_score >= 0.30
    news_penalty = 0.0
    if raw_action == "LONG":
        if news_conflict_long:
            news_penalty = 8.0
        elif news_hits >= 2 and news_score >= 0.30:
            news_penalty = -2.0
    elif raw_action == "SHORT":
        if news_conflict_short:
            news_penalty = 8.0
        elif news_hits >= 2 and news_score <= -0.30:
            news_penalty = -2.0
    if news_penalty > 0:
        conf = max(0.0, conf - news_penalty)
    elif news_penalty < 0:
        conf = min(99.0, conf + abs(news_penalty))
    blockers: List[str] = []
    if raw_action == "LONG" and not all(long_core.values()):
        blockers.append("long_core_incomplete")
    if raw_action == "SHORT" and not all(short_core.values()):
        blockers.append("short_core_incomplete")
    if raw_action == "SHORT" and (r1h is not None and r1h <= 34 and (ret1 or 0.0) >= 0):
        blockers.append("short_rebound_risk")
    if (fr is not None and imb is not None) and ((fr <= 0.97 and imb >= 0.08) or (fr >= 1.03 and imb <= -0.08)):
        blockers.append("flow_depth_divergence")
    if flow_top_div:
        blockers.append("flow_top_divergence")
    if emd_atr > 0.25:
        blockers.append("entry_price_mismatch")
    if raw_action == "LONG" and news_conflict_long:
        blockers.append("news_conflict_bearish")
    if raw_action == "SHORT" and news_conflict_short:
        blockers.append("news_conflict_bullish")
    blockers = list(dict.fromkeys(blockers))
    if raw_action == "LONG" and (ret1 is not None and ret1 < 0):
        conf = min(conf, 82.0)
    if raw_action == "SHORT" and (ret1 is not None and ret1 > 0):
        conf = min(conf, 82.0)
    if raw_action in ("LONG", "SHORT") and abs(ret1 or 0.0) < 0.12 and abs(ret4 or 0.0) < 0.18:
        conf = min(conf, 74.0)
    if raw_action in ("LONG", "SHORT"):
        conf = min(conf, 96.0)
    action = raw_action
    if raw_action in ("LONG", "SHORT") and blockers:
        action = "WAIT"
    if action == "WAIT":
        conf = min(conf, 45.0 if dq != "LOW" else 35.0)
    if err_n > 0:
        conf = min(conf, 50.0)
    if dq == "LOW":
        conf = min(conf, 35.0)

    sl, tp1, tp2 = entry, entry, entry
    if entry is None:
        entry = lp or 0.0
    if at and at > 0:
        if action == "SHORT":
            sl, tp1, tp2 = entry + 1.2 * at, entry - 0.8 * at, entry - 1.6 * at
        else:
            sl, tp1, tp2 = entry - 1.2 * at, entry + 0.8 * at, entry + 1.6 * at
    if action != "WAIT":
        wait_p = "none"
        wait_s = "none"
    else:
        if "flow_depth_divergence" in blockers:
            wait_p = "flow_depth_divergence"
        elif "flow_top_divergence" in blockers:
            wait_p = "flow_top_divergence"
        elif "news_conflict_bearish" in blockers or "news_conflict_bullish" in blockers:
            wait_p = "news_conflict"
        elif any(x in blockers for x in ("long_core_incomplete", "short_core_incomplete")):
            wait_p = "core_incomplete"
        else:
            wait_p = "mtf_conflict_or_low_confidence"
        wait_s = "liquidity_penalty" if liq > 0 else ("news_risk" if news_hits >= 2 and abs(news_score) >= 0.30 else "trend_conflict")
    out = os.path.join(LOG_DIR, f"analysis_{symbol}_{srv_ms}.txt")
    lines: List[str] = []
    lines.append(f"snapshot_id={sid} | snapshot_server_time_utc={srv_utc} | analysis_time_utc={now_utc} | analysis_time_local={now_local} | horizon=30m~1h | mode=precision | data_quality={dq}")
    lines += ["", "[SNAPSHOT_META]", f"symbol={symbol}", f"server_time={srv_utc}", f"generated_at={gen_utc}", f"snapshot_age_sec={age:.3f}", f"errors_count={err_n}", f"self_check_status={g(s,'diagnostics.self_check.status','UNKNOWN')}", f"news_status={news_status}", f"news_hits={news_hits}", f"news_score={news_score}", "source_endpoints=/api/v3/time,/api/v3/ticker/24hr,/api/v3/ticker?windowSize=1h,/api/v3/ticker?windowSize=4h,/api/v3/ticker/bookTicker,/api/v3/depth,/api/v3/klines,/api/v3/trades,/api/v3/aggTrades"]
    lines += ["", "[DATA_QUALITY_GATE]", f"K1={'PASS' if k1 else 'FAIL'} | value_lastPrice={lp} | rule=lastPrice_non_null", f"K2={'PASS' if k2 else 'FAIL'} | value_spread_bps={spr} | rule=orderbook_non_empty", f"K3={'PASS' if k3 else 'FAIL'} | value_len_5m={c5}_len_15m={c15}_len_30m={c30}_len_1h={c1h} | rule=5m>=120_15m>=96_(30m>=72_or_1h>=48)", f"K4={'PASS' if k4 else 'FAIL'} | value_trade_count={int(rc+ac)} | rule=recent_plus_agg_ge_80", f"K5={'PASS' if k5 else 'FAIL'} | value_ret_1h={ret1}_ret_4h={ret4} | rule=ticker_window_h1_h4_present", f"K6={'PASS' if k6 else 'FAIL'} | value_book_spread_bps={sf(g(s,'features.book_ticker.spread_bps'))} | rule=book_ticker_spread_present", f"K7={'PASS' if k7 else 'FAIL'} | value_snapshot_age_sec={age:.3f} | rule=freshness_le_1200s", f"missing_key_modules={miss}"]
    lines += ["", "[FEATURE_TABLE]", f"lastPrice={lp}", f"ret_1h_pct={ret1}", f"ret_4h_pct={ret4}", f"range_1h_pct={r1}", f"range_4h_pct={r4}", f"spread_bps={spr}", f"imbalance={imb}", f"top_imbalance={top_imb}", f"flow_source={flow_src}", f"flow_ratio={fr}", f"flow_imbalance_qty={fi}", f"atr15={at}", f"news_score={news_score}", f"news_hits={news_hits}"]
    lines += ["", "[MULTI_TIMEFRAME]", f"5m_EMA20={e5} | 5m_EMA50={e5b} | 5m_RSI={r5} | 5m_trend={t5}", f"15m_EMA20={e15} | 15m_EMA50={e15b} | 15m_RSI={r15} | 15m_trend={t15}", f"30m_EMA20={e30} | 30m_EMA50={e30b} | 30m_RSI={r30} | 30m_trend={t30}", f"1h_EMA20={e1} | 1h_EMA50={e1b} | 1h_RSI={r1h} | 1h_trend={t1h}", f"mtf_alignment={mtf}"]
    lines += ["", "[FLOW_AND_ORDERBOOK]", f"recent_count={int(rc)}", f"agg_count={int(ac)}", f"flow_source={flow_src}", f"buy_qty={sf(g(s,'flow.trade_features.buy_qty'))}", f"sell_qty={sf(g(s,'flow.trade_features.sell_qty'))}", f"buy_sell_ratio={fr}", f"imbalance_qty={fi}", f"depth_imbalance={imb}", f"orderbook_top_imbalance={top_imb}", f"spread_bps={spr}"]
    lines += ["", "[SCORING_BREAKDOWN]", f"long_score={lscore:.2f}", f"short_score={sscore:.2f}", f"conflict_penalty={conflict:.2f}", f"liquidity_penalty={liq:.2f}", f"news_penalty={news_penalty:.2f}", f"final_confidence={conf:.2f}"]
    lines += ["", "[DECISION_TRACE]"]
    for k, v in long_core.items():
        lines.append(f"{k}={b2s(v)}")
    for k, v in short_core.items():
        lines.append(f"{k}={b2s(v)}")
    lines += [f"raw_action={raw_action}", f"core_complete={b2s(core_complete)}", f"hard_blockers={blockers}", f"news_status={news_status}", f"news_hits={news_hits}", f"news_score={news_score}", f"wait_reason_primary={wait_p}", f"wait_reason_secondary={wait_s}"]
    lines += ["", "[RISK_PLAN]", "entry_rule=use_last_price_with_book_mid_guard", f"entry_source={entry_source}", f"entry={entry}", f"entry_mid_diff={emd}", f"entry_mid_diff_atr={emd_atr}", f"sl={sl}", f"tp1={tp1}", f"tp2={tp2}", "position_size=0.0500", "leverage_cap=3"]
    lines += ["", "[INVALIDATION_AND_TIMEOUT]", f"invalidation_1_mtf_conflict={1 if mtf == 'conflict' else 0}", f"invalidation_2_flow_ratio_cross_1p00={1 if ((fr or 1.0) < 1.0 and action == 'LONG') or ((fr or 1.0) > 1.0 and action == 'SHORT') else 0}", f"invalidation_3_spread_bps_gt={max(10.0, (spr or 0.0) + 3.0):.4f}", f"invalidation_4_top_imbalance_divergence={1 if flow_top_div else 0}", "timeout_exit_min=45"]
    lines += ["", "[COUNTER_SCENARIO]", f"counter_trigger_1_trend15m={'DOWN' if action == 'LONG' else 'UP'}", f"counter_trigger_2_flow_ratio_{'le' if action == 'LONG' else 'ge'}={'0.97' if action == 'LONG' else '1.03'}", f"counter_trigger_3_price_{'lt' if action == 'LONG' else 'gt'}_ema50_15m={e15b}"]
    lines += ["", "[FINAL_DECISION]", f"action={action}", f"confidence={int(round(conf))}", f"data_quality={dq}", f"wait_reason_if_any={wait_p if action == 'WAIT' else 'none'}", "execution_note=generated_by_realtime_master_agent"]
    with open(out, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")
    return out


def stage_b_symbol(symbol: str) -> Dict[str, Any]:
    sc = run_cmd([sys.executable, SELF_CHECK_PY, "--symbol", symbol])
    if sc.returncode != 0 or "status=FAIL" in (sc.stdout + sc.stderr):
        raise RuntimeError(f"self_check failed for {symbol}: {sc.stdout}\n{sc.stderr}")
    rid = int(time.time() * 1000)
    snap_path = os.path.join(LOG_DIR, f"snapshot_{symbol}_{rid}.json")
    cmd = [sys.executable, DATA_PY, "--symbol", symbol, "--intervals", "1m", "3m", "5m", "15m", "30m", "1h", "--kline-limit", "360", "--hist-period", "15m", "--hist-limit", "192", "--depth", "150", "--include-exchange-info", "--include-trades", "--trades-limit", "1000", "--out", snap_path, "--strict"]
    sp = run_cmd(cmd)
    if sp.returncode != 0:
        cmd2 = [x for x in cmd if x != "--strict"]
        sp2 = run_cmd(cmd2)
        if sp2.returncode != 0:
            raise RuntimeError(f"data.py failed for {symbol}: {sp.stdout}\n{sp.stderr}\n{sp2.stdout}\n{sp2.stderr}")
    agent_path = snap_path.replace(".json", "_agent.txt")
    analysis_path = build_analysis(snap_path, symbol)
    gd = run_cmd([sys.executable, GUARD_PY, "--analysis", analysis_path, "--inplace"])
    if gd.returncode != 0 or "status=FAIL" in (gd.stdout + gd.stderr):
        raise RuntimeError(f"guard failed for {symbol}: {gd.stdout}\n{gd.stderr}")
    final = parse_final_decision(analysis_path)
    return {
        "symbol": symbol,
        "agent_path": os.path.abspath(agent_path),
        "analysis_path": os.path.abspath(analysis_path),
        "action": final.get("action", "WAIT"),
        "confidence": final.get("confidence", 0),
        "data_quality": final.get("data_quality", "UNKNOWN"),
        "guard_stdout": gd.stdout.strip(),
    }


def main() -> int:
    ap = argparse.ArgumentParser(description="Realtime 0_master_prompt runner")
    ap.add_argument("--watchlist-size", type=int, default=5)
    ap.add_argument("--tradeable-only", action="store_true", help="Deprecated: kept for compatibility.")
    ap.add_argument("--min-confidence", type=float, default=68.0, help="Minimum confidence for actionable LONG/SHORT.")
    ap.add_argument("--max-candidates", type=int, default=20, help="Deprecated: kept for compatibility.")
    args = ap.parse_args()
    os.makedirs(LOG_DIR, exist_ok=True)
    watch_n = max(1, min(args.watchlist_size, 5))
    watch, ranked_symbols, scan_path = stage_a_scan(watch_n)
    print(f"STAGEA_WATCHLIST_SYMBOLS={' '.join(watch)}")
    results: List[Dict[str, Any]] = []
    for sym in watch:
        r = stage_b_symbol(sym)
        results.append(r)
        print(f"AGENT_PATH = {r['agent_path']}")
        print(f"ANALYSIS_PATH = {r['analysis_path']}")

    actionable: List[Dict[str, Any]] = []
    for r in results:
        action = str(r.get("action", "WAIT")).upper()
        conf = sf(r.get("confidence")) or 0.0
        dq = str(r.get("data_quality", "UNKNOWN")).upper()
        if action in ("LONG", "SHORT") and conf >= args.min_confidence and dq in ("HIGH", "MED"):
            actionable.append(r)

    no_trade = len(actionable) == 0
    if no_trade:
        final_watch: List[str] = []
        no_trade_reason = "all_symbols_wait_or_no_qualified_candidates"
        next_trigger_1 = "flow_divergence_clears"
        next_trigger_2 = "mtf_alignment_5m_15m_1h"
        print("NO_TRADE=true")
        print(f"NO_TRADE_REASON={no_trade_reason}")
        print(f"NEXT_TRIGGER_1={next_trigger_1}")
        print(f"NEXT_TRIGGER_2={next_trigger_2}")
    else:
        final_watch = [str(r.get("symbol")) for r in actionable if str(r.get("symbol"))]
        no_trade_reason = "none"
        next_trigger_1 = "none"
        next_trigger_2 = "none"

    watchlist_line = f"WATCHLIST_SYMBOLS={' '.join(final_watch)}"
    print(watchlist_line)
    res_path = os.path.join(LOG_DIR, f"master_realtime_run_{dt.datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
    with open(res_path, "w", encoding="utf-8") as f:
        json.dump(
            {
                "scan_path": scan_path,
                "stagea_watchlist_symbols": watch,
                "watchlist_symbols": final_watch,
                "actionable_count": len(actionable),
                "no_trade": no_trade,
                "no_trade_reason": no_trade_reason,
                "next_trigger_1": next_trigger_1,
                "next_trigger_2": next_trigger_2,
                "tradeable_only": args.tradeable_only,
                "min_confidence": args.min_confidence,
                "results": results,
                "ranked_symbol_count": len(ranked_symbols),
                "run_time_utc": utc_now_iso(),
            },
            f,
            ensure_ascii=False,
            indent=2,
        )
    print(f"SCAN_PATH={os.path.abspath(scan_path)}")
    print(f"RESULT_PATH={os.path.abspath(res_path)}")
    print(watchlist_line)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
