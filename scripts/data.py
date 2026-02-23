# data.py
# Binance.US Spot 行情快照抓取脚本
# 依赖：标准库 + requests
import sys
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8")

import argparse
import datetime as dt
import json
import os
import time
from typing import Any, Dict, List, Optional, Tuple

import requests

SYMBOL_DEFAULT = "DOGEUSDT"
BASE_DEFAULT = "https://api.binance.us"

INTERVALS_DEFAULT = "1m,3m,5m,15m,30m,1h"
KLINE_LIMIT_DEFAULT = 360

HIST_PERIOD_DEFAULT = "15m"
HIST_LIMIT_DEFAULT = 192

DEPTH_LEVELS_DEFAULT = 50
TRADES_LIMIT_DEFAULT = 1000

TIMEOUT_DEFAULT = 12
MAX_RETRIES_DEFAULT = 4

VALID_KLINE_INTERVALS = {
    "1m", "3m", "5m", "15m", "30m",
    "1h", "2h", "4h", "6h", "8h", "12h",
    "1d", "3d", "1w", "1M",
}
VALID_HIST_PERIODS = {"5m", "15m", "30m", "1h", "2h", "4h", "6h", "12h", "1d"}

# Binance.US spot 不提供 futures data（taker/funding/OI 等衍生品指标）。
TAKER_ENDPOINTS: List[str] = []
TAKER_FALLBACK_PERIODS: List[str] = []


def utc_now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z")


def ms_to_iso_utc(ms: int) -> str:
    return dt.datetime.fromtimestamp(ms / 1000.0, tz=dt.timezone.utc).isoformat().replace("+00:00", "Z")


def safe_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        if isinstance(x, (int, float)):
            return float(x)
        s = str(x).strip()
        if not s:
            return None
        return float(s)
    except Exception:
        return None


def clamp_int(x: int, lo: int, hi: int) -> int:
    return max(lo, min(hi, x))


def parse_csv_list(s: str) -> List[str]:
    parts = [p.strip() for p in (s or "").split(",")]
    return [p for p in parts if p]


def normalize_intervals_arg(arg_val: Any) -> List[str]:
    tokens: List[str] = []
    if arg_val is None:
        return tokens
    if isinstance(arg_val, list):
        for t in arg_val:
            tokens.extend(parse_csv_list(str(t)))
    else:
        tokens.extend(parse_csv_list(str(arg_val)))
    return [t.strip() for t in tokens if t and str(t).strip()]


def validate_intervals(intervals: List[str]) -> List[str]:
    if not intervals:
        raise ValueError("intervals 不能为空")

    digits_only = [iv for iv in intervals if iv.isdigit()]
    if digits_only:
        raise ValueError(
            f"检测到疑似 PowerShell 参数被拆分：{digits_only}。\n"
            f'请用 --intervals 1m 3m 5m 15m 30m 1h 或 --intervals "1m,3m,5m,15m,30m,1h"\n'
            f"当前收到：{intervals}"
        )

    bad = [iv for iv in intervals if iv not in VALID_KLINE_INTERVALS]
    if bad:
        raise ValueError(f"不合法 interval：{bad}。只允许：{sorted(VALID_KLINE_INTERVALS)}")

    seen = set()
    out = []
    for iv in intervals:
        if iv not in seen:
            seen.add(iv)
            out.append(iv)
    return out


def validate_hist_period(period: str) -> str:
    p = (period or "").strip()
    if p not in VALID_HIST_PERIODS:
        raise ValueError(f"不合法 hist-period：{p}。只允许：{sorted(VALID_HIST_PERIODS)}")
    return p


def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": "binance-us-spot-snapshot/2.0",
        "Accept": "application/json",
    })
    return s


def _sleep(sec: float) -> None:
    if sec and sec > 0:
        time.sleep(sec)


def http_get_json(
    session: requests.Session,
    base_url: str,
    path: str,
    params: Optional[Dict[str, Any]] = None,
    timeout: int = TIMEOUT_DEFAULT,
    max_retries: int = MAX_RETRIES_DEFAULT,
) -> Tuple[Optional[Any], Optional[Dict[str, Any]]]:
    url = base_url.rstrip("/") + path
    last_err: Optional[Dict[str, Any]] = None

    for attempt in range(max_retries + 1):
        try:
            resp = session.get(url, params=params, timeout=timeout)
            status = resp.status_code

            if status in (429, 418):
                ra = resp.headers.get("Retry-After")
                ra_sec = safe_float(ra) if ra else None
                wait = ra_sec if ra_sec is not None else ((2 ** attempt) * 1.5)
                last_err = {"path": path, "params": params or {}, "status_code": status, "error_text": (resp.text or "")[:800]}
                if attempt < max_retries:
                    _sleep(wait)
                    continue
                return None, last_err

            if status in (500, 502, 503, 504):
                last_err = {"path": path, "params": params or {}, "status_code": status, "error_text": (resp.text or "")[:800]}
                if attempt < max_retries:
                    _sleep((2 ** attempt) * 0.8)
                    continue
                return None, last_err

            if not (200 <= status < 300):
                return None, {"path": path, "params": params or {}, "status_code": status, "error_text": (resp.text or "")[:800]}

            try:
                return resp.json(), None
            except Exception as je:
                return None, {
                    "path": path,
                    "params": params or {},
                    "status_code": status,
                    "error_text": f"JSON parse error: {je}; body={(resp.text or '')[:250]}",
                }

        except requests.Timeout as te:
            last_err = {"path": path, "params": params or {}, "status_code": None, "error_text": f"Timeout: {te}"}
            if attempt < max_retries:
                _sleep((2 ** attempt) * 0.8)
                continue
            return None, last_err

        except requests.RequestException as re:
            last_err = {"path": path, "params": params or {}, "status_code": None, "error_text": f"RequestException: {re}"}
            if attempt < max_retries:
                _sleep((2 ** attempt) * 0.8)
                continue
            return None, last_err

    return None, last_err or {"path": path, "params": params or {}, "status_code": None, "error_text": "Unknown error"}


def record_error(errors: Dict[str, Any], key: str, err: Dict[str, Any]) -> None:
    errors[key] = {
        "path": err.get("path"),
        "params": err.get("params", {}),
        "status_code": err.get("status_code"),
        "error": err.get("error_text"),
        "ts_utc": utc_now_iso(),
    }


def deep_get(obj: Any, path: str, default: Any = None) -> Any:
    cur = obj
    for part in path.split("."):
        if isinstance(cur, dict) and part in cur:
            cur = cur[part]
        else:
            return default
    return cur


def parse_iso_utc(ts: Any) -> Optional[dt.datetime]:
    if not isinstance(ts, str) or not ts.strip():
        return None
    try:
        return dt.datetime.fromisoformat(ts.replace("Z", "+00:00")).astimezone(dt.timezone.utc)
    except Exception:
        return None


def diagnose_error(err: Dict[str, Any]) -> Dict[str, Any]:
    status = err.get("status_code")
    path = str(err.get("path") or "")
    msg = str(err.get("error") or "")
    likely_cause = "unknown"
    retryable = False
    action = "检查网络状态与接口参数"

    if status in (418, 429):
        likely_cause = "rate_limit"
        retryable = True
        action = "触发限流；降低频率、减少 symbol 并重试"
    elif status in (500, 502, 503, 504):
        likely_cause = "exchange_server_error"
        retryable = True
        action = "交易所服务端波动；短暂等待后重试"
    elif status == 404:
        likely_cause = "endpoint_not_supported_or_not_found"
        retryable = False
        action = "端点不存在或当前市场不支持；检查 API 路径/市场类型"
    elif status == 400:
        likely_cause = "bad_request_param"
        retryable = False
        action = "请求参数不合法；检查 symbol/interval/windowSize/limit"
    elif status == 403:
        likely_cause = "forbidden_or_geo_restricted"
        retryable = False
        action = "访问受限；检查网络出口与 API 使用范围"
    elif "Timeout:" in msg:
        likely_cause = "network_timeout"
        retryable = True
        action = "网络超时；增大 timeout 或稍后重试"
    elif "RequestException:" in msg:
        likely_cause = "network_exception"
        retryable = True
        action = "网络异常；检查 DNS/代理/连通性后重试"
    elif "JSON parse error" in msg:
        likely_cause = "invalid_json_payload"
        retryable = True
        action = "返回体非预期 JSON；稍后重试并记录异常返回"

    return {
        "path": path,
        "status_code": status,
        "likely_cause": likely_cause,
        "retryable": retryable,
        "suggested_action": action,
    }


def build_error_diagnostics(errors: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    if not isinstance(errors, dict):
        return out
    for key, err in errors.items():
        if isinstance(err, dict):
            out[key] = diagnose_error(err)
    return out


def run_spot_self_check(snapshot: Dict[str, Any]) -> Dict[str, Any]:
    intervals = deep_get(snapshot, "meta.intervals", []) or []
    checks: Dict[str, bool] = {}

    checks["meta.server_time_ms"] = isinstance(deep_get(snapshot, "meta.server_time_ms"), int)
    checks["market.last_price"] = safe_float(deep_get(snapshot, "market.ticker_24h.lastPrice")) is not None

    bids = deep_get(snapshot, "order_book.bids", []) or []
    asks = deep_get(snapshot, "order_book.asks", []) or []
    checks["order_book.depth"] = (
        isinstance(bids, list) and len(bids) > 0 and isinstance(asks, list) and len(asks) > 0
    )

    checks["order_book.spread_imbalance"] = (
        safe_float(deep_get(snapshot, "features.order_book.spread_bps")) is not None
        and safe_float(deep_get(snapshot, "features.order_book.imbalance")) is not None
    )

    for iv, need in (("5m", 120), ("15m", 96), ("30m", 72), ("1h", 48)):
        if iv in intervals:
            rows = deep_get(snapshot, f"candles.regular.{iv}", []) or []
            checks[f"candles.{iv}"] = isinstance(rows, list) and len(rows) >= need

    recent_cnt = safe_float(deep_get(snapshot, "flow.trade_features.recent_count")) or 0.0
    agg_cnt = safe_float(deep_get(snapshot, "flow.trade_features.agg_count")) or 0.0
    checks["flow.trade_count"] = (recent_cnt + agg_cnt) >= 80.0
    checks["book_ticker.spread"] = safe_float(deep_get(snapshot, "features.book_ticker.spread_bps")) is not None
    checks["ticker_window.h1_h4"] = (
        safe_float(deep_get(snapshot, "features.ticker_window.h1.price_change_pct")) is not None
        and safe_float(deep_get(snapshot, "features.ticker_window.h4.price_change_pct")) is not None
    )
    checks["avg_price"] = safe_float(deep_get(snapshot, "market.avg_price.price")) is not None

    generated_at = parse_iso_utc(deep_get(snapshot, "meta.generated_at"))
    server_time = parse_iso_utc(deep_get(snapshot, "meta.server_time"))
    freshness_sec = None
    if generated_at is not None and server_time is not None:
        freshness_sec = abs((generated_at - server_time).total_seconds())
    checks["freshness<=20m"] = freshness_sec is not None and freshness_sec <= 20 * 60

    critical = [
        "meta.server_time_ms",
        "market.last_price",
        "order_book.depth",
        "order_book.spread_imbalance",
        "flow.trade_count",
        "ticker_window.h1_h4",
        "freshness<=20m",
    ]
    if "5m" in intervals:
        critical.append("candles.5m")
    if "15m" in intervals:
        critical.append("candles.15m")
    if "30m" in intervals:
        critical.append("candles.30m")
    elif "1h" in intervals:
        critical.append("candles.1h")

    failed = [k for k, ok in checks.items() if not ok]
    failed_critical = [k for k in critical if not checks.get(k, False)]
    failed_non_critical = [k for k in failed if k not in failed_critical]

    errors = snapshot.get("errors", {}) if isinstance(snapshot, dict) else {}
    errors_count = len(errors) if isinstance(errors, dict) else 0

    status = "PASS"
    if failed_critical:
        status = "FAIL"
    elif failed_non_critical or errors_count > 0:
        status = "WARN"

    return {
        "status": status,
        "checked_at_utc": utc_now_iso(),
        "errors_count": errors_count,
        "freshness_sec": freshness_sec,
        "checks": checks,
        "failed": failed,
        "failed_critical": failed_critical,
        "failed_non_critical": failed_non_critical,
    }


def parse_kline_rows(rows: Any) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    if not isinstance(rows, list):
        return out
    for r in rows:
        if not isinstance(r, list) or len(r) < 7:
            continue
        ot = int(r[0])
        ct = int(r[6]) if len(r) > 6 else None
        out.append({
            "open_time_ms": ot,
            "open_time": ms_to_iso_utc(ot),
            "open": safe_float(r[1]),
            "high": safe_float(r[2]) if len(r) > 2 else None,
            "low": safe_float(r[3]) if len(r) > 3 else None,
            "close": safe_float(r[4]) if len(r) > 4 else None,
            "volume": safe_float(r[5]) if len(r) > 5 else None,
            "close_time_ms": ct,
            "close_time": ms_to_iso_utc(ct) if isinstance(ct, int) else None,
            "trades": safe_float(r[8]) if len(r) > 8 else None,
            "taker_buy_base": safe_float(r[9]) if len(r) > 9 else None,
            "taker_buy_quote": safe_float(r[10]) if len(r) > 10 else None,
        })
    return out


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
    gains = []
    losses = []
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
    trs = []
    for i in range(1, n):
        trs.append(max(
            highs[i] - lows[i],
            abs(highs[i] - closes[i - 1]),
            abs(lows[i] - closes[i - 1]),
        ))
    if len(trs) < period:
        return None
    atr = sum(trs[:period]) / period
    for tr in trs[period:]:
        atr = (atr * (period - 1) + tr) / period
    return atr


def order_book_features(bids: List[List[Any]], asks: List[List[Any]]) -> Dict[str, Optional[float]]:
    if not bids or not asks:
        return {"spread_bps": None, "imbalance": None}
    best_bid = safe_float(bids[0][0]) if len(bids[0]) >= 1 else None
    best_ask = safe_float(asks[0][0]) if len(asks[0]) >= 1 else None
    if best_bid is None or best_ask is None or best_bid <= 0 or best_ask <= 0:
        return {"spread_bps": None, "imbalance": None}
    mid = (best_bid + best_ask) / 2.0
    spread_bps = ((best_ask - best_bid) / mid) * 10000.0 if mid > 0 else None

    bid_qty = 0.0
    ask_qty = 0.0
    for p, q in bids:
        qq = safe_float(q)
        if qq is not None:
            bid_qty += qq
    for p, q in asks:
        qq = safe_float(q)
        if qq is not None:
            ask_qty += qq
    denom = bid_qty + ask_qty
    imbalance = ((bid_qty - ask_qty) / denom) if denom > 0 else None
    return {"spread_bps": spread_bps, "imbalance": imbalance}


def pick_base_interval(intervals: List[str]) -> str:
    for iv in ("15m", "30m", "5m", "1h", "3m", "1m"):
        if iv in intervals:
            return iv
    return intervals[0]


def is_binance_us_spot(base_url: str) -> bool:
    host = (base_url or "").lower()
    return "api.binance.us" in host


def load_exchange_info_cache(cache_path: str, max_age_sec: int = 86400) -> Optional[Dict[str, Any]]:
    try:
        if not os.path.exists(cache_path):
            return None
        st = os.stat(cache_path)
        if time.time() - st.st_mtime > max_age_sec:
            return None
        with open(cache_path, "r", encoding="utf-8") as f:
            obj = json.load(f)
        if isinstance(obj, dict) and "data" in obj:
            return obj["data"]
        return None
    except Exception:
        return None


def save_exchange_info_cache(cache_path: str, data: Any) -> None:
    try:
        obj = {"saved_at_utc": utc_now_iso(), "data": data}
        with open(cache_path, "w", encoding="utf-8") as f:
            json.dump(obj, f, ensure_ascii=False)
    except Exception:
        pass


def compute_candle_features(candles: List[Dict[str, Any]]) -> Dict[str, Any]:
    closes: List[float] = []
    highs: List[float] = []
    lows: List[float] = []
    for c in candles or []:
        if not isinstance(c, dict):
            continue
        cl, hi, lo = c.get("close"), c.get("high"), c.get("low")
        if isinstance(cl, (int, float)) and isinstance(hi, (int, float)) and isinstance(lo, (int, float)):
            closes.append(float(cl)); highs.append(float(hi)); lows.append(float(lo))

    last_close = closes[-1] if closes else None
    ret_1_candle_pct = None
    if len(closes) >= 2 and closes[-2] != 0:
        ret_1_candle_pct = (closes[-1] - closes[-2]) / closes[-2] * 100.0

    return {
        "last_close": last_close,
        "ret_1_candle_pct": ret_1_candle_pct,
        "rsi14_last": rsi_last(closes, 14) if closes else None,
        "atr14_last": atr_last(highs, lows, closes, 14) if closes else None,
        "ema20_last": ema_last(closes, 20) if closes else None,
        "ema50_last": ema_last(closes, 50) if closes else None,
    }


def to_pct(a: Optional[float], b: Optional[float]) -> Optional[float]:
    if a is None or b is None or b == 0:
        return None
    return (a - b) / b * 100.0


def ticker_window_features(ticker: Any) -> Dict[str, Optional[float]]:
    if not isinstance(ticker, dict):
        return {
            "last_price": None,
            "open_price": None,
            "high_price": None,
            "low_price": None,
            "quote_volume": None,
            "price_change_pct": None,
            "range_pct": None,
        }
    last_price = safe_float(ticker.get("lastPrice"))
    open_price = safe_float(ticker.get("openPrice"))
    high_price = safe_float(ticker.get("highPrice"))
    low_price = safe_float(ticker.get("lowPrice"))
    quote_volume = safe_float(ticker.get("quoteVolume"))
    pcp = safe_float(ticker.get("priceChangePercent"))
    range_pct = to_pct(high_price, open_price) if high_price is not None and open_price is not None else None
    if range_pct is not None and low_price is not None and open_price not in (None, 0):
        lo = to_pct(low_price, open_price)
        if lo is not None:
            range_pct = range_pct - lo
    return {
        "last_price": last_price,
        "open_price": open_price,
        "high_price": high_price,
        "low_price": low_price,
        "quote_volume": quote_volume,
        "price_change_pct": pcp,
        "range_pct": range_pct,
    }


def parse_spot_trades(rows: Any) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    if not isinstance(rows, list):
        return out
    for it in rows:
        if not isinstance(it, dict):
            continue
        ts = it.get("time")
        try:
            ts = int(ts) if ts is not None else None
        except Exception:
            ts = None
        out.append({
            "id": it.get("id"),
            "price": safe_float(it.get("price")),
            "qty": safe_float(it.get("qty")),
            "quoteQty": safe_float(it.get("quoteQty")),
            "time_ms": ts,
            "time": ms_to_iso_utc(ts) if isinstance(ts, int) else None,
            "isBuyerMaker": bool(it.get("isBuyerMaker")) if "isBuyerMaker" in it else None,
            "isBestMatch": bool(it.get("isBestMatch")) if "isBestMatch" in it else None,
        })
    return out


def parse_agg_trades(rows: Any) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    if not isinstance(rows, list):
        return out
    for it in rows:
        if not isinstance(it, dict):
            continue
        ts = it.get("T")
        try:
            ts = int(ts) if ts is not None else None
        except Exception:
            ts = None
        out.append({
            "a": it.get("a"),
            "price": safe_float(it.get("p")),
            "qty": safe_float(it.get("q")),
            "time_ms": ts,
            "time": ms_to_iso_utc(ts) if isinstance(ts, int) else None,
            "m": bool(it.get("m")) if "m" in it else None,
        })
    return out


def trade_flow_features(recent: List[Dict[str, Any]], agg: List[Dict[str, Any]]) -> Dict[str, Optional[float]]:
    buy_qty = 0.0
    sell_qty = 0.0
    buy_notional = 0.0
    sell_notional = 0.0
    recent_cnt = 0
    agg_cnt = 0
    last_ts = None

    for t in recent:
        q = safe_float(t.get("qty"))
        p = safe_float(t.get("price"))
        is_bm = t.get("isBuyerMaker")
        if q is None or p is None or is_bm is None:
            continue
        recent_cnt += 1
        n = q * p
        # isBuyerMaker=True => seller aggressor; False => buyer aggressor
        if is_bm:
            sell_qty += q
            sell_notional += n
        else:
            buy_qty += q
            buy_notional += n
        ts = t.get("time_ms")
        if isinstance(ts, int) and (last_ts is None or ts > last_ts):
            last_ts = ts

    for t in agg:
        q = safe_float(t.get("qty"))
        p = safe_float(t.get("price"))
        m = t.get("m")
        if q is None or p is None or m is None:
            continue
        agg_cnt += 1
        n = q * p
        if m:
            sell_qty += q
            sell_notional += n
        else:
            buy_qty += q
            buy_notional += n
        ts = t.get("time_ms")
        if isinstance(ts, int) and (last_ts is None or ts > last_ts):
            last_ts = ts

    denom_qty = buy_qty + sell_qty
    denom_notional = buy_notional + sell_notional
    bs_ratio = (buy_qty / sell_qty) if sell_qty > 0 else (None if buy_qty == 0 else 99.0)
    imbalance_qty = ((buy_qty - sell_qty) / denom_qty) if denom_qty > 0 else None
    imbalance_notional = ((buy_notional - sell_notional) / denom_notional) if denom_notional > 0 else None

    return {
        "recent_count": float(recent_cnt),
        "agg_count": float(agg_cnt),
        "buy_qty": buy_qty if denom_qty > 0 else None,
        "sell_qty": sell_qty if denom_qty > 0 else None,
        "buy_notional": buy_notional if denom_notional > 0 else None,
        "sell_notional": sell_notional if denom_notional > 0 else None,
        "buy_sell_ratio": bs_ratio,
        "imbalance_qty": imbalance_qty,
        "imbalance_notional": imbalance_notional,
        "last_trade_time_ms": float(last_ts) if isinstance(last_ts, int) else None,
        "last_trade_time": ms_to_iso_utc(last_ts) if isinstance(last_ts, int) else None,
    }


def book_ticker_features(book_ticker: Any) -> Dict[str, Optional[float]]:
    if not isinstance(book_ticker, dict):
        return {"bidPrice": None, "askPrice": None, "bidQty": None, "askQty": None, "spread_bps": None, "imbalance_top": None}
    bid_p = safe_float(book_ticker.get("bidPrice"))
    ask_p = safe_float(book_ticker.get("askPrice"))
    bid_q = safe_float(book_ticker.get("bidQty"))
    ask_q = safe_float(book_ticker.get("askQty"))
    spread_bps = None
    if bid_p is not None and ask_p is not None and bid_p > 0 and ask_p > 0:
        mid = (bid_p + ask_p) / 2.0
        if mid > 0:
            spread_bps = ((ask_p - bid_p) / mid) * 10000.0
    denom = (bid_q or 0.0) + (ask_q or 0.0)
    imb = ((bid_q - ask_q) / denom) if denom > 0 else None
    return {
        "bidPrice": bid_p,
        "askPrice": ask_p,
        "bidQty": bid_q,
        "askQty": ask_q,
        "spread_bps": spread_bps,
        "imbalance_top": imb,
    }


def build_agent_pack(snapshot: Dict[str, Any], json_path: str, agent_path: str) -> str:
    meta = snapshot.get("meta", {}) or {}
    errors = snapshot.get("errors", {}) or {}

    def get(path: str, default=None):
        cur: Any = snapshot
        for part in path.split("."):
            if isinstance(cur, dict) and part in cur:
                cur = cur[part]
            else:
                return default
        return cur

    def fmt(x: Any) -> str:
        if x is None:
            return "NULL"
        try:
            return f"{float(x):.8f}".rstrip("0").rstrip(".")
        except Exception:
            return str(x)

    intervals = meta.get("intervals", [])
    base_iv = get("features.base_interval")
    by_iv = get("features.by_interval", {}) or {}
    by_iv = by_iv if isinstance(by_iv, dict) else {}
    f_5m = by_iv.get("5m", {})
    f_15m = by_iv.get("15m", {})
    f_30m = by_iv.get("30m", {})
    f_1h = by_iv.get("1h", {})

    key_metrics = {
        "markPrice": safe_float(get("market.premium_index.markPrice")),
        "lastPrice": safe_float(get("market.ticker_24h.lastPrice")),
        "book_bid": safe_float(get("market.book_ticker.bidPrice")),
        "book_ask": safe_float(get("market.book_ticker.askPrice")),
        "book_spread_bps": safe_float(get("features.book_ticker.spread_bps")),
        "avg_price_5m": safe_float(get("market.avg_price.price")),
        "ret_1h_pct": safe_float(get("features.ticker_window.h1.price_change_pct")),
        "ret_4h_pct": safe_float(get("features.ticker_window.h4.price_change_pct")),
        "range_1h_pct": safe_float(get("features.ticker_window.h1.range_pct")),
        "range_4h_pct": safe_float(get("features.ticker_window.h4.range_pct")),
        "trade_recent_count": safe_float(get("flow.trade_features.recent_count")),
        "trade_agg_count": safe_float(get("flow.trade_features.agg_count")),
        "trade_buy_sell_ratio": safe_float(get("flow.trade_features.buy_sell_ratio")),
        "trade_imbalance_qty": safe_float(get("flow.trade_features.imbalance_qty")),
        "base_interval": base_iv,
        "last_close": get("features.last_close"),
        "ret_1_candle_pct": get("features.ret_1_candle_pct"),
        "rsi14_last": get("features.rsi14_last"),
        "atr14_last": get("features.atr14_last"),
        "ema20_last": get("features.ema20_last"),
        "ema50_last": get("features.ema50_last"),
        "funding_rate": get("funding.current_rate"),
        "funding_annualized_pct": get("funding.annualized_pct"),
        "oi_change_from_prev_pct": get("open_interest.change_from_prev_pct"),
        "spread_bps": get("features.order_book.spread_bps"),
        "imbalance": get("features.order_book.imbalance"),

        "rsi14_5m_last": f_5m.get("rsi14_last"),
        "atr14_5m_last": f_5m.get("atr14_last"),
        "ema20_5m_last": f_5m.get("ema20_last"),
        "ema50_5m_last": f_5m.get("ema50_last"),
        "ret_1_candle_5m_pct": f_5m.get("ret_1_candle_pct"),

        "rsi14_15m_last": f_15m.get("rsi14_last"),
        "atr14_15m_last": f_15m.get("atr14_last"),
        "ema20_15m_last": f_15m.get("ema20_last"),
        "ema50_15m_last": f_15m.get("ema50_last"),
        "ret_1_candle_15m_pct": f_15m.get("ret_1_candle_pct"),

        "rsi14_30m_last": f_30m.get("rsi14_last"),
        "atr14_30m_last": f_30m.get("atr14_last"),
        "ema20_30m_last": f_30m.get("ema20_last"),
        "ema50_30m_last": f_30m.get("ema50_last"),
        "ret_1_candle_30m_pct": f_30m.get("ret_1_candle_pct"),

        "rsi14_1h_last": f_1h.get("rsi14_last"),
        "atr14_1h_last": f_1h.get("atr14_last"),
        "ema20_1h_last": f_1h.get("ema20_last"),
        "ema50_1h_last": f_1h.get("ema50_last"),
        "ret_1_candle_1h_pct": f_1h.get("ret_1_candle_pct"),
    }

    def L(path: str) -> int:
        v = get(path, [])
        return len(v) if isinstance(v, list) else 0

    market_type = str(meta.get("market_type") or "").lower()
    is_spot = market_type == "spot"

    K = {}
    prem = get("market.premium_index", {}) or {}
    if is_spot:
        K["K1_price"] = (get("market.ticker_24h.lastPrice") is not None) or (get("market.premium_index.markPrice") is not None)
    else:
        K["K1_premiumIndex"] = isinstance(prem, dict) and (("markPrice" in prem) or ("lastFundingRate" in prem))
        K["K2_lastPrice"] = get("market.ticker_24h.lastPrice") is not None
    bids = get("order_book.bids", []) or []
    asks = get("order_book.asks", []) or []
    K["K3_orderbook"] = (isinstance(bids, list) and len(bids) > 0 and isinstance(asks, list) and len(asks) > 0
                         and get("features.order_book.spread_bps") is not None and get("features.order_book.imbalance") is not None)
    has_5m = "5m" in intervals
    has_15m = "15m" in intervals
    has_30m = "30m" in intervals
    has_1h = "1h" in intervals

    K["K4_candles_5m"] = (not has_5m) or (L("candles.regular.5m") >= 120)
    K["K5_candles_15m"] = (not has_15m) or (L("candles.regular.15m") >= 96)
    if has_30m:
        K["K6_context_30m_or_1h"] = (L("candles.regular.30m") >= 72)
    elif has_1h:
        K["K6_context_30m_or_1h"] = (L("candles.regular.1h") >= 48)
    else:
        K["K6_context_30m_or_1h"] = True

    if is_spot:
        K["K7_mark_context"] = True
    elif has_15m:
        K["K7_mark_context"] = (L("candles.mark.15m") >= 96)
    elif has_30m:
        K["K7_mark_context"] = (L("candles.mark.30m") >= 72)
    elif has_1h:
        K["K7_mark_context"] = (L("candles.mark.1h") >= 48)
    else:
        K["K7_mark_context"] = True
    if is_spot:
        recent_cnt = safe_float(get("flow.trade_features.recent_count")) or 0.0
        agg_cnt = safe_float(get("flow.trade_features.agg_count")) or 0.0
        K["K8_trade_flow"] = (recent_cnt + agg_cnt) >= 80
        K["K9_book_ticker"] = get("features.book_ticker.spread_bps") is not None
        K["K10_ticker_window"] = (get("features.ticker_window.h1.price_change_pct") is not None) and (
            get("features.ticker_window.h4.price_change_pct") is not None
        )
        K["K11_avg_price"] = get("market.avg_price.price") is not None
    else:
        oi_cur = get("open_interest.current", {}) or {}
        K["K8_open_interest"] = (isinstance(oi_cur, dict) and len(oi_cur) > 0) or (get("open_interest.change_from_prev_pct") is not None)

    hist_limit = meta.get("hist_limit", 72) or 72
    need_funding = 80 if hist_limit >= 80 else max(24, int(hist_limit))
    need_taker = 48 if hist_limit >= 48 else max(12, int(hist_limit))
    need_sentiment = 48 if hist_limit >= 48 else max(12, int(hist_limit))
    if not is_spot:
        K["K9_funding_hist"] = (L("funding.history") >= need_funding)
        K["K10_taker_vol"] = (L("flow.taker_buy_sell_volume") >= need_taker)
        K["K11_longshort"] = (L("sentiment.global_long_short_account_ratio") >= need_sentiment)

    missing = [k for k, v in K.items() if not v]
    missing_n = len(missing)
    errors_count = len(errors) if isinstance(errors, dict) else 0
    self_check = get("diagnostics.self_check", {}) or {}
    self_check_status = str(self_check.get("status") or "UNKNOWN")
    self_check_failed_critical = self_check.get("failed_critical", [])
    self_check_failed = self_check.get("failed", [])

    if is_spot:
        if missing_n >= 5:
            dq = "LOW"
        elif 3 <= missing_n <= 4:
            dq = "MED"
        else:
            dq = "HIGH" if errors_count == 0 else "MED"
    else:
        if missing_n >= 4:
            dq = "LOW"
        elif 2 <= missing_n <= 3:
            dq = "MED"
        else:
            dq = "HIGH" if errors_count == 0 else "MED"

    def pick_candles(path: str, n: int):
        arr = get(path, []) or []
        if not isinstance(arr, list):
            return []
        arr = arr[-n:]
        out = []
        for c in arr:
            if not isinstance(c, dict):
                continue
            out.append({"t": c.get("open_time"), "o": c.get("open"), "h": c.get("high"),
                        "l": c.get("low"), "c": c.get("close"), "v": c.get("volume")})
        return out

    c5 = pick_candles("candles.regular.5m", 120)
    c15 = pick_candles("candles.regular.15m", 96)
    c30 = pick_candles("candles.regular.30m", 72)
    c1h = pick_candles("candles.regular.1h", 48)
    ob_top = {"bids_top20": bids[:20] if isinstance(bids, list) else [],
              "asks_top20": asks[:20] if isinstance(asks, list) else []}

    lines = []
    lines.append("AGENT_PACK_VERSION=2")
    lines.append(f"snapshot_id={os.path.basename(json_path)}")
    lines.append(f"symbol={meta.get('symbol')}")
    lines.append(f"generated_at_utc={meta.get('generated_at')}")
    lines.append(f"server_time_utc={meta.get('server_time')}")
    lines.append(f"server_time_ms={meta.get('server_time_ms')}")
    lines.append(f"intervals={intervals}")
    lines.append(f"json_path={os.path.abspath(json_path)}")
    lines.append(f"agent_pack_path={os.path.abspath(agent_path)}")
    lines.append("")
    lines.append(f"data_quality={dq}")
    lines.append(f"errors_count={errors_count}")
    lines.append(f"self_check_status={self_check_status}")
    lines.append(f"self_check_failed_critical={self_check_failed_critical}")
    lines.append(f"self_check_failed={self_check_failed}")
    lines.append(f"missing_modules={missing}")
    lines.append("")
    lines.append("[KEY_METRICS]")
    for k, v in key_metrics.items():
        lines.append(f"{k}={fmt(v)}")
    lines.append("")
    lines.append("[ERRORS_SUMMARY]")
    if isinstance(errors, dict) and errors:
        shown = 0
        for ek, ev in errors.items():
            if shown >= 12:
                break
            lines.append(f"- {ek}: path={ev.get('path')} status_code={ev.get('status_code')} params={ev.get('params', {})}")
            shown += 1
    else:
        lines.append("(none)")
    lines.append("")
    lines.append("[CANDLES_REGULAR_5M_LAST120]")
    lines.append(json.dumps(c5, ensure_ascii=False))
    lines.append("")
    lines.append("[CANDLES_REGULAR_15M_LAST96]")
    lines.append(json.dumps(c15, ensure_ascii=False))
    lines.append("")
    lines.append("[CANDLES_REGULAR_30M_LAST72]")
    lines.append(json.dumps(c30, ensure_ascii=False))
    lines.append("")
    lines.append("[CANDLES_REGULAR_1H_LAST48]")
    lines.append(json.dumps(c1h, ensure_ascii=False))
    lines.append("")
    lines.append("[ORDER_BOOK_TOP20]")
    lines.append(json.dumps(ob_top, ensure_ascii=False))
    lines.append("")
    lines.append("NOTE: 建议分析优先读取本文件；如需更多字段，再读取 json_path 指向的 JSON（建议只读一次）。")
    return "\n".join(lines) + "\n"


def build_summary_txt(snapshot: Dict[str, Any], json_path: str, agent_path: str) -> str:
    meta = snapshot.get("meta", {}) or {}
    feats = snapshot.get("features", {}) or {}
    err_cnt = len(snapshot.get("errors") or {})
    self_check = deep_get(snapshot, "diagnostics.self_check", {}) or {}
    lines = []
    lines.append("【给分析Agent的摘要】")
    lines.append(f"- symbol: {meta.get('symbol')}")
    lines.append(f"- server_time_utc: {meta.get('server_time')} ({meta.get('server_time_ms')})")
    lines.append(f"- generated_at_utc: {meta.get('generated_at')}")
    lines.append(f"- errors_count: {err_cnt}")
    lines.append(f"- self_check_status: {self_check.get('status')}")
    lines.append(f"- self_check_failed_critical: {self_check.get('failed_critical')}")
    lines.append("")
    lines.append("【关键特征】")
    lines.append(f"- base_interval: {feats.get('base_interval')}")
    lines.append(f"- last_close: {feats.get('last_close')}")
    lines.append(f"- ret_1_candle_pct: {feats.get('ret_1_candle_pct')}")
    lines.append(f"- rsi14_last: {feats.get('rsi14_last')}")
    lines.append(f"- atr14_last: {feats.get('atr14_last')}")
    lines.append(f"- ema20_last: {feats.get('ema20_last')}")
    lines.append(f"- ema50_last: {feats.get('ema50_last')}")
    lines.append(f"- spread_bps: {feats.get('order_book', {}).get('spread_bps')}")
    lines.append(f"- imbalance: {feats.get('order_book', {}).get('imbalance')}")
    lines.append(f"- book_spread_bps: {feats.get('book_ticker', {}).get('spread_bps')}")
    lines.append(f"- trade_buy_sell_ratio: {feats.get('trade_flow', {}).get('buy_sell_ratio')}")
    lines.append(f"- trade_imbalance_qty: {feats.get('trade_flow', {}).get('imbalance_qty')}")
    lines.append(f"- ret_1h_pct: {feats.get('ticker_window', {}).get('h1', {}).get('price_change_pct')}")
    lines.append(f"- ret_4h_pct: {feats.get('ticker_window', {}).get('h4', {}).get('price_change_pct')}")
    by_iv = feats.get("by_interval", {}) if isinstance(feats.get("by_interval"), dict) else {}
    for iv in ("5m", "15m", "30m", "1h"):
        ivf = by_iv.get(iv, {}) if isinstance(by_iv.get(iv), dict) else {}
        if ivf:
            lines.append(
                f"- {iv}_view: ret_1={ivf.get('ret_1_candle_pct')} rsi14={ivf.get('rsi14_last')} "
                f"ema20={ivf.get('ema20_last')} ema50={ivf.get('ema50_last')}"
            )
    lines.append("")
    lines.append(f"JSON_FILE: {os.path.abspath(json_path)}")
    lines.append(f"AGENT_PACK: {os.path.abspath(agent_path)}")
    return "\n".join(lines) + "\n"


def main() -> int:
    ap = argparse.ArgumentParser(description="Binance.US spot snapshot fetcher (public endpoints)")
    ap.add_argument("--symbol", default=SYMBOL_DEFAULT, help="交易对，例如 DOGEUSDT")
    ap.add_argument("--base", default=BASE_DEFAULT, help="base url，默认 https://api.binance.us")

    ap.add_argument(
        "--intervals",
        nargs="+",
        default=[INTERVALS_DEFAULT],
        help='K线 interval 列表：--intervals 1m 3m 5m 15m 30m 1h 或 --intervals "1m,3m,5m,15m,30m,1h"'
    )
    ap.add_argument("--kline-limit", type=int, default=KLINE_LIMIT_DEFAULT)
    ap.add_argument("--hist-period", default=HIST_PERIOD_DEFAULT)
    ap.add_argument("--hist-limit", type=int, default=HIST_LIMIT_DEFAULT)
    ap.add_argument("--depth", type=int, default=DEPTH_LEVELS_DEFAULT)
    ap.add_argument("--trades-limit", type=int, default=TRADES_LIMIT_DEFAULT)
    ap.add_argument("--include-trades", action="store_true")
    ap.add_argument("--include-exchange-info", action="store_true")
    ap.add_argument("--out", default="")
    ap.add_argument("--no-text", action="store_true")
    ap.add_argument("--strict", action="store_true")

    args = ap.parse_args()

    symbol = (args.symbol or "").strip().upper()
    if not symbol:
        print("ERROR: --symbol 不能为空", file=sys.stderr)
        return 2

    base_url = (args.base or "").strip().rstrip("/")
    if not base_url.startswith("http"):
        print("ERROR: --base 必须是 http(s) URL", file=sys.stderr)
        return 2

    try:
        intervals = validate_intervals(normalize_intervals_arg(args.intervals))
        hist_period = validate_hist_period(args.hist_period)
    except ValueError as ve:
        print(f"ERROR: {ve}", file=sys.stderr)
        return 2

    kline_limit = clamp_int(int(args.kline_limit), 1, 1500)
    hist_limit = clamp_int(int(args.hist_limit), 1, 500)
    depth_levels = clamp_int(int(args.depth), 1, 1000)
    trades_limit = clamp_int(int(args.trades_limit), 1, 1000)

    base_interval = pick_base_interval(intervals)
    use_binance_us_spot = is_binance_us_spot(base_url)
    if not use_binance_us_spot:
        print("ERROR: 当前版本仅支持 Binance.US API，请使用 --base https://api.binance.us", file=sys.stderr)
        return 2

    path_time = "/api/v3/time"
    path_ticker_24h = "/api/v3/ticker/24hr"
    path_ticker_window = "/api/v3/ticker"
    path_book_ticker = "/api/v3/ticker/bookTicker"
    path_avg_price = "/api/v3/avgPrice"
    path_depth = "/api/v3/depth"
    path_klines_regular = "/api/v3/klines"
    path_recent_trades = "/api/v3/trades"
    path_agg_trades = "/api/v3/aggTrades"
    path_exchange_info = "/api/v3/exchangeInfo"

    session = make_session()
    errors: Dict[str, Any] = {}
    critical_failures: List[str] = []
    hist_effective: Dict[str, Any] = {}

    def fetch(name: str, path: str, params: Optional[Dict[str, Any]] = None, critical: bool = False) -> Optional[Any]:
        data, err = http_get_json(session, base_url, path, params=params)
        if err:
            record_error(errors, name, err)
            if critical:
                critical_failures.append(name)
            return None
        return data

    def fetch_taker_with_endpoint_fallback(period: str, limit: int, expected_min_points: int) -> Optional[List[Dict[str, Any]]]:
        """
        兼容保留：Binance.US spot 无 taker ratio，当前版本默认不调用本函数。
        """
        used_path = None
        used_period = period
        used_limit = limit
        fallback_used = False

        def _normalize_list(raw: Any) -> List[Dict[str, Any]]:
            out: List[Dict[str, Any]] = []
            if not isinstance(raw, list):
                return out
            for it in raw:
                if not isinstance(it, dict):
                    continue
                ts = it.get("timestamp")
                try:
                    ts = int(ts) if ts is not None else None
                except Exception:
                    ts = None
                out.append({
                    "timestamp_ms": ts,
                    "timestamp": ms_to_iso_utc(ts) if isinstance(ts, int) else None,
                    "buyVol": safe_float(it.get("buyVol")),
                    "sellVol": safe_float(it.get("sellVol")),
                    "buySellRatio": safe_float(it.get("buySellRatio")),
                })
            return out

        # 1) 端点选择回退（遇 404 才换）
        last_err = None
        raw = None
        for pth in TAKER_ENDPOINTS:
            data, err = http_get_json(session, base_url, pth, params={"symbol": symbol, "period": period, "limit": limit})
            if err is None:
                raw = data
                used_path = pth
                break
            last_err = err
            if err.get("status_code") == 404:
                continue
            else:
                # 非 404：不再换端点（例如 400/418/429/5xx）
                break

        if used_path is None:
            # 两个都失败才记 errors（这样不会“明明已回退成功还算错误”）
            record_error(errors, "taker_buy_sell_volume", last_err or {"path": "taker", "params": {}, "status_code": None, "error_text": "unknown"})
            critical_failures.append("taker_buy_sell_volume")
            hist_effective["taker_buy_sell_volume"] = {
                "used_path": None, "requested_period": period, "requested_limit": limit,
                "used_period": None, "used_limit": None, "fallback_used": False, "points": None
            }
            return None

        norm = _normalize_list(raw)

        # 2) 点数不足 -> period fallback（同一端点下尝试更细 period 拉更多点）
        if isinstance(norm, list) and len(norm) < expected_min_points:
            for fb in TAKER_FALLBACK_PERIODS:
                if fb == period:
                    continue
                fb_limit = clamp_int(limit * (2 if fb == "30m" else 4 if fb == "15m" else 12), 1, 500)
                data2, err2 = http_get_json(session, base_url, used_path, params={"symbol": symbol, "period": fb, "limit": fb_limit})
                if err2 is None:
                    norm2 = _normalize_list(data2)
                    if len(norm2) > len(norm):
                        norm = norm2
                        used_period = fb
                        used_limit = fb_limit
                        fallback_used = True
                        if len(norm) >= expected_min_points:
                            break

        hist_effective["taker_buy_sell_volume"] = {
            "used_path": used_path,
            "requested_period": period,
            "requested_limit": limit,
            "used_period": used_period,
            "used_limit": used_limit,
            "fallback_used": fallback_used,
            "points": len(norm) if isinstance(norm, list) else None,
        }
        return norm

    # /time
    server_time_ms = None
    t = fetch("time", path_time)
    if isinstance(t, dict) and "serverTime" in t:
        try:
            server_time_ms = int(t["serverTime"])
        except Exception:
            server_time_ms = None
    if server_time_ms is None:
        server_time_ms = int(time.time() * 1000)
    server_time_iso = ms_to_iso_utc(server_time_ms)

    if args.out:
        out_json = args.out
    else:
        out_json = f"snapshot_{symbol}_{server_time_ms}.json"
    out_json = os.path.abspath(out_json)
    base_no_ext = out_json[:-5] if out_json.lower().endswith(".json") else out_json
    out_txt = base_no_ext + ".txt"
    agent_txt = base_no_ext + "_agent.txt"

    ticker_24h = fetch("ticker_24h", path_ticker_24h, {"symbol": symbol}, critical=True)
    ticker_1h = fetch("ticker_1h", path_ticker_window, {"symbol": symbol, "windowSize": "1h"})
    ticker_4h = fetch("ticker_4h", path_ticker_window, {"symbol": symbol, "windowSize": "4h"})
    book_ticker = fetch("book_ticker", path_book_ticker, {"symbol": symbol}, critical=True)
    avg_price = fetch("avg_price", path_avg_price, {"symbol": symbol})
    premium_index = {}
    open_interest_cur = {}
    funding_hist: Any = []

    depth_req_limit = clamp_int(depth_levels, 5, 5000)
    depth_raw = fetch("depth", path_depth, {"symbol": symbol, "limit": depth_req_limit}, critical=True)

    candles_regular: Dict[str, List[Dict[str, Any]]] = {}
    candles_mark: Dict[str, List[Dict[str, Any]]] = {}
    candles_index: Dict[str, List[Dict[str, Any]]] = {}
    candles_premium: Dict[str, List[Dict[str, Any]]] = {}

    for iv in intervals:
        raw = fetch(f"klines_regular_{iv}", path_klines_regular, {"symbol": symbol, "interval": iv, "limit": kline_limit},
                    critical=(iv == base_interval))
        candles_regular[iv] = parse_kline_rows(raw) if raw is not None else []

        candles_mark[iv] = []
        candles_index[iv] = []
        candles_premium[iv] = []

    if not candles_regular.get(base_interval):
        critical_failures.append(f"klines_regular_{base_interval}(empty)")
    oi_hist = []
    global_ls = []
    top_ls_acc = []
    top_ls_pos = []
    taker_bsv_norm = []
    basis = []

    flow_limit = clamp_int(trades_limit, 50, 1000)
    raw_recent_trades = fetch("recent_trades", path_recent_trades, {"symbol": symbol, "limit": flow_limit})
    raw_agg_trades = fetch("agg_trades", path_agg_trades, {"symbol": symbol, "limit": flow_limit})

    recent_trades_out = parse_spot_trades(raw_recent_trades)
    agg_trades_out = parse_agg_trades(raw_agg_trades)
    trade_flow_out = trade_flow_features(recent_trades_out, agg_trades_out)

    trades_out: Any = []
    if args.include_trades:
        trades_out = {"recent": recent_trades_out, "agg": agg_trades_out}

    exchange_info_out: Any = {}
    if args.include_exchange_info:
        cache_path = os.path.join(os.path.dirname(out_json) or ".", "exchangeInfo_cache_binance_us_spot.json")
        cached = load_exchange_info_cache(cache_path, 86400)
        if cached is not None:
            exchange_info_out = cached
        else:
            ex = fetch("exchange_info", path_exchange_info)
            if ex is not None:
                exchange_info_out = ex
                save_exchange_info_cache(cache_path, ex)
            else:
                exchange_info_out = {}

    funding_rate = None
    next_funding_time = None
    if isinstance(premium_index, dict):
        funding_rate = safe_float(premium_index.get("lastFundingRate"))
        nft = premium_index.get("nextFundingTime")
        try:
            next_funding_time = int(nft) if nft is not None else None
        except Exception:
            next_funding_time = None
    funding_annualized_pct = (funding_rate * 3.0 * 365.0 * 100.0) if funding_rate is not None else None

    oi_change_from_prev_pct = None
    if isinstance(oi_hist, list) and len(oi_hist) >= 2:
        def _pick_oi(d):
            """兼容 openInterestHist 不同字段名：openInterest / sumOpenInterest / sumOpenInterestValue"""
            if not isinstance(d, dict):
                return None
            for k in ("openInterest", "sumOpenInterest", "sumOpenInterestValue"):
                v = safe_float(d.get(k))
                if v is not None:
                    return v
            return None

        last_oi = _pick_oi(oi_hist[-1])
        prev_oi = _pick_oi(oi_hist[-2])

        if last_oi is not None and prev_oi is not None and prev_oi != 0:
            oi_change_from_prev_pct = (last_oi - prev_oi) / prev_oi * 100.0

    order_book_out = {"lastUpdateId": None, "bids": [], "asks": [], "features": {"spread_bps": None, "imbalance": None}}
    if isinstance(depth_raw, dict):
        order_book_out["lastUpdateId"] = depth_raw.get("lastUpdateId")
        bids = depth_raw.get("bids") if isinstance(depth_raw.get("bids"), list) else []
        asks = depth_raw.get("asks") if isinstance(depth_raw.get("asks"), list) else []
        bids_s = bids[:depth_levels]
        asks_s = asks[:depth_levels]
        order_book_out["bids"] = bids_s
        order_book_out["asks"] = asks_s
        order_book_out["features"] = order_book_features(bids_s, asks_s)

    book_ticker_out = book_ticker if isinstance(book_ticker, dict) else {}
    book_ticker_feat = book_ticker_features(book_ticker_out)

    depth_feat = order_book_out.get("features") or {}
    merged_spread_bps = depth_feat.get("spread_bps")
    merged_imbalance = depth_feat.get("imbalance")
    if merged_spread_bps is None:
        merged_spread_bps = book_ticker_feat.get("spread_bps")
    if merged_imbalance is None:
        merged_imbalance = book_ticker_feat.get("imbalance_top")

    ticker_1h_feat = ticker_window_features(ticker_1h)
    ticker_4h_feat = ticker_window_features(ticker_4h)

    base_c = candles_regular.get(base_interval, [])
    f_base = compute_candle_features(base_c)
    features_by_interval: Dict[str, Dict[str, Any]] = {}
    for iv in ("1m", "3m", "5m", "15m", "30m", "1h", "4h", "1d"):
        c_iv = candles_regular.get(iv, [])
        if c_iv:
            features_by_interval[iv] = compute_candle_features(c_iv)
    if base_interval not in features_by_interval:
        features_by_interval[base_interval] = f_base

    market_type = "spot"
    source_name = "binance_us_spot"

    snapshot: Dict[str, Any] = {
        "meta": {
            "source": source_name,
            "schema_version": "binance_us_spot_snapshot_v2_1",
            "market_type": market_type,
            "base_url": base_url,
            "symbol": symbol,
            "generated_at": utc_now_iso(),
            "server_time_ms": server_time_ms,
            "server_time": server_time_iso,
            "hist_period": hist_period,
            "hist_limit": hist_limit,
            "kline_limit": kline_limit,
            "intervals": intervals,
            "hist_effective": hist_effective,
        },
        "errors": errors,
        "market": {
            "ticker_24h": ticker_24h if isinstance(ticker_24h, dict) else {},
            "ticker_1h": ticker_1h if isinstance(ticker_1h, dict) else {},
            "ticker_4h": ticker_4h if isinstance(ticker_4h, dict) else {},
            "book_ticker": book_ticker_out,
            "avg_price": avg_price if isinstance(avg_price, dict) else {},
            "premium_index": premium_index if isinstance(premium_index, dict) else {},
        },
        "funding": {
            "current_rate": funding_rate,
            "annualized_pct": funding_annualized_pct,
            "next_funding_time_ms": next_funding_time,
            "next_funding_time": ms_to_iso_utc(next_funding_time) if isinstance(next_funding_time, int) else None,
            "history": funding_hist if isinstance(funding_hist, list) else [],
        },
        "open_interest": {
            "current": open_interest_cur if isinstance(open_interest_cur, dict) else {},
            "history": oi_hist if isinstance(oi_hist, list) else [],
            "change_from_prev_pct": oi_change_from_prev_pct,
        },
        "sentiment": {
            "global_long_short_account_ratio": global_ls if isinstance(global_ls, list) else [],
            "top_long_short_account_ratio": top_ls_acc if isinstance(top_ls_acc, list) else [],
            "top_long_short_position_ratio": top_ls_pos if isinstance(top_ls_pos, list) else [],
        },
        "flow": {
            "taker_buy_sell_volume": taker_bsv_norm if isinstance(taker_bsv_norm, list) else [],
            "recent_trades": recent_trades_out,
            "agg_trades": agg_trades_out,
            "trade_features": trade_flow_out,
        },
        "basis": basis if isinstance(basis, list) else [],
        "order_book": order_book_out,
        "candles": {
            "regular": {iv: candles_regular.get(iv, []) for iv in intervals},
            "mark": {iv: candles_mark.get(iv, []) for iv in intervals},
            "index": {iv: candles_index.get(iv, []) for iv in intervals},
            "premium": {iv: candles_premium.get(iv, []) for iv in intervals},
        },
        "features": {
            "base_interval": base_interval,
            "last_close": f_base.get("last_close"),
            "ret_1_candle_pct": f_base.get("ret_1_candle_pct"),
            "rsi14_last": f_base.get("rsi14_last"),
            "atr14_last": f_base.get("atr14_last"),
            "ema20_last": f_base.get("ema20_last"),
            "ema50_last": f_base.get("ema50_last"),
            "oi_hist_change_pct": oi_change_from_prev_pct,
            "funding_rate": funding_rate,
            "funding_annualized_pct": funding_annualized_pct,
            "order_book": {
                "spread_bps": merged_spread_bps,
                "imbalance": merged_imbalance,
            },
            "book_ticker": book_ticker_feat,
            "trade_flow": trade_flow_out,
            "ticker_window": {
                "h1": ticker_1h_feat,
                "h4": ticker_4h_feat,
            },
            "by_interval": features_by_interval,
        },
        "exchange_info": exchange_info_out if args.include_exchange_info else {},
        "trades": trades_out,
    }
    snapshot["diagnostics"] = {
        "generated_at_utc": utc_now_iso(),
        "error_diagnostics": build_error_diagnostics(errors),
    }
    snapshot["diagnostics"]["self_check"] = run_spot_self_check(snapshot)

    os.makedirs(os.path.dirname(out_json) or ".", exist_ok=True)
    with open(out_json, "w", encoding="utf-8") as f:
        json.dump(snapshot, f, ensure_ascii=False, indent=2)

    # ✅ PowerShell 5.1 友好：txt 输出用 utf-8-sig（带 BOM），Get-Content 默认也能正确识别
    if not args.no_text:
        agent_pack = build_agent_pack(snapshot, out_json, agent_txt)
        with open(agent_txt, "w", encoding="utf-8-sig") as f:
            f.write(agent_pack)
        summary_txt = build_summary_txt(snapshot, out_json, agent_txt)
        with open(out_txt, "w", encoding="utf-8-sig") as f:
            f.write(summary_txt)

    strict_self_check = deep_get(snapshot, "diagnostics.self_check", {}) or {}
    strict_self_check_fail = str(strict_self_check.get("status") or "").upper() == "FAIL"
    if args.strict and (critical_failures or strict_self_check_fail):
        print("STRICT MODE: critical failures:", file=sys.stderr)
        for k in critical_failures:
            print(f"  - {k}", file=sys.stderr)
        if strict_self_check_fail:
            print("STRICT MODE: self_check failed critical checks:", file=sys.stderr)
            for item in strict_self_check.get("failed_critical", []) or []:
                print(f"  - {item}", file=sys.stderr)
        print(f"Snapshot written: {out_json}", file=sys.stderr)
        if not args.no_text:
            print(f"Agent pack written: {agent_txt}", file=sys.stderr)
            print(f"Summary written: {out_txt}", file=sys.stderr)
        return 2

    print(f"OK: snapshot -> {out_json}")
    if not args.no_text:
        print(f"OK: agent_pack -> {agent_txt}")
        print(f"OK: summary -> {out_txt}")
    if errors:
        print(f"WARNING: errors_count={len(errors)} (see snapshot.errors)")
    self_check_status = str(deep_get(snapshot, "diagnostics.self_check.status") or "UNKNOWN")
    if self_check_status in ("WARN", "FAIL"):
        print(f"WARNING: self_check_status={self_check_status} (see snapshot.diagnostics.self_check)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
