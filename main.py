import csv
import json
import math
import os
import time
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import requests


# ============================================================
# 자동매매 실행 엔진 v1.0 (가상매매 전용)
# ROLE: 시장 해석 + 선발대 스캔 + 가상매매 + 텔레그램 리포트
# NOTE:
# - 실거래 연결 없음
# - 업비트 공개 API 기준
# - DOM/확산도는 기본값 placeholder 구조 포함
# - 바로 실행 가능한 뼈대 + 실전 확장용 구조
# ============================================================


# =========================
# 환경설정
# =========================
UPBIT_BASE = "https://api.upbit.com/v1"
COINGECKO_BASE = "https://api.coingecko.com/api/v3"

BOT_TOKEN = "8768443500:AAEjitG2DO9GlC0K0JqRlm_67BsxLoNQda4"
CHAT_ID = "7697037478"

print("DEBUG BOT_TOKEN =", repr(BOT_TOKEN))
print("DEBUG CHAT_ID =", repr(CHAT_ID))

REPORT_EVERY_MIN = 30
DOM_CHECK_MIN = 30
SPREAD_CHECK_MIN = 30
SCAN_CHECK_MIN = 60
LOOP_SECONDS = 180  # 3분

TRADE_LOG_FILE = "trades.csv"
MARKET_LOG_FILE = "market_state_log.csv"
STATE_FILE = "engine_state.json"

ENTRY_MARKET_STATE = {"TRENDING", "RECOVERING"}
MIN_TRADE_VALUE_KRW = 4_000_000_000  # 40억
MIN_STRENGTH = 70.0
MAX_SIMULTANEOUS_POSITIONS = 3
DEFAULT_POSITION_SIZE_KRW = 100_000  # 가상매매 포지션 크기
TAKE_PROFIT_PCT = 0.02              # 2%
STOP_LOSS_PCT = -0.015              # -1.5%
BTC_STRENGTH_BREAKOUT_LEVEL = 100.0
USDT_STRENGTH_ALERT_1 = 140.0
USDT_STRENGTH_ALERT_2 = 200.0

WATCHLIST = [
    "KRW-BTC", "KRW-ETH", "KRW-XRP", "KRW-SOL", "KRW-ADA",
    "KRW-DOGE", "KRW-SUI", "KRW-LINK", "KRW-AVAX", "KRW-TRX",
]

SPREAD_MARKETS = [
    "KRW-BTC", "KRW-ETH", "KRW-XRP", "KRW-SOL", "KRW-ADA",
    "KRW-LINK", "KRW-AVAX", "KRW-SUI", "KRW-DOGE", "KRW-TRX",
    "KRW-HBAR", "KRW-DOT", "KRW-APT", "KRW-NEAR", "KRW-ATOM",
]


# =========================
# 데이터 모델
# =========================
@dataclass
class StrengthSnapshot:
    market: str
    price: float
    change_pct_15m: float
    low_change_pct_8h: float
    volume_1h_krw: float
    trade_value_24h_krw: float
    strength_score: float
    state: str


@dataclass
class DomSnapshot:
    btc_d: float
    eth_d: float
    btc_d_change: float
    eth_d_change: float
    checked_at: str


@dataclass
class SpreadSnapshot:
    score: float
    rising_count: int
    total_count: int
    checked_at: str


@dataclass
class Position:
    market: str
    entry_price: float
    quantity: float
    entry_time: str
    entry_reason: str
    position_size_krw: float
    last_price: float = 0.0
    pnl_pct: float = 0.0
    pnl_krw: float = 0.0


@dataclass
class EngineState:
    open_positions: List[Dict]
    last_report_ts: float
    last_dom_ts: float
    last_spread_ts: float
    last_scan_ts: float
    last_btc_strength_alert: str
    last_usdt_strength_alert: str
    last_market_state: str


# =========================
# 유틸
# =========================
def now_kst_str() -> str:
    return datetime.now(timezone.utc).astimezone().strftime("%Y-%m-%d %H:%M:%S")


def safe_float(value, default=0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except Exception:
        return default


def request_json(url: str, params: Optional[dict] = None, timeout: int = 10):
    resp = requests.get(url, params=params, timeout=timeout)
    resp.raise_for_status()
    return resp.json()


def ensure_csv(path: str, headers: List[str]) -> None:
    if not os.path.exists(path):
        with open(path, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.writer(f)
            writer.writerow(headers)


def append_csv(path: str, row: List):
    with open(path, "a", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        writer.writerow(row)


def send_telegram(message: str) -> None:
    if not BOT_TOKEN or not CHAT_ID:
        print("[TELEGRAM SKIP] BOT_TOKEN / CHAT_ID 미설정")
        print(message)
        return

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": CHAT_ID,
        "text": message,
        "disable_web_page_preview": True,
    }
    try:
        requests.post(url, json=payload, timeout=10)
    except Exception as e:
        print(f"[TELEGRAM ERROR] {e}")


# =========================
# 상태 저장
# =========================
def load_state() -> EngineState:
    if not os.path.exists(STATE_FILE):
        return EngineState(
            open_positions=[],
            last_report_ts=0,
            last_dom_ts=0,
            last_spread_ts=0,
            last_scan_ts=0,
            last_btc_strength_alert="",
            last_usdt_strength_alert="",
            last_market_state="UNKNOWN",
        )

    with open(STATE_FILE, "r", encoding="utf-8") as f:
        raw = json.load(f)
    return EngineState(**raw)


def save_state(state: EngineState) -> None:
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(asdict(state), f, ensure_ascii=False, indent=2)


# =========================
# 업비트 데이터
# =========================
def get_all_krw_markets() -> List[str]:
    markets = request_json(f"{UPBIT_BASE}/market/all", params={"isDetails": "false"})
    return [m["market"] for m in markets if m["market"].startswith("KRW-")]


def get_ticker(markets: List[str]) -> Dict[str, dict]:
    chunks = [markets[i:i + 100] for i in range(0, len(markets), 100)]
    result: Dict[str, dict] = {}
    for chunk in chunks:
        data = request_json(f"{UPBIT_BASE}/ticker", params={"markets": ",".join(chunk)})
        for item in data:
            result[item["market"]] = item
    return result


def get_candles_minutes(market: str, unit: int, count: int) -> List[dict]:
    return request_json(
        f"{UPBIT_BASE}/candles/minutes/{unit}",
        params={"market": market, "count": count},
    )


def get_orderbook_units(market: str) -> dict:
    data = request_json(f"{UPBIT_BASE}/orderbook", params={"markets": market})
    return data[0] if data else {}


# =========================
# 강도 계산
# =========================
def compute_strength_snapshot(market: str, ticker: dict) -> StrengthSnapshot:
    candles_15m = get_candles_minutes(market, 15, 2)
    candles_60m = get_candles_minutes(market, 60, 8)

    current_price = safe_float(ticker.get("trade_price"))
    trade_value_24h = safe_float(ticker.get("acc_trade_price_24h"))

    if len(candles_15m) >= 2:
        current_close = safe_float(candles_15m[0].get("trade_price"))
        prev_close = safe_float(candles_15m[1].get("trade_price"))
        change_pct_15m = ((current_close - prev_close) / prev_close * 100) if prev_close else 0.0
    else:
        change_pct_15m = 0.0

    if candles_60m:
        recent_lows = [safe_float(c.get("low_price")) for c in candles_60m]
        latest_low = recent_lows[0]
        past_min_low = min(recent_lows[1:]) if len(recent_lows) > 1 else latest_low
        low_change_pct_8h = ((latest_low - past_min_low) / past_min_low * 100) if past_min_low else 0.0
        volume_1h_krw = safe_float(candles_60m[0].get("candle_acc_trade_price"))
    else:
        low_change_pct_8h = 0.0
        volume_1h_krw = 0.0

    # 가벼운 실전형 점수 모델 (추후 정교화 가능)
    score = 50.0
    score += max(min(change_pct_15m * 10, 20), -20)
    score += max(min(low_change_pct_8h * 8, 20), -20)
    score += 10 if trade_value_24h >= MIN_TRADE_VALUE_KRW else -15
    score += 10 if volume_1h_krw >= 200_000_000 else 0
    score = max(0.0, min(score, 150.0))

    if score >= 110:
        state = "STRONG"
    elif score >= 90:
        state = "STABLE"
    elif score >= 70:
        state = "RECOVERING"
    elif score >= 50:
        state = "WEAK"
    else:
        state = "COLLAPSE"

    return StrengthSnapshot(
        market=market,
        price=current_price,
        change_pct_15m=round(change_pct_15m, 3),
        low_change_pct_8h=round(low_change_pct_8h, 3),
        volume_1h_krw=round(volume_1h_krw, 2),
        trade_value_24h_krw=round(trade_value_24h, 2),
        strength_score=round(score, 2),
        state=state,
    )


def low_not_lower_8h(market: str) -> bool:
    candles_60m = get_candles_minutes(market, 60, 8)
    if len(candles_60m) < 4:
        return False
    lows = [safe_float(c["low_price"]) for c in candles_60m]
    latest_low = lows[0]
    base_low = min(lows[1:])
    return latest_low >= base_low


def volume_increasing_1h(market: str) -> bool:
    candles = get_candles_minutes(market, 60, 3)
    if len(candles) < 3:
        return False
    current_v = safe_float(candles[0].get("candle_acc_trade_price"))
    prev_v = safe_float(candles[1].get("candle_acc_trade_price"))
    return current_v > prev_v


def strength_rising(snapshot: StrengthSnapshot) -> bool:
    return snapshot.change_pct_15m > 0 and snapshot.low_change_pct_8h >= 0


# =========================
# BTC / USDT / DOM / 확산도
# =========================
def get_btc_snapshot() -> StrengthSnapshot:
    ticker = get_ticker(["KRW-BTC"])["KRW-BTC"]
    return compute_strength_snapshot("KRW-BTC", ticker)


def get_usdt_snapshot() -> StrengthSnapshot:
    ticker = get_ticker(["KRW-USDT"])["KRW-USDT"]
    return compute_strength_snapshot("KRW-USDT", ticker)


def get_dominance_snapshot(prev: Optional[DomSnapshot] = None) -> DomSnapshot:
    data = request_json(
        f"{COINGECKO_BASE}/global",
        params={"x_cg_demo_api_key": os.getenv("COINGECKO_API_KEY", "")},
    )
    dom = data.get("data", {}).get("market_cap_percentage", {})
    btc_d = safe_float(dom.get("btc"))
    eth_d = safe_float(dom.get("eth"))

    prev_btc = prev.btc_d if prev else btc_d
    prev_eth = prev.eth_d if prev else eth_d

    return DomSnapshot(
        btc_d=round(btc_d, 3),
        eth_d=round(eth_d, 3),
        btc_d_change=round(btc_d - prev_btc, 3),
        eth_d_change=round(eth_d - prev_eth, 3),
        checked_at=now_kst_str(),
    )


def get_spread_snapshot() -> SpreadSnapshot:
    tickers = get_ticker(SPREAD_MARKETS)
    rising = 0
    total = 0
    for market in SPREAD_MARKETS:
        ticker = tickers.get(market)
        if not ticker:
            continue
        change_rate = safe_float(ticker.get("signed_change_rate")) * 100
        total += 1
        if change_rate > 0:
            rising += 1

    score = (rising / total * 100) if total else 0.0
    return SpreadSnapshot(
        score=round(score, 2),
        rising_count=rising,
        total_count=total,
        checked_at=now_kst_str(),
    )


# =========================
# 시장 상태 판정
# =========================
def evaluate_market_state(
    btc: StrengthSnapshot,
    usdt: StrengthSnapshot,
    dom: Optional[DomSnapshot],
    spread: Optional[SpreadSnapshot],
) -> Tuple[str, str]:
    spread_score = spread.score if spread else 50.0
    btc_d_change = dom.btc_d_change if dom else 0.0

    if usdt.strength_score >= 120 and btc.strength_score < 60:
        return "CRISIS", "USDT 압력 강하고 BTC 힘 부족"

    if usdt.strength_score >= 100 and btc.strength_score < 75:
        return "RISK", "USDT 우세 / BTC 약세 구간"

    if btc.strength_score >= 90 and spread_score >= 60 and btc_d_change >= -0.2:
        return "TRENDING", "BTC 주도 + 확산 진행"

    if btc.strength_score >= 70 and spread_score >= 45:
        return "RECOVERING", "BTC 회복 + 알트 확산 준비"

    return "NORMAL", "혼조 구간 / 추세 확정 전"


# =========================
# 선발대 스캔
# =========================
def scan_candidates() -> List[StrengthSnapshot]:
    markets = get_all_krw_markets()
    tickers = get_ticker(markets)
    candidates: List[StrengthSnapshot] = []

    for market in markets:
        try:
            ticker = tickers.get(market)
            if not ticker:
                continue

            trade_value_24h = safe_float(ticker.get("acc_trade_price_24h"))
            if trade_value_24h < MIN_TRADE_VALUE_KRW:
                continue

            snapshot = compute_strength_snapshot(market, ticker)
            if snapshot.strength_score < MIN_STRENGTH:
                continue

            if not low_not_lower_8h(market):
                continue

            candidates.append(snapshot)
        except Exception as e:
            print(f"[SCAN ERROR] {market}: {e}")

    candidates.sort(key=lambda x: (x.strength_score, x.trade_value_24h_krw), reverse=True)
    return candidates[:15]


# =========================
# 진입 판단
# =========================
def already_holding(state: EngineState, market: str) -> bool:
    return any(p["market"] == market for p in state.open_positions)


def check_entry_signals(
    candidates: List[StrengthSnapshot],
    market_state: str,
    state: EngineState,
) -> List[Tuple[StrengthSnapshot, str]]:
    if market_state not in ENTRY_MARKET_STATE:
        return []

    if len(state.open_positions) >= MAX_SIMULTANEOUS_POSITIONS:
        return []

    entries: List[Tuple[StrengthSnapshot, str]] = []
    for coin in candidates:
        try:
            if already_holding(state, coin.market):
                continue
            if coin.strength_score < MIN_STRENGTH:
                continue
            if not volume_increasing_1h(coin.market):
                continue
            if not strength_rising(coin):
                continue

            reason = (
                f"선발대 조건 충족 | 강도={coin.strength_score} | "
                f"15m변화={coin.change_pct_15m}% | 8h저점={coin.low_change_pct_8h}%"
            )
            entries.append((coin, reason))

            if len(state.open_positions) + len(entries) >= MAX_SIMULTANEOUS_POSITIONS:
                break
        except Exception as e:
            print(f"[ENTRY CHECK ERROR] {coin.market}: {e}")

    return entries


# =========================
# 가상매매
# =========================
def open_virtual_position(state: EngineState, coin: StrengthSnapshot, reason: str) -> Position:
    quantity = DEFAULT_POSITION_SIZE_KRW / coin.price if coin.price > 0 else 0.0
    pos = Position(
        market=coin.market,
        entry_price=coin.price,
        quantity=quantity,
        entry_time=now_kst_str(),
        entry_reason=reason,
        position_size_krw=DEFAULT_POSITION_SIZE_KRW,
        last_price=coin.price,
    )
    state.open_positions.append(asdict(pos))

    append_csv(
        TRADE_LOG_FILE,
        [
            now_kst_str(), "OPEN", coin.market, coin.price, quantity,
            "", "", 0, 0, reason,
        ],
    )

    send_telegram(
        f"🟢 [가상진입]\n"
        f"종목: {coin.market}\n"
        f"진입가: {coin.price:,.0f}\n"
        f"수량: {quantity:.8f}\n"
        f"사유: {reason}"
    )
    return pos


def close_virtual_position(state: EngineState, market: str, exit_price: float, exit_reason: str) -> None:
    remaining = []
    closed = None
    for p in state.open_positions:
        if p["market"] == market and closed is None:
            closed = p
        else:
            remaining.append(p)

    if not closed:
        return

    entry_price = safe_float(closed["entry_price"])
    qty = safe_float(closed["quantity"])
    pnl_pct = ((exit_price - entry_price) / entry_price * 100) if entry_price else 0.0
    pnl_krw = (exit_price - entry_price) * qty

    state.open_positions = remaining

    append_csv(
        TRADE_LOG_FILE,
        [
            now_kst_str(), "CLOSE", market, entry_price, qty,
            exit_price, round(pnl_pct, 4), round(pnl_krw, 2),
            round(exit_price * qty, 2), exit_reason,
        ],
    )

    emoji = "🔵" if pnl_pct >= 0 else "🔴"
    send_telegram(
        f"{emoji} [가상청산]\n"
        f"종목: {market}\n"
        f"진입가: {entry_price:,.0f}\n"
        f"청산가: {exit_price:,.0f}\n"
        f"손익률: {pnl_pct:.2f}%\n"
        f"손익금액: {pnl_krw:,.0f}원\n"
        f"사유: {exit_reason}"
    )


def manage_positions(state: EngineState, market_state: str, btc: StrengthSnapshot) -> None:
    if not state.open_positions:
        return

    markets = [p["market"] for p in state.open_positions]
    tickers = get_ticker(markets)

    to_close: List[Tuple[str, float, str]] = []

    for p in state.open_positions:
        market = p["market"]
        ticker = tickers.get(market)
        if not ticker:
            continue

        last_price = safe_float(ticker.get("trade_price"))
        entry_price = safe_float(p["entry_price"])
        pnl_pct = ((last_price - entry_price) / entry_price) if entry_price else 0.0

        p["last_price"] = last_price
        p["pnl_pct"] = round(pnl_pct * 100, 4)
        p["pnl_krw"] = round((last_price - entry_price) * safe_float(p["quantity"]), 2)

        if pnl_pct >= TAKE_PROFIT_PCT:
            to_close.append((market, last_price, "목표수익 도달"))
            continue

        if pnl_pct <= STOP_LOSS_PCT:
            to_close.append((market, last_price, "손절 기준 도달"))
            continue

        if market_state in {"RISK", "CRISIS"} and btc.strength_score < 70:
            to_close.append((market, last_price, f"시장 상태 악화: {market_state}"))
            continue

        try:
            coin_snapshot = compute_strength_snapshot(market, ticker)
            if coin_snapshot.strength_score < 70:
                to_close.append((market, last_price, "종목 강도 70 이탈"))
        except Exception as e:
            print(f"[POSITION CHECK ERROR] {market}: {e}")

    for market, price, reason in to_close:
        close_virtual_position(state, market, price, reason)


# =========================
# 이벤트 알림
# =========================
def notify_key_events(state: EngineState, btc: StrengthSnapshot, usdt: StrengthSnapshot) -> None:
    # BTC 100 돌파 / 이탈
    if btc.strength_score >= BTC_STRENGTH_BREAKOUT_LEVEL and state.last_btc_strength_alert != "OVER_100":
        send_telegram(
            f"🔥 [이벤트] BTC 강도 100 돌파\n가격: {btc.price:,.0f}\n강도: {btc.strength_score}"
        )
        state.last_btc_strength_alert = "OVER_100"
    elif btc.strength_score < BTC_STRENGTH_BREAKOUT_LEVEL and state.last_btc_strength_alert != "UNDER_100":
        send_telegram(
            f"⚠️ [이벤트] BTC 강도 100 이탈\n가격: {btc.price:,.0f}\n강도: {btc.strength_score}"
        )
        state.last_btc_strength_alert = "UNDER_100"

    # USDT 140 / 200 돌파
    if usdt.strength_score >= USDT_STRENGTH_ALERT_2 and state.last_usdt_strength_alert != "OVER_200":
        send_telegram(
            f"🚨 [이벤트] USDT 강도 200 돌파\n가격: {usdt.price:,.2f}\n강도: {usdt.strength_score}"
        )
        state.last_usdt_strength_alert = "OVER_200"
    elif usdt.strength_score >= USDT_STRENGTH_ALERT_1 and state.last_usdt_strength_alert not in {"OVER_140", "OVER_200"}:
        send_telegram(
            f"⚠️ [이벤트] USDT 강도 140 돌파\n가격: {usdt.price:,.2f}\n강도: {usdt.strength_score}"
        )
        state.last_usdt_strength_alert = "OVER_140"
    elif usdt.strength_score < USDT_STRENGTH_ALERT_1 and state.last_usdt_strength_alert != "NORMAL":
        state.last_usdt_strength_alert = "NORMAL"


# =========================
# 리포트
# =========================
def format_positions(state: EngineState) -> str:
    if not state.open_positions:
        return "없음"

    lines = []
    for p in state.open_positions:
        lines.append(
            f"- {p['market']} | 진입 {safe_float(p['entry_price']):,.0f} | "
            f"현재 {safe_float(p.get('last_price', 0)):,.0f} | "
            f"손익 {safe_float(p.get('pnl_pct', 0)):.2f}%"
        )
    return "\n".join(lines)


def send_market_report(
    btc: StrengthSnapshot,
    usdt: StrengthSnapshot,
    dom: Optional[DomSnapshot],
    spread: Optional[SpreadSnapshot],
    market_state: str,
    interpretation: str,
    state: EngineState,
) -> None:
    dom_text = (
        f"BTC.D {dom.btc_d}% ({dom.btc_d_change:+.3f}) / ETH.D {dom.eth_d}% ({dom.eth_d_change:+.3f})"
        if dom else "DOM 데이터 없음"
    )
    spread_text = (
        f"{spread.score}% ({spread.rising_count}/{spread.total_count})"
        if spread else "확산도 데이터 없음"
    )

    msg = (
        f"📊 [시장 리포트] {now_kst_str()}\n\n"
        f"BTC: {btc.price:,.0f} | 강도 {btc.strength_score} | 상태 {btc.state}\n"
        f"USDT: {usdt.price:,.2f} | 강도 {usdt.strength_score} | 상태 {usdt.state}\n"
        f"DOM: {dom_text}\n"
        f"확산도: {spread_text}\n"
        f"시장 상태: {market_state}\n"
        f"해석: {interpretation}\n\n"
        f"📦 보유 포지션\n{format_positions(state)}"
    )
    send_telegram(msg)


# =========================
# 로그
# =========================
def log_market_state(
    btc: StrengthSnapshot,
    usdt: StrengthSnapshot,
    dom: Optional[DomSnapshot],
    spread: Optional[SpreadSnapshot],
    market_state: str,
    interpretation: str,
) -> None:
    append_csv(
        MARKET_LOG_FILE,
        [
            now_kst_str(),
            btc.price, btc.strength_score, btc.state,
            usdt.price, usdt.strength_score, usdt.state,
            dom.btc_d if dom else "", dom.btc_d_change if dom else "",
            dom.eth_d if dom else "", dom.eth_d_change if dom else "",
            spread.score if spread else "", spread.rising_count if spread else "",
            spread.total_count if spread else "",
            market_state, interpretation,
        ],
    )


# =========================
# 메인 루프
# =========================
def main():
    ensure_csv(
        TRADE_LOG_FILE,
        [
            "timestamp", "action", "market", "entry_price", "quantity",
            "exit_price", "pnl_pct", "pnl_krw", "position_value", "reason",
        ],
    )
    ensure_csv(
        MARKET_LOG_FILE,
        [
            "timestamp",
            "btc_price", "btc_strength", "btc_state",
            "usdt_price", "usdt_strength", "usdt_state",
            "btc_d", "btc_d_change", "eth_d", "eth_d_change",
            "spread_score", "spread_rising_count", "spread_total_count",
            "market_state", "interpretation",
        ],
    )

    state = load_state()
    prev_dom: Optional[DomSnapshot] = None
    latest_dom: Optional[DomSnapshot] = None
    latest_spread: Optional[SpreadSnapshot] = None
    latest_candidates: List[StrengthSnapshot] = []

    send_telegram("🚀 자동매매 실행 엔진 v1.0 시작 (가상매매 모드)")

    while True:
        try:
            loop_started = time.time()
            now_ts = time.time()

            btc = get_btc_snapshot()
            usdt = get_usdt_snapshot()

            # DOM 30분
            if now_ts - state.last_dom_ts >= DOM_CHECK_MIN * 60 or latest_dom is None:
                latest_dom = get_dominance_snapshot(prev_dom)
                prev_dom = latest_dom
                state.last_dom_ts = now_ts

            # 확산도 30분
            if now_ts - state.last_spread_ts >= SPREAD_CHECK_MIN * 60 or latest_spread is None:
                latest_spread = get_spread_snapshot()
                state.last_spread_ts = now_ts

            market_state, interpretation = evaluate_market_state(btc, usdt, latest_dom, latest_spread)

            # 상태 변화 알림
            if market_state != state.last_market_state:
                send_telegram(
                    f"🧭 [시장상태 변경] {state.last_market_state} → {market_state}\n사유: {interpretation}"
                )
                state.last_market_state = market_state

            notify_key_events(state, btc, usdt)

            # 선발대 스캔 1시간
            if now_ts - state.last_scan_ts >= SCAN_CHECK_MIN * 60 or not latest_candidates:
                latest_candidates = scan_candidates()
                state.last_scan_ts = now_ts
                if latest_candidates:
                    top_text = "\n".join(
                        f"- {c.market} | 강도 {c.strength_score} | 24h거래대금 {c.trade_value_24h_krw:,.0f}"
                        for c in latest_candidates[:5]
                    )
                    send_telegram(f"🔎 [선발대 스캔 상위]\n{top_text}")

            # 진입
            entries = check_entry_signals(latest_candidates, market_state, state)
            for coin, reason in entries:
                open_virtual_position(state, coin, reason)

            # 포지션 관리
            manage_positions(state, market_state, btc)

            # 30분 리포트
            if now_ts - state.last_report_ts >= REPORT_EVERY_MIN * 60:
                send_market_report(btc, usdt, latest_dom, latest_spread, market_state, interpretation, state)
                state.last_report_ts = now_ts

            # 로그 기록
            log_market_state(btc, usdt, latest_dom, latest_spread, market_state, interpretation)
            save_state(state)

            elapsed = time.time() - loop_started
            sleep_seconds = max(5, LOOP_SECONDS - int(elapsed))
            print(f"[{now_kst_str()}] loop done | market_state={market_state} | sleep={sleep_seconds}s")
            time.sleep(sleep_seconds)

        except KeyboardInterrupt:
            send_telegram("🛑 자동매매 실행 엔진 수동 중지")
            save_state(state)
            raise
        except Exception as e:
            err = f"❌ [ENGINE ERROR] {type(e).__name__}: {e}"
            print(err)
            send_telegram(err)
            save_state(state)
            time.sleep(15)


if __name__ == "__main__":
    main()

