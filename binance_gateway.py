"""
binance_gateway.py — 币安数据通用网关模块

提供统一的、带容错机制的币安 API 访问能力。
支持线路热切换（Proxy Pool），当主线路额度耗尽或超时时自动切换。
"""

import os
import time
import requests

# ============ 网络配置 ============
# 线路池：增加备用反代以应对额度耗尽或暂时性屏蔽
PROXY_POOL = [
    "https://bn-proxy-tokyo.vercel.app",   # 线路A (主 - 日本东京)
    "https://binance-proxy.vercel.app"      # 线路B (备 - 仅支持Spot接口)
]
_proxy_idx = 0

USE_TOR = os.environ.get("USE_TOR") == "true"

def get_base_url():
    """动态获取当前的 FAPI/API 线路地址"""
    if USE_TOR:
        return "https://fapi.binance.com", "https://api.binance.com"

    env_fapi = os.environ.get("BINANCE_FAPI_URL")
    env_api  = os.environ.get("BINANCE_API_URL")
    if env_fapi and env_api:
        return env_fapi, env_api

    base = PROXY_POOL[_proxy_idx]
    return base, base

FAPI_BASE, API_BASE = get_base_url()

_DEFAULT_UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"

def create_session():
    s = requests.Session()
    if USE_TOR:
        s.proxies = {"http": "socks5h://127.0.0.1:9050", "https": "socks5h://127.0.0.1:9050"}
    s.headers.update({"User-Agent": _DEFAULT_UA, "Accept-Encoding": "gzip, deflate"})
    return s

_session = create_session()

def fetch_json(url, retries=3, timeout=60, retry_delay=5, **kwargs):
    """
    带线路热切换的通用 JSON 请求。
    """
    global _proxy_idx, FAPI_BASE, API_BASE
    current_url = url

    for attempt in range(1, retries + 1):
        try:
            res = _session.get(current_url, timeout=timeout, **kwargs)
            res.raise_for_status()
            return res.json()
        except Exception as e:
            if attempt < retries and not USE_TOR:
                # 轮换线路 (静默切换)
                old_base = PROXY_POOL[_proxy_idx]
                _proxy_idx = (_proxy_idx + 1) % len(PROXY_POOL)
                new_base = PROXY_POOL[_proxy_idx]
                FAPI_BASE, API_BASE = new_base, new_base
                current_url = current_url.replace(old_base, new_base)
                # print(f"  🧠 [自动容错] 线路故障，已切换至: {new_base}") # 降低噪音，静默切换
                time.sleep(retry_delay)
            else:
                if attempt == retries: return None
    return None

def get_all_usdt_perpetuals():
    url = f"{FAPI_BASE}/fapi/v1/exchangeInfo"
    data = fetch_json(url)
    if not data: return []
    symbols = []
    for s in data.get("symbols", []):
        if (s.get("quoteAsset") == "USDT" and s.get("contractType") == "PERPETUAL" and s.get("status") == "TRADING"):
            symbols.append(s["symbol"])
    return symbols

def fetch_klines(symbol, interval="1h", limit=200):
    """拉取 K 线，支持线路热切换"""
    global _proxy_idx, FAPI_BASE, API_BASE
    for attempt in range(1, 4):
        url = f"{FAPI_BASE}/fapi/v1/klines?symbol={symbol}&interval={interval}&limit={limit}"
        try:
            res = _session.get(url, timeout=20)
            if res.status_code == 200:
                return symbol, res.json()
            elif res.status_code == 404:
                raise Exception("404 Not Found")
            res.raise_for_status()
        except Exception:
            if attempt < 3 and not USE_TOR:
                _proxy_idx = (_proxy_idx + 1) % len(PROXY_POOL)
                FAPI_BASE, API_BASE = PROXY_POOL[_proxy_idx], PROXY_POOL[_proxy_idx]
    return symbol, None

def fetch_funding_rate(symbol=None, limit=1):
    url = f"{FAPI_BASE}/fapi/v1/fundingRate"
    params = {"limit": limit}
    if symbol: params["symbol"] = symbol
    return fetch_json(url, params=params)

def fetch_all_prices():
    url = f"{FAPI_BASE}/fapi/v1/ticker/price"
    data = fetch_json(url)
    if not data: return {}
    return {item['symbol']: float(item['price']) for item in data}

def fetch_open_interest(symbol):
    url = f"{FAPI_BASE}/fapi/v1/openInterest"
    params = {"symbol": symbol}
    data = fetch_json(url, params=params)
    if not data: return 0.0
    return float(data.get('openInterest', 0))

def fetch_oi_history(symbol, period="1h", limit=24):
    url = f"{FAPI_BASE}/futures/data/openInterestHist"
    params = {"symbol": symbol, "period": period, "limit": limit}
    return fetch_json(url, params=params)

def fetch_cmc_data():
    """获取全市场 24h 价格数据，用于提取市值等辅助信息 (从现货接口)"""
    url = f"{API_BASE}/api/v3/ticker/24hr"
    return fetch_json(url)
