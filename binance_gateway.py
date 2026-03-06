"""
binance_gateway.py — 币安数据通用网关模块

提供统一的、带有 Tor 穿透和容错机制的币安 API 访问能力。
所有需要币安数据的脚本只需:
    from binance_gateway import create_session, fetch_json, get_all_usdt_perpetuals, fetch_klines
"""

import os
import time
import requests

# ============ 网络配置 ============
USE_TOR = os.environ.get("USE_TOR") == "true"
# Tor 模式下直连官方；否则走用户自定义的反代节点
FAPI_BASE = "https://fapi.binance.com" if USE_TOR else os.environ.get("BINANCE_FAPI_URL", "https://binance.794988.xyz")
API_BASE  = "https://api.binance.com"  if USE_TOR else os.environ.get("BINANCE_API_URL",  "https://binance.794988.xyz")

_DEFAULT_UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"


def create_session():
    """
    创建一个预配置的 requests.Session。
    - 如果环境变量 USE_TOR=true，自动挂载 socks5h 代理。
    - 全局注入伪装 User-Agent。
    """
    s = requests.Session()
    if USE_TOR:
        s.proxies = {
            "http":  "socks5h://127.0.0.1:9050",
            "https": "socks5h://127.0.0.1:9050",
        }
    s.headers.update({"User-Agent": _DEFAULT_UA})
    return s


# 全局唯一 Session（模块级单例）
_session = create_session()


def fetch_json(url, retries=3, timeout=60, retry_delay=15):
    """
    带重试的通用 JSON 请求。
    :param url: 完整 URL
    :param retries: 最大重试次数
    :param timeout: 单次请求超时（秒）
    :param retry_delay: 重试间隔（秒）
    :return: 解析后的 dict/list，失败返回 None
    """
    for attempt in range(1, retries + 1):
        try:
            res = _session.get(url, timeout=timeout)
            res.raise_for_status()
            return res.json()
        except Exception as e:
            print(f"  [尝试 {attempt}/{retries}] 请求失败: {e}")
            if attempt < retries:
                print(f"  等待 {retry_delay}s 后重试...")
                time.sleep(retry_delay)
    return None


def get_all_usdt_perpetuals():
    """
    获取币安全部 USDT 本位永续合约交易对名称。
    :return: symbol 字符串列表，如 ['BTCUSDT', 'ETHUSDT', ...]
    """
    url = f"{FAPI_BASE}/fapi/v1/exchangeInfo"
    data = fetch_json(url)
    if not data:
        return []
    symbols = []
    for s in data.get("symbols", []):
        if (s.get("quoteAsset") == "USDT"
                and s.get("contractType") == "PERPETUAL"
                and s.get("status") == "TRADING"):
            symbols.append(s["symbol"])
    return symbols


def fetch_klines(symbol, interval="1h", limit=200):
    """
    拉取单个交易对的 K 线数据。
    :return: (symbol, klines_list) 或 (symbol, None)
    """
    url = f"{FAPI_BASE}/fapi/v1/klines?symbol={symbol}&interval={interval}&limit={limit}"
    try:
        res = _session.get(url, timeout=30)
        if res.status_code == 200:
            return symbol, res.json()
    except Exception:
        pass
    return symbol, None


def fetch_funding_rate(symbol=None, limit=1):
    """
    拉取资金费率（预留接口，供未来资金费率监控使用）。
    """
    url = f"{FAPI_BASE}/fapi/v1/fundingRate"
    params = {"limit": limit}
    if symbol:
        params["symbol"] = symbol
    try:
        res = _session.get(url, params=params, timeout=30)
        if res.status_code == 200:
            return res.json()
    except Exception:
        pass
    return None


def fetch_open_interest(symbol):
    """
    拉取持仓量（预留接口，供未来持仓异动监控使用）。
    """
    url = f"{FAPI_BASE}/fapi/v2/openInterest?symbol={symbol}"
    try:
        res = _session.get(url, timeout=30)
        if res.status_code == 200:
            return res.json()
    except Exception:
        pass
    return None
