import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
from datetime import datetime, timedelta
import os

USE_TOR = os.environ.get("USE_TOR") == "true"
# 如果使用 Tor，直接连接官方。如果在本地测试则使用您的代理。
BASE_URL = "https://fapi.binance.com" if USE_TOR else os.environ.get("BINANCE_BASE_URL", "https://binance.794988.xyz")

def get_session():
    s = requests.Session()
    if USE_TOR:
        s.proxies = {'http': 'socks5h://127.0.0.1:9050', 'https': 'socks5h://127.0.0.1:9050'}
    s.headers.update({'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'})
    return s

_session = get_session()

# ================= 配置偏好 =================
INTERVAL = "1h"            # 监测的K线级别 (1小时级别最适合量化横盘突破)
LIMIT = 200                # 最大追溯范围 (200根K线，约 8 天)
AMPLITUDE_THRESHOLD = 0.02 # 振幅收敛阈值 (2%): 区间最高价与最低价之差不超过 2%
# ============================================

def get_all_usdt_perpetuals():
    url = f"{BASE_URL}/fapi/v1/exchangeInfo"

    try:
        res = _session.get(url, timeout=30)
        res.raise_for_status()
        data = res.json()
    except Exception as e:
        print(f"获取 exchangeInfo 失败: {e}")
        return []

    symbols = []
    # 筛选出 交易中 的 USDT 永续合约
    for s in data.get('symbols', []):
        if s['quoteAsset'] == 'USDT' and s['contractType'] == 'PERPETUAL' and s['status'] == 'TRADING':
            symbols.append(s['symbol'])
    return symbols

def fetch_klines(symbol):
    """拉取单个币种的 K 线数据"""
    url = f"{BASE_URL}/fapi/v1/klines?symbol={symbol}&interval={INTERVAL}&limit={LIMIT}"
    try:
        res = _session.get(url, timeout=30)
        if res.status_code == 200:
            return symbol, res.json()
    except:
        pass
    return symbol, None

def calc_sideways(klines, threshold=AMPLITUDE_THRESHOLD):
    """
    核心横盘判定算法（向后追溯法）：
    从最新的一根K线开始往前遍历，不断扩张当前的 [最高价, 最低价] 区间。
    一旦该区间的振幅 (Max - Min) / Min 大于 threshold (如 2%)，即视为突破，停止记时。
    """
    if not klines:
        return 0, 0, 0

    klines_reversed = list(reversed(klines))

    current_high = -float('inf')
    current_low = float('inf')

    duration = 0
    for kline in klines_reversed:
        high_price = float(kline[2])
        low_price = float(kline[3])

        temp_high = max(current_high, high_price)
        temp_low = min(current_low, low_price)

        # 计算当前探测窗口的总振幅
        amp = (temp_high - temp_low) / temp_low if temp_low > 0 else 0

        # 振幅超标，意味着不在这个横盘收敛区间了，停止追溯
        if amp > threshold:
            break

        current_high = temp_high
        current_low = temp_low
        duration += 1

    final_amp = (current_high - current_low) / current_low if duration > 0 and current_low > 0 else 0
    current_price = float(klines[-1][4]) # 最新收盘价

    return duration, final_amp, current_price

def main():
    # 注意：运行在云端需显式调整为北京时间
    bj_time = datetime.utcnow() + timedelta(hours=8)
    print(f"[{bj_time.strftime('%Y-%m-%d %H:%M:%S')}] (北京时间) 开始获取全网 USDT 永续合约列表...")

    symbols = get_all_usdt_perpetuals()
    if not symbols:
        print("未能获取到合约列表，程序退出。请检查反代节点网络状态。")
        return

    print(f"共获取到 {len(symbols)} 个活跃合约，启动 20 线程并发拉取引擎...")

    results = []
    start_time = time.time()

    # 并发提速拉取 300+ 币种K线
    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = {executor.submit(fetch_klines, sym): sym for sym in symbols}

        count = 0
        for future in as_completed(futures):
            count += 1
            if count % 50 == 0:
                print(f" -> 已扫描 {count}/{len(symbols)}...")

            symbol = futures[future]
            sym_kline = future.result()

            if sym_kline and sym_kline[1]:
                duration, amp, price = calc_sideways(sym_kline[1])
                results.append({
                    "symbol": symbol,
                    "duration": duration,
                    "amplitude": amp,
                    "price": price
                })

    time_taken = time.time() - start_time
    print(f"数据全部拉取并计算完毕！核心引擎耗时: {time_taken:.2f}s")

    # 按横盘持续时间绝对降序排列
    results.sort(key=lambda x: x["duration"], reverse=True)

    # 写入 Markdown 报告
    report_path = "sideways_report.md"
    with open(report_path, "w", encoding="utf-8", errors="ignore") as f:
        f.write("# 📊 币安 USDT 永续合约【极佳横盘猎手】榜单\n\n")
        f.write(f"> **生成时间**: {bj_time.strftime('%Y-%m-%d %H:%M:%S')} (UTC+8)\n")
        f.write(f"> **运算规则**: 追溯过去 {LIMIT} 根 1小时K线，筛选价格被严格压制在 **{AMPLITUDE_THRESHOLD*100}%** 振幅内的标的。\n")
        f.write(f"> **网络隧道**: `{'Tor 匿名网络 (德国/日本等出口)' if USE_TOR else BASE_URL}`\n\n")

        f.write("| 排名 | 合约标的 | 横盘时长 (小时) | 极致压缩振幅 | 当前价格 | TradingView |\n")
        f.write("|---|---|---|---|---|---|\n")

        # 过滤掉杂音: 仅呈现横盘 > 6 小时的硬核标的
        valid_results = [r for r in results if r["duration"] > 6]

        for i, r in enumerate(valid_results):
            sym = r["symbol"]
            dur = r["duration"]
            amp = f'{r["amplitude"] * 100:.2f}%'
            price = f'${r["price"]:g}'
            # 提供直接点开看图的快捷链接
            link = f"[K线直达](https://www.binance.com/zh-CN/futures/{sym})"

            if i < 3:
                 f.write(f"| 🏆 {i+1} | **{sym}** | **{dur} 根 K线** | {amp} | {price} | {link} |\n")
            else:
                 f.write(f"| {i+1} | {sym} | {dur} 根 K线 | {amp} | {price} | {link} |\n")

        if not valid_results:
             f.write("| - | 当前全网无极端横盘标的，波动性正常释放中 | - | - | - | - |\n")

    print(f"\n[OK] 分析报告已安全写出至: {report_path}")

if __name__ == "__main__":
    main()
