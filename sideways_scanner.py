"""
sideways_scanner.py — 币安 USDT 永续合约横盘扫描引擎

纯业务逻辑，网络层全部委托给 binance_gateway.py。
"""

from concurrent.futures import ThreadPoolExecutor, as_completed
import time
from datetime import datetime, timedelta

from binance_gateway import get_all_usdt_perpetuals, fetch_klines, USE_TOR

# ================= 配置偏好 =================
INTERVAL = "1h"            # 监测的K线级别
LIMIT = 200                # 最大追溯范围 (200根K线，约 8 天)
AMPLITUDE_THRESHOLD = 0.02 # 振幅收敛阈值 (2%)
# ============================================


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

        amp = (temp_high - temp_low) / temp_low if temp_low > 0 else 0

        if amp > threshold:
            break

        current_high = temp_high
        current_low = temp_low
        duration += 1

    final_amp = (current_high - current_low) / current_low if duration > 0 and current_low > 0 else 0
    current_price = float(klines[-1][4])

    return duration, final_amp, current_price


def _fetch_klines_wrapper(symbol):
    """对 gateway 的 fetch_klines 做一层包装，传入本扫描器的配置参数"""
    return fetch_klines(symbol, interval=INTERVAL, limit=LIMIT)


def main():
    bj_time = datetime.utcnow() + timedelta(hours=8)
    print(f"[{bj_time.strftime('%Y-%m-%d %H:%M:%S')}] (北京时间) 开始获取全网 USDT 永续合约列表...")

    symbols = get_all_usdt_perpetuals()
    if not symbols:
        print("未能获取到合约列表，程序退出。请检查网络隧道状态。")
        return

    print(f"共获取到 {len(symbols)} 个活跃合约，启动 20 线程并发拉取引擎...")

    results = []
    start_time = time.time()

    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = {executor.submit(_fetch_klines_wrapper, sym): sym for sym in symbols}

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
    tunnel_info = "Tor 匿名网络 (德国/日本等出口)" if USE_TOR else "Cloudflare Worker 反代"
    report_path = "sideways_report.md"
    with open(report_path, "w", encoding="utf-8", errors="ignore") as f:
        f.write("# 📊 币安 USDT 永续合约【极佳横盘猎手】榜单\n\n")
        f.write(f"> **生成时间**: {bj_time.strftime('%Y-%m-%d %H:%M:%S')} (UTC+8)\n")
        f.write(f"> **运算规则**: 追溯过去 {LIMIT} 根 1小时K线，筛选价格被严格压制在 **{AMPLITUDE_THRESHOLD*100}%** 振幅内的标的。\n")
        f.write(f"> **网络隧道**: `{tunnel_info}`\n\n")

        f.write("| 排名 | 合约标的 | 横盘时长 (小时) | 极致压缩振幅 | 当前价格 | TradingView |\n")
        f.write("|---|---|---|---|---|---|\n")

        # 过滤掉杂音: 仅呈现横盘 > 6 小时的硬核标的
        valid_results = [r for r in results if r["duration"] > 6]

        for i, r in enumerate(valid_results):
            sym = r["symbol"]
            dur = r["duration"]
            amp = f'{r["amplitude"] * 100:.2f}%'
            price = f'${r["price"]:g}'
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
