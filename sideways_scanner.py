"""
sideways_scanner.py — 币安 USDT 永续合约横盘扫描引擎

纯业务逻辑，网络层全部委托给 binance_gateway.py。
基于布林带收敛 (Bollinger Band Squeeze) 算法，寻找爆发前兆。
"""

import os
import time
import requests
import json
import pandas as pd
import numpy as np
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta

from binance_gateway import get_all_usdt_perpetuals, fetch_klines, USE_TOR

# ================= 配置偏好 (全面环境变量化) =================
# 从环境变量读取，容错处理：当传入空字符串时，使用 or 降级到默认值
INTERVAL = os.environ.get("INTERVAL") or "1h"
LIMIT = int(os.environ.get("LIMIT") or "200")
BBW_THRESHOLD = float(os.environ.get("BBW_THRESHOLD") or "0.05") # 布林带宽度阈值 (5%)
BB_WINDOW = int(os.environ.get("BB_WINDOW") or "20")             # 布林带计算周期 (默认 20)
BB_TOLERANCE = int(os.environ.get("BB_TOLERANCE") or "1")        # 容忍单根K线的假突破/插针扩大布林带的次数
MIN_DURATION = int(os.environ.get("MIN_DURATION") or "6")        # 最低上榜条件 (默认收敛 > 6 根 K 线)

# 飞书 Webhook 机器人地址
FEISHU_WEBHOOK = os.environ.get("FEISHU_WEBHOOK") or ""

# ================= 黑名单配置 =================
# 过滤天然无波动的稳定币、指数合约等无交易参考价值的标的
BLACKLIST = {
    "USDCUSDT",     # 稳定币
    "BTCDOMUSDT",   # BTC市占率指数
    "DEFIUSDT",     # DeFi 综合指数
    "BLUEBIRDUSDT", # 蓝鸟指数 (Twitter概念)
    "FOOTBALLUSDT", # 足球粉丝代币指数
}
# =========================================================

HISTORY_FILE = "sideways_history.json"

def load_history():
    if os.path.exists(HISTORY_FILE):
        try:
            with open(HISTORY_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}

def save_history(valid_results):
    history = {}
    for i, r in enumerate(valid_results[:25]):
        history[r["symbol"]] = {"rank": i + 1}
    try:
        with open(HISTORY_FILE, "w", encoding="utf-8") as f:
            json.dump(history, f, indent=4)
    except Exception as e:
        print(f"保存历史记录失败: {e}")


def calc_bollinger_squeeze(klines):
    """
    核心横盘判定算法（布林带压缩宽度倒推法）+ 容错机制：
    1. 计算 20 周期的布林带宽度 BBW = (Upper - Lower) / MA
    2. 从最新一根 K 线往前倒推，统计 BBW 连续小于 BBW_THRESHOLD 的根数。
    3. 如果期间偶尔遇到一根长横插针破坏了布林带（BBW变大），只要连续超标次数 <= BB_TOLERANCE，就继续算作横盘/收敛周期内。
    """
    if not klines or len(klines) < BB_WINDOW:
        return 0, 0, 0

    closes = [float(k[4]) for k in klines]
    df = pd.DataFrame({'close': closes})

    # 布林带计算 (使用总体标准差 ddof=0 保持与大部分交易所一致)
    df['ma'] = df['close'].rolling(window=BB_WINDOW).mean()
    df['std'] = df['close'].rolling(window=BB_WINDOW).std(ddof=0)
    df['upper'] = df['ma'] + 2 * df['std']
    df['lower'] = df['ma'] - 2 * df['std']

    df['bbw'] = (df['upper'] - df['lower']) / df['ma']
    bbw_series = df['bbw'].dropna().tolist()

    if not bbw_series:
        return 0, 0, 0

    # 从最近日期倒推
    bbw_reversed = list(reversed(bbw_series))

    duration = 0
    violations = 0

    for bw in bbw_reversed:
        if bw <= BBW_THRESHOLD:
            duration += 1
            violations = 0 # 一旦回到极窄，重置连续破坏次数
        else:
            violations += 1
            if violations > BB_TOLERANCE:
                # 连续破坏次数超标，彻底打断收敛倒计时
                break
            # 容忍期内的张口，依然算入蓄势时长
            duration += 1

    final_bbw = bbw_reversed[0]
    current_price = closes[-1]

    return duration, final_bbw, current_price


def _fetch_klines_wrapper(symbol):
    """对 gateway 的 fetch_klines 做一层包装"""
    return fetch_klines(symbol, interval=INTERVAL, limit=LIMIT)


def notify_feishu(valid_results, bj_time):
    """将横盘榜单格式化并推送到飞书机器人"""
    if not FEISHU_WEBHOOK:
        return

    time_str = bj_time.strftime("%Y-%m-%d %H:%M")

    md_lines = []
    md_lines.append(f"⏱️ **生成时间**: `{time_str}` (北经时间)")
    md_lines.append(f"⚙️ **参数**: 追溯 `{LIMIT}` 根 `{INTERVAL}` K线 | BBW < **{BBW_THRESHOLD*100:.1f}%**")
    md_lines.append(f"🛡️ **策略**: 布林带极致收敛，最大容错 `{BB_TOLERANCE}` 根K线\n---")

    if not valid_results:
         md_lines.append("\n✅ *当前全网无极致收敛标的，波动性正常释放中*")
    else:
         md_lines.append("\n🏆 **【极佳横盘猎手: 布林带收敛榜】(按时长降序)**\n")

         history = load_history()

         # 飞书卡片篇幅有限，最多推送前 25 名最极致的
         for i, r in enumerate(valid_results[:25]):
             sym = r["symbol"]
             dur = r["duration"]
             amp = f'{r["amplitude"] * 100:.2f}%'
             price = f'${r["price"]:g}'
             link = f"[{sym}](https://www.coinglass.com/tv/zh/Binance_{sym})"

             curr_rank = i + 1
             trend_icon = "➖"
             if sym not in history:
                 trend_icon = "🆕" # 新晋榜单
             else:
                 prev_rank = history[sym].get("rank", 999)
                 if curr_rank < prev_rank:
                     trend_icon = "⬆️" # 排名上升
                 elif curr_rank > prev_rank:
                     trend_icon = "⬇️" # 排名下降
                 else:
                     trend_icon = "➖" # 排名持平

             medal = "🥇" if i == 0 else "🥈" if i == 1 else "🥉" if i == 2 else f" {i+1}."
             md_lines.append(f"{medal} **{link}** {trend_icon} | **{dur}** 根缩圈 | 现 BBW {amp} | 现价 {price}")

         if len(valid_results) > 25:
             md_lines.append(f"\n*(共有 {len(valid_results)} 个币满足条件，这里仅展示前25名)*")

    card = {
        "msg_type": "interactive",
        "card": {
            "config": { "wide_screen_mode": True },
            "header": {
                "title": {
                    "tag": "plain_text",
                    "content": "📊 币安 USDT 永续合约【横盘爆发雷达】"
                },
                "template": "turquoise"
            },
            "elements": [
                {
                    "tag": "markdown",
                    "content": "\n".join(md_lines)
                }
            ]
        }
    }

    try:
        req = requests.post(FEISHU_WEBHOOK, json=card, timeout=10)
        req.raise_for_status()
        print(f"✅ 成功推送到飞书，共播报 {len(valid_results)} 个标的。")
    except Exception as e:
        print(f"❌ 飞书推送异常: {e}")


def main():
    bj_time = datetime.utcnow() + timedelta(hours=8)
    print(f"[{bj_time.strftime('%Y-%m-%d %H:%M:%S')}] (北京时间) 开始获取全网 USDT 永续合约列表...")
    print(f"当前配置: 周期={INTERVAL}, 追溯={LIMIT}, BBW阈值={BBW_THRESHOLD}, 容忍度={BB_TOLERANCE}")

    symbols = get_all_usdt_perpetuals()
    if not symbols:
        print("未能获取到合约列表，程序退出。请检查网络隧道状态。")
        return

    # 剔除黑名单干扰标的
    symbols = [sym for sym in symbols if sym not in BLACKLIST]

    print(f"共获取到 {len(symbols)} 个有效合约（已过滤黑名单），启动并发拉取引擎...")

    results = []
    start_time = time.time()

    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = {executor.submit(_fetch_klines_wrapper, sym): sym for sym in symbols}

        count = 0
        for future in as_completed(futures):
            count += 1
            if count % 100 == 0:
                print(f" -> 已扫描 {count}/{len(symbols)}...")

            symbol = futures[future]
            sym_kline = future.result()

            if sym_kline and sym_kline[1]:
                duration, bbw, price = calc_bollinger_squeeze(sym_kline[1])
                results.append({
                    "symbol": symbol,
                    "duration": duration,
                    "amplitude": bbw, # 此处 amplitude 含义变为 bbw 宽度
                    "price": price
                })

    time_taken = time.time() - start_time
    print(f"数据拉取并计算完毕！核心耗时: {time_taken:.2f}s")

    # 按收敛时间绝对降序排列
    results.sort(key=lambda x: x["duration"], reverse=True)

    # 仅保留缩圈时长 >= MIN_DURATION 的核心标的
    valid_results = [r for r in results if r["duration"] >= MIN_DURATION]

    # 1. 写入 Markdown 本地报告 (作为全量数据归档)
    tunnel_info = "Tor 匿名网络" if USE_TOR else "Vercel Edge 反代"
    report_path = "sideways_report.md"
    with open(report_path, "w", encoding="utf-8", errors="ignore") as f:
        f.write("# 📊 币安 USDT 永续合约【极佳横盘猎手: 布林带】全量榜单\n\n")
        f.write(f"> **生成时间**: {bj_time.strftime('%Y-%m-%d %H:%M:%S')} (UTC+8)\n")
        f.write(f"> **运算规则**: 追溯过去 {LIMIT} 根 `{INTERVAL}` 级别K线，寻找布林带极度压缩 (BBW < **{BBW_THRESHOLD*100:.1f}%**) 且蓄势最久的标的。\n")
        f.write(f"> **网络隧道**: `{tunnel_info}`\n\n")

        f.write("| 排名 | 合约标的 | 极致缩圈时长 (K线数) | 当前布林宽度 (BBW) | 当前价格 | TradingView |\n")
        f.write("|---|---|---|---|---|---|\n")

        for i, r in enumerate(valid_results):
            sym = r["symbol"]
            dur = r["duration"]
            amp = f'{r["amplitude"] * 100:.2f}%'
            price = f'${r["price"]:g}'
            link = f"[直达](https://www.coinglass.com/tv/zh/Binance_{sym})"
            f.write(f"| {i+1} | **{sym}** | **{dur}** | {amp} | {price} | {link} |\n")

        if not valid_results:
             f.write("| - | 当前全网无极端横盘标的 | - | - | - | - |\n")

    print(f"\n[OK] 全量报告已归档至: {report_path}")

    # 2. 推送到飞书
    notify_feishu(valid_results, bj_time)

    # 3. 覆盖写入本次历史记录，供下次执行作对比
    save_history(valid_results)

if __name__ == "__main__":
    main()
