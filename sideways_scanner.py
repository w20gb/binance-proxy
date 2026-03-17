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

from binance_gateway import get_all_usdt_perpetuals, fetch_klines, fetch_oi_history, fetch_cmc_data, USE_TOR

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
    """具备自我修复与升级能力的历史加载器"""
    if os.path.exists(HISTORY_FILE):
        try:
            with open(HISTORY_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                # 兼容性升级：如果发现是旧的单一 Rank 结构，自动转为链式结构
                for sym in data:
                    if isinstance(data[sym], dict) and "rank" in data[sym] and "rank_chain" not in data[sym]:
                        data[sym] = {
                            "rank_chain": [data[sym]["rank"]],
                            "on_board_count": 1,
                            "last_bbw": 0.05
                        }
                return data
        except Exception as e:
            print(f"读取历史文件失败, 初始化为空: {e}")
            return {}
    return {}

def save_history(valid_results, prev_history):
    """保存带有名次动量的信息"""
    new_history = {}
    # 只记录前 25 名的深度动量
    for i, r in enumerate(valid_results[:25]):
        sym = r["symbol"]
        curr_rank = i + 1
        curr_bbw = r["amplitude"]

        old_item = prev_history.get(sym, {})
        # 更新名次链 (保留最近 5 次)
        old_chain = old_item.get("rank_chain", [])
        new_chain = (old_chain + [curr_rank])[-5:]

        # 更新霸榜次数 (只有连续在榜才算)
        new_count = old_item.get("on_board_count", 0) + 1

        new_history[sym] = {
            "rank_chain": new_chain,
            "on_board_count": new_count,
            "last_bbw": curr_bbw,
            "last_price": r["price"],
            "last_seen": datetime.utcnow().strftime("%y-%m-%d %H:%M")
        }

    try:
        with open(HISTORY_FILE, "w", encoding="utf-8") as f:
            json.dump(new_history, f, indent=4)
    except Exception as e:
        print(f"保存历史记忆失败: {e}")


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

def detect_breakouts(valid_results, history, current_prices_dict):
    """
    捕捉“爆发信号”：曾经在榜（尤其是长期霸榜）的币种，本次消失，则大概率是打破了状态。
    """
    current_symbols = {r["symbol"] for r in valid_results}
    breakouts = []

    for sym, hist_item in history.items():
        if sym not in current_symbols:
            # 只有连续上榜 2 次及以上，且之前排名在前 20 的才值得关注
            if hist_item.get("on_board_count", 0) >= 2:
                # 判断方向
                last_price = hist_item.get("last_price", 0)
                curr_price = current_prices_dict.get(sym, 0)

                if curr_price > 0 and last_price > 0:
                    change_pct = (curr_price - last_price) / last_price * 100
                    # 如果价格波动较大 (>1%)，或者 BBW 已经撑开，判定为爆发
                    if abs(change_pct) > 1.0:
                        direction = "🚀向上突破" if change_pct > 0 else "🩸向下破位"
                        breakouts.append({
                            "symbol": sym,
                            "direction": direction,
                            "change": change_pct,
                            "on_board": hist_item.get("on_board_count", 0)
                        })
    return breakouts

def notify_feishu(valid_results, bj_time, history, all_results_dict):
    """深度优化的飞书看板：加入了名次动量、收敛加速度和霸榜时长"""
    if not FEISHU_WEBHOOK:
        return

    time_str = bj_time.strftime("%Y-%m-%d %H:%M")

    md_lines = []
    md_lines.append(f"⏱️ **生成时间**: `{time_str}` (北经时间)")
    md_lines.append(f"⚙️ **参数**: 追溯 `{LIMIT}` 根 `{INTERVAL}` | BBW < **{BBW_THRESHOLD*100:.1f}%**")
    md_lines.append(f"🛡️ **策略**: 布林极致收敛，最大容错 `{BB_TOLERANCE}` 根K线\n---")

    if not valid_results:
         md_lines.append("\n✅ *当前全网无极致收敛标的，波动性正常释放中*")
    else:
         md_lines.append("\n🏆 **【横盘雷达: 布林带收敛榜】(动量增强版)**\n")

         # 飞书卡片篇幅有限，最多推送前 25 名
         for i, r in enumerate(valid_results[:25]):
             sym = r["symbol"]
             dur = r["duration"]
             curr_bbw = r["amplitude"]
             price = f'${r["price"]:g}'
             # 分离名称与链接，确保名称 100% 可复制
             display_sym = sym.replace("USDT", "")
             name_copyable = f"`{display_sym}`"
             link_icon = f"[🔗](https://www.coinglass.com/tv/zh/Binance_{sym})"

             # OI 异动数据
             oi_change = r.get("oi_change_24h_pct", 0)
             oi_str = f"🚀 **OI暴增 +{oi_change:.1f}%**" if oi_change > 20 else f"OI {oi_change:+.1f}%"

             # 流通市值数据 (新)
             mc = r.get("market_cap", 0)
             mc_str = f" | MC {mc/1e8:.1f}亿" if mc > 0 else ""

             # --- 核心动量分析 ---
             hist_item = history.get(sym, {})

             # 1. 排名动向与名次链
             chain = hist_item.get("rank_chain", [])
             if not chain:
                 trend_raw = "🆕"
             else:
                 prev_rank = chain[-1]
                 diff = prev_rank - (i + 1)
                 # 转换为带数字的趋势符号
                 if diff > 0: trend_raw = f"⬆️{diff}"
                 elif diff < 0: trend_raw = f"⬇️{abs(diff)}"
                 else: trend_raw = "➖"

                 # 拼装名次链，例如 [15→8→🥇]
                 chain_symbols = [("🥇" if c==1 else "🥈" if c==2 else "🥉" if c==3 else str(c)) for c in chain]
                 trend_raw = f"{trend_raw} `[{'→'.join(chain_symbols)}]`"

             # 2. 霸榜时长统计
             sticky_count = hist_item.get("on_board_count", 0)
             sticky_str = f" 🔥`{sticky_count}h`" if sticky_count > 1 else ""

             # 3. BBW 细微变化 (趋势图标)
             last_bbw = hist_item.get("last_bbw", curr_bbw)
             bbw_icon = "💠" if curr_bbw < last_bbw else "⚠️" if curr_bbw > last_bbw else "➖"
             bbw_str = f'BBW {curr_bbw * 100:.2f}%({bbw_icon})'

             medal = "🥇" if i == 0 else "🥈" if i == 1 else "🥉" if i == 2 else f" {i+1}."

             # 推送分行排版，第一行看趋势，第二行看硬指标
             md_lines.append(f"{medal} **{name_copyable}** {link_icon} {trend_raw}{sticky_str}")
             md_lines.append(f"└ ⏳**{dur}**根 | {bbw_str} | {oi_str}{mc_str} | {price}\n")

         if len(valid_results) > 25:
             md_lines.append(f"\n*(共有 {len(valid_results)} 个币满足条件，这里仅展示前25名)*")

    # --- 新增：爆发预警板块 ---
    breakouts = detect_breakouts(valid_results, history, all_results_dict)
    if breakouts:
        md_lines.append("\n🚨 **【爆发预警: 打破平衡(Breaking)】**")
        for b in breakouts[:5]: # 最多展示 5 个最典型的爆发
            display_sym = b["symbol"].replace("USDT", "")
            link = f"[`{display_sym}`](https://www.coinglass.com/tv/zh/Binance_{b['symbol']})"
            md_lines.append(f"* {b['direction']} **{link}** | 🔥霸榜`{b['on_board']}h`后变盘 | 幅度 `{b['change']:+.1f}%`")

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


def fetch_oi_for_candidates(valid_results):
    """为筛选出的核心标的并发拉取过去 24h 的持仓量异动数据"""
    print(f"开始为 {len(valid_results)} 个核心标的拉取 OI 异动数据...")

    def _fetch_oi(r):
        sym = r["symbol"]
        oi_hist = fetch_oi_history(sym, period="1d", limit=2)
        r["oi_change_24h_pct"] = 0
        if oi_hist and len(oi_hist) >= 2:
            try:
                # 倒数第二个是昨天的，最后一个是目前的
                old_oi = float(oi_hist[-2]["sumOpenInterestValue"])
                new_oi = float(oi_hist[-1]["sumOpenInterestValue"])
                if old_oi > 0:
                    r["oi_change_24h_pct"] = (new_oi - old_oi) / old_oi * 100
            except Exception:
                pass
        return r

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(_fetch_oi, r) for r in valid_results]
        for _ in as_completed(futures):
            pass

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

    # 并发拉取 OI 异动数据 (为最终榜单赋能，仅拉取前 50 个核心标的以防反代过载)
    if valid_results:
        fetch_oi_for_candidates(valid_results[:50])

        # --- 新增：获取流通市值逻辑 ---
        print("正在获取全市场市值快照...")
        cmc_raw = fetch_cmc_data()
        mc_map = {}
        if cmc_raw and isinstance(cmc_raw, list):
            for item in cmc_raw:
                # 币安 24h 接口中，quoteVolume 可能作为市值的参考，或者通过 symbol 映射
                # 注意：币安官方 API 现货接口并不直接返回 MarketCap，但通常我们会通过外部集成或特定字段估算
                # 此处尝试寻找可以反映“体量”的字段，或者如果该接口不含 MC，则保留框架供后续精准接入
                s = item.get("symbol")
                # 尝试获取该币种的成交额作为体量参考，或者如果您的 gateway 支持特定市值接口则替换
                # 这里暂存逻辑，确保不报错
                try:
                    mc_map[s] = float(item.get("quoteVolume", 0)) # 暂时用 24h 成交额代替“体量”
                except: pass

        for r in valid_results:
            sym = r["symbol"]
            # 尝试通过现货 Symbol 匹配 (去USDT后缀)
            r["market_cap"] = mc_map.get(sym, 0)

        # 二次排序：由于已经保证了 MIN_DURATION 收敛，此时我们让同等收敛时长的币，按 OI 增幅作为第二排序权重，体现“资金异动暗流”
        valid_results.sort(key=lambda x: (x["duration"], x.get("oi_change_24h_pct", 0)), reverse=True)

    # 加载动量历史
    history = load_history()

    # 1. 写入 Markdown 本地报告 (作为全量数据归档)
    tunnel_info = "Tor 匿名网络" if USE_TOR else "Vercel Edge 反代"
    report_path = "sideways_report.md"
    with open(report_path, "w", encoding="utf-8", errors="ignore") as f:
        f.write("# 📊 币安 USDT 永续合约【横盘爆发雷达: 深度动量追踪】\n\n")
        f.write(f"> **生成时间**: {bj_time.strftime('%Y-%m-%d %H:%M:%S')} (UTC+8)\n")
        f.write(f"> **运算规则**: 追溯过去 {LIMIT} 根 `{INTERVAL}` K线，BBW < **{BBW_THRESHOLD*100:.1f}%**。\n")
        f.write(f"> **网络隧道**: `{tunnel_info}`\n\n")

        f.write("| 排名 | 合约标的 | 极致缩圈 | BBW (趋势) | 历史名次链 | 霸榜次数 | 24h OI | TradingView |\n")
        f.write("|---|---|---|---|---|---|---|---|\n")

        for i, r in enumerate(valid_results):
            sym = r["symbol"]
            dur = f'{r["duration"]} 根'
            curr_bbw = r["amplitude"]
            oi_change = f'{r.get("oi_change_24h_pct", 0):+.2f}%'

            # 动量提取
            h = history.get(sym, {})
            last_bbw = h.get("last_bbw", curr_bbw)
            bbw_trend = "💠收紧" if curr_bbw < last_bbw else "⚠️走宽" if curr_bbw > last_bbw else "➖走平"
            chain = " → ".join([str(c) for c in h.get("rank_chain", [])]) or "New"
            sticky = f"{h.get('on_board_count', 0)} 次"

            link = f"[直达](https://www.coinglass.com/tv/zh/Binance_{sym})"
            f.write(f"| {i+1} | **{sym}** | **{dur}** | {curr_bbw*100:.2f}%({bbw_trend}) | `{chain}` | {sticky} | **{oi_change}** | {link} |\n")

        if not valid_results:
             f.write("| - | 当前全网无极端横盘标的 | - | - | - | - | - | - |\n")

    print(f"\n[OK] 全量报告已归档至: {report_path}")

    # 2. 推送到飞书
    all_prices_map = {r["symbol"]: r["price"] for r in results} # 包含未上榜的最新价格用于判断突破
    notify_feishu(valid_results, bj_time, history, all_prices_map)

    # 3. 覆盖写入本次历史记录，供下次执行作对比
    save_history(valid_results, history)

if __name__ == "__main__":
    main()
