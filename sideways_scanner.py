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
import asyncio
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from playwright.async_api import async_playwright

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

def format_number(n):
    """美化数字显示"""
    if n is None or n == 0: return "0"
    if n > 1e8: return f"{n/1e8:.2f}亿"
    if n > 1e4: return f"{n/1e4:.0f}万"
    return f"{n:.1f}"

async def fetch_coinglass_market_data():
    """【黑科技】通过 Playwright 掠夺 Coinglass 的全市场流通市值与持仓数据"""
    target_url = "https://www.coinglass.com/zh/exchanges/Binance"
    results = {}

    async with async_playwright() as p:
        # 遵循协议 Rule 17: 本地优先复用 Edge 实现秒开，GitHub Action 则回退到标准 Chromium
        try:
            browser = await p.chromium.launch(headless=True, channel="msedge")
        except:
            browser = await p.chromium.launch(headless=True)

        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
        page = await context.new_page()

        # 数据劫持脚本 (同步 1h 级别比值模块的高版本逻辑)
        inject_js = """
        (function() {
            const originalParse = JSON.parse;
            JSON.parse = function(text) {
                const result = originalParse.apply(this, arguments);
                try {
                    if (text && text.length > 500 && result && typeof result === 'object') {
                        let list = null;
                        if (Array.isArray(result)) list = result;
                        else if (result.data && Array.isArray(result.data)) list = result.data;
                        else if (result.list && Array.isArray(result.list)) list = result.list;
                        else if (result.data && result.data.list && Array.isArray(result.data.list)) list = result.data.list;

                        if (list && list.length > 5) {
                            let first = list[0];
                            if (first && typeof first === 'object') {
                                let keys = Object.keys(first);
                                let hasSymbol = keys.includes('symbol') || keys.includes('uSymbol');
                                let hasOi = keys.includes('openInterest') || keys.includes('oi');
                                let hasCap = keys.includes('marketCap') || keys.includes('fdv');
                                if (hasSymbol && hasOi && hasCap) {
                                    if (window.onCapturedData) window.onCapturedData(JSON.stringify(list));
                                }
                            }
                        }
                    }
                } catch(e) {}
                return result;
            };
        })();
        """
        data_captured = asyncio.Future()

        async def on_data(d):
            if not data_captured.done(): data_captured.set_result(d)

        await page.expose_function("onCapturedData", on_data)
        await page.add_init_script(inject_js)

        try:
            await page.goto(target_url, wait_until="domcontentloaded", timeout=60000)
            # 等待数据包到达
            raw_json = await asyncio.wait_for(data_captured, timeout=40.0)
            data_list = json.loads(raw_json)

            if not isinstance(data_list, list):
                print(f"⚠️ [Debug] Captured data is not a list: {type(data_list)}")
                return results

            for item in data_list:
                if not isinstance(item, dict): continue

                sym = str(item.get("symbol") or item.get("uSymbol") or "").replace("/USDT", "").replace("1000", "")
                if not sym: continue
                # 关键字段提取 (使用 get 容错)
                oi_val = float(item.get("openInterest") or item.get("oi") or 0)
                mc_val = float(item.get("marketCap") or item.get("fullyDilutedMarketCap") or item.get("fdv") or 0)

                if mc_val > 0:
                    results[f"{sym}USDT"] = {"oi": oi_val, "mc": mc_val}

        except Exception as e:
            print(f"⚠️ Coinglass 数据抓取异常: {e}")
        finally:
            await browser.close()
    return results

def notify_feishu(valid_results, bj_time, history, all_results_dict):
    """深度优化的飞书看板：加入了名次动量、收敛加速度和霸榜时长"""
    if not FEISHU_WEBHOOK:
        return

    time_str = bj_time.strftime("%Y-%m-%d %H:%M")

    md_lines = []
    md_lines.append(f"⏱️ **生成时间**: `{time_str}` (北京时间)")
    md_lines.append(f"⚙️ **参数**: 追溯 `{LIMIT}` 根 `{INTERVAL}` | BBW < **{BBW_THRESHOLD*100:.1f}%**")
    md_lines.append(f"🔤 **排序方式**: 时长Top25按名称升序\n---")

    if not valid_results:
         md_lines.append("\n✅ *当前全网无极致收敛标的，波动性正常释放中*")
    else:
         md_lines.append("\n🏆 **【横盘雷达: 缩圈中的资金异动 (OI/MC Ratio)】**\n")

         # 飞书卡片篇幅有限，最多推送前 25 名
         for i, r in enumerate(valid_results[:25]):
             sym = r["symbol"]
             dur = r["duration"]
             curr_bbw = r["amplitude"]
             price = f'${r["price"]:g}'

             # 提取 OI, MC 和 Ratio
             oi_val = r.get("oi_value", 0)
             mc_val = r.get("mc_value", 0)
             ratio = r.get("oi_mc_ratio", 0)

             # 校对显示：格式 OI: 1.2亿 / MC: 2.4亿 = 0.50
             comp_str = f"OI:`{format_number(oi_val)}` / MC:`{format_number(mc_val)}` = **`{ratio:.2f}`**"

             # OI 24h 异动符号
             oi_change = r.get("oi_change_24h_pct", 0)
             oi_trend = f" (OI {oi_change:+.1f}%)" if abs(oi_change) > 5 else ""

             # 分离名称与链接
             display_sym = sym.replace("USDT", "")
             name_copyable = f"`{display_sym}`"
             link_icon = f"[🔗](https://www.coinglass.com/tv/zh/Binance_{sym})"

             # --- 核心动量分析 ---
             hist_item = history.get(sym, {})
             chain = hist_item.get("rank_chain", [])
             if not chain:
                 trend_raw = "🆕"
             else:
                 prev_rank = chain[-1]
                 diff = prev_rank - (i + 1)
                 trend_raw = f"⬆️{diff}" if diff > 0 else f"⬇️{abs(diff)}" if diff < 0 else "➖"

             sticky_count = hist_item.get("on_board_count", 0)
             sticky_str = f" 🔥`{sticky_count}h`" if sticky_count > 1 else ""

             # BBW 细微变化
             last_bbw = hist_item.get("last_bbw", curr_bbw)
             bbw_icon = "💠" if curr_bbw < last_bbw else "⚠️" if curr_bbw > last_bbw else "➖"
             bbw_str = f'BBW {curr_bbw * 100:.2f}%({bbw_icon})'

             medal = "🥇" if i == 0 else "🥈" if i == 1 else "🥉" if i == 2 else f" {i+1}."

             # 排版：第一行 Symbol + 趋势；第二行 缩圈+BBW；第三行 核心校对数据
             md_lines.append(f"{medal} **{name_copyable}** {link_icon} {trend_raw}{sticky_str}")
             md_lines.append(f"└ ⏳**{dur}**根 | {bbw_str}{oi_trend}")
             md_lines.append(f"└ 📊 {comp_str} | {price}\n")

         if len(valid_results) > 25:
             md_lines.append(f"\n*(共有 {len(valid_results)} 个币满足条件，这里仅展示前25名)*")

    # 爆发预警板块
    breakouts = detect_breakouts(valid_results, history, all_results_dict)
    if breakouts:
        md_lines.append("\n🚨 **【爆发预警: 打破平衡(Breaking)】**")
        for b in breakouts[:5]:
            display_sym = b["symbol"].replace("USDT", "")
            link = f"[`{display_sym}`](https://www.coinglass.com/tv/zh/Binance_{b['symbol']})"
            md_lines.append(f"* {b['direction']} **{link}** | 🔥霸榜`{b['on_board']}h`后变盘 | 幅度 `{b['change']:+.1f}%`")

    card = {
        "msg_type": "interactive",
        "card": {
            "config": { "wide_screen_mode": True },
            "header": {
                "title": { "tag": "plain_text", "content": "📊 币安 USDT 永续合约【横盘爆发雷达】" },
                "template": "turquoise"
            },
            "elements": [ { "tag": "markdown", "content": "\n".join(md_lines) } ]
        }
    }

    try:
        req = requests.post(FEISHU_WEBHOOK, json=card, timeout=10)
        req.raise_for_status()
        print(f"✅ 成功推送到飞书，共播报 {len(valid_results)} 个标的。")
    except Exception as e:
        print(f"❌ 飞书推送异常: {e}")


def fetch_oi_for_candidates(valid_results):
    """拉取 24h OI 变化 (保持原逻辑作为补充)"""
    def _fetch_oi(r):
        sym = r["symbol"]
        oi_hist = fetch_oi_history(sym, period="1d", limit=2)
        r["oi_change_24h_pct"] = 0
        if oi_hist and len(oi_hist) >= 2:
            try:
                old_oi = float(oi_hist[-2]["sumOpenInterestValue"])
                new_oi = float(oi_hist[-1]["sumOpenInterestValue"])
                if old_oi > 0: r["oi_change_24h_pct"] = (new_oi - old_oi) / old_oi * 100
            except: pass
        return r

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(_fetch_oi, r) for r in valid_results]
        for _ in as_completed(futures): pass

def main():
    bj_time = datetime.utcnow() + timedelta(hours=8)
    print(f"[{bj_time.strftime('%Y-%m-%d %H:%M:%S')}] 开始获取全网 USDT 永续合约列表...")

    symbols = get_all_usdt_perpetuals()
    if not symbols: return
    symbols = [sym for sym in symbols if sym not in BLACKLIST]

    results = []
    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = {executor.submit(_fetch_klines_wrapper, sym): sym for sym in symbols}
        for future in as_completed(futures):
            symbol = futures[future]
            sym_kline = future.result()
            if sym_kline and sym_kline[1]:
                duration, bbw, price = calc_bollinger_squeeze(sym_kline[1])
                results.append({"symbol": symbol, "duration": duration, "amplitude": bbw, "price": price})

    # 仅保留横盘标的
    valid_results = [r for r in results if r["duration"] >= MIN_DURATION]

    if valid_results:
        # 1. 虽然已经有了 Coinglass 的 OI，但 24h 变化依然通过原 Binance 接口拉取以保持一致性
        fetch_oi_for_candidates(valid_results[:50])

        # 2. 从 Coinglass 获取流通市值和精准的 OI 总量
        print("正在从 Coinglass 抓取全市场流通市值快照...")
        cg_data = asyncio.run(fetch_coinglass_market_data())

        for r in valid_results:
            sym = r["symbol"]
            item = cg_data.get(sym, {})
            r["oi_value"] = item.get("oi", 0)
            r["mc_value"] = item.get("mc", 0)
            r["oi_mc_ratio"] = r["oi_value"] / r["mc_value"] if r["mc_value"] > 0 else 0

        # 1. 首先按横盘时长 (duration) 降序排列，次要按 BBW (amplitude) 升序
        valid_results.sort(key=lambda x: (-x["duration"], x["amplitude"]))

        # 2. 截取前 25 名（横盘最久的标的集）
        top_candidates = valid_results[:25]

        # 3. 对这最久的 25 个标的进行币种名升序排列 (用户最新精确需求)
        top_candidates.sort(key=lambda x: x["symbol"])

        # 4. 重新拼接结果，确保报告和推送的前 25 是时长最久且名称有序的
        valid_results = top_candidates + valid_results[25:]

    # 归档 Markdown 报告
    history = load_history()
    report_path = "sideways_report.md"
    with open(report_path, "w", encoding="utf-8", errors="ignore") as f:
        f.write("# 📊 币安 USDT 永续合约【横盘爆发雷达】\n\n")
        f.write(f"> **生成时间**: {bj_time.strftime('%Y-%m-%d %H:%M:%S')} | **排序规则**: 时长前25名按名称升序\n\n")
        f.write("| 排名 | 合约标的 | Ratio | OI | MC | 极致缩圈 | BBW | 24h OI% | TradingView |\n")
        f.write("|---|---|---|---|---|---|---|---|---|\n")
        for i, r in enumerate(valid_results):
            f.write(f"| {i+1} | **{r['symbol']}** | **{r.get('oi_mc_ratio',0):.2f}** | {format_number(r.get('oi_value',0))} | {format_number(r.get('mc_value',0))} | {r['duration']} 根 | {r['amplitude']*100:.2f}% | {r.get('oi_change_24h_pct',0):+.1f}% | [直达](https://www.coinglass.com/tv/zh/Binance_{r['symbol']}) |\n")

    # 推送飞书
    all_prices_map = {r["symbol"]: r["price"] for r in results}
    notify_feishu(valid_results, bj_time, history, all_prices_map)
    save_history(valid_results, history)

if __name__ == "__main__":
    main()
