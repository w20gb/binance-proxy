import requests
import json
import time
from datetime import datetime

print(">>> 开始验证币安公开反代节点可行性")

test_symbol = "BTCUSDT"
test_interval = "1h"
test_limit = 5

proxies_to_test = [
    # 策略 1: 官方直连 (用于对照基准, 大陆/美国会451)
    "https://fapi.binance.com",

    # 策略 2: 常见知名公开第三方反代节点 (通常提供免翻墙访问)
    "https://fapi.binanceapi.com",
    "https://fapi.hbdm.com",
    "https://fapi.mexc.com", # MEXC's wrapper if it exists (highly unlikely but worth checking)
    "https://api.binance.vision",  # 仅限现货, 用于对比网络环境

    # 策略 3: 使用第三方通用加速代理包装官方URL
    "https://corsproxy.io/?https://fapi.binance.com"
]

def test_proxy(base_url):
    print(f"\n[{datetime.now().strftime('%H:%M:%S')}] 正在测试节点: {base_url}")

    # 区分是否使用了 cors代理包装
    if "corsproxy" in base_url:
        url = f"{base_url}/fapi/v1/klines?symbol={test_symbol}&interval={test_interval}&limit={test_limit}"
    elif "api.binance.vision" in base_url:
        print(" -> 跳过 (该节点仅供现货 api.binance.vision 测试, 无合约 v1/klines 接口)")
        return
    else:
        url = f"{base_url}/fapi/v1/klines?symbol={test_symbol}&interval={test_interval}&limit={test_limit}"

    try:
        start_time = time.time()
        # 设置合理的超时时间，伪装User-Agent防拦截
        headers = {
             'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        res = requests.get(url, headers=headers, timeout=10)
        delay = time.time() - start_time

        print(f" -> 状态码: {res.status_code} | 耗时: {delay:.2f}s")

        if res.status_code == 200:
            data = res.json()
            if isinstance(data, list) and len(data) > 0:
                print(f" -> [OK] 成功拉取! 返回了 {len(data)} 根 K 线。首根时间戳: {data[0][0]}")
                return True
            else:
                 print(f" -> [异常] 状态码200但返回格式未知: {str(data)[:100]}")
        elif res.status_code == 451:
            print(" -> [拦截] 遭到币安地区管控拦截 (451 Unavailable For Legal Reasons)")
        else:
             print(f" -> [失败] 响应截断 / 错误: {res.text[:100]}")

    except requests.exceptions.Timeout:
         print(" -> [超时] 无法连接该节点 (Timeout)")
    except Exception as e:
         print(f" -> [异常] Fetch 失败: {str(e)}")

    return False

success_count = 0
for proxy in proxies_to_test:
    if test_proxy(proxy):
        success_count += 1
    time.sleep(1) # 简单防并发限流

print("\n>>> 测试完成")
print(f">>> 总计可用节点数量: {success_count} / {len(proxies_to_test)}")
