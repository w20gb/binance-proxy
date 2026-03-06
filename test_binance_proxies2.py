import requests
import time
from datetime import datetime

print(">>> 开始第二轮反代节点寻找测试")

test_symbol = "BTCUSDT"
test_interval = "1h"
test_limit = 5

proxies_to_test = [
    # 策略 4: GitHub 上常见的开源免翻墙代理 (搜集了一些常用的)
    "https://api.wazirx.com/sapi/v1/tickers/24hr", # wazirx often proxies binance
    "https://api.vnesx.com", # another known proxy
    "https://bn.gico.cc",
    "https://testnet.binancefuture.com", # 测试网, 看网络通不通
    "https://cryptoproxy.app/binance-fapi",

    # 策略 5: 使用专门针对中国的域名
    "https://fapi.binance.info", # 币安官方的备用域名
]

def test_proxy(base_url):
    print(f"\n[{datetime.now().strftime('%H:%M:%S')}] 正在测试节点: {base_url}")

    if "api.wazirx.com" in base_url:
        url = base_url
    elif "cryptoproxy.app" in base_url:
        url = f"{base_url}/v1/klines?symbol={test_symbol}&interval={test_interval}&limit={test_limit}"
    else:
        url = f"{base_url}/fapi/v1/klines?symbol={test_symbol}&interval={test_interval}&limit={test_limit}"

    try:
        start_time = time.time()
        headers = {'User-Agent': 'Mozilla/5.0'}
        res = requests.get(url, headers=headers, timeout=5)
        delay = time.time() - start_time

        print(f" -> 状态码: {res.status_code} | 耗时: {delay:.2f}s")

        if res.status_code == 200:
            print(f" -> [OK] 成功拉取! 数据样例: {str(res.json())[:100]}")
            return True
        elif res.status_code == 451:
            print(" -> [拦截] 451")
        else:
             print(f" -> [失败]: {res.text[:100]}")

    except Exception as e:
         print(f" -> [异常] Fetch 失败: {str(e)[:100]}")

    return False

success_count = 0
for proxy in proxies_to_test:
    if test_proxy(proxy):
        success_count += 1
    time.sleep(1)

print("\n>>> 第二轮测试完成")
print(f">>> 总计可用节点: {success_count} / {len(proxies_to_test)}")
