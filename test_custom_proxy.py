import requests
import time

proxy_url = "https://binance-proxy.3046790769.workers.dev"

def test_custom_proxy():
    print(f">>> 开始测试自定义域名代理: {proxy_url}")

    time_url = f"{proxy_url}/fapi/v1/time"
    try:
        res = requests.get(time_url, timeout=5)
        print(f"[测试 1] Ping 接口状态码: {res.status_code}")
        if res.status_code == 200:
            print(f" -> 服务器时间: {res.json()}")
    except Exception as e:
        print(f"[测试 1] 错误: {e}")

if __name__ == "__main__":
    test_custom_proxy()
