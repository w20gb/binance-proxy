/**
 * 币安 API 反向代理 — Deno Deploy 版
 *
 * 功能：将所有请求透明转发至币安官方 API，自动路由到正确的域名。
 * 部署在 Deno Deploy 东京节点后，出口 IP 为日本普通 IDC，不会被币安 451 封锁。
 *
 * 使用方式：
 *   原本: https://fapi.binance.com/fapi/v1/klines?symbol=BTCUSDT&interval=1h
 *   改为: https://你的项目名.deno.dev/fapi/v1/klines?symbol=BTCUSDT&interval=1h
 */

Deno.serve(async (request: Request) => {

    // 处理 CORS 预检请求
    if (request.method === "OPTIONS") {
        return new Response(null, {
            headers: {
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Methods": "GET, POST, PUT, DELETE, OPTIONS",
                "Access-Control-Allow-Headers": "Content-Type, X-MBX-APIKEY",
            },
        });
    }

    const url = new URL(request.url);
    const path = url.pathname;
    const search = url.search;

    // 智能路由：根据路径前缀自动选择币安的对应域名
    let targetHost = "api.binance.com";        // 默认：现货 API
    if (path.startsWith("/fapi")) {
        targetHost = "fapi.binance.com";          // U本位合约 API
    } else if (path.startsWith("/dapi")) {
        targetHost = "dapi.binance.com";          // 币本位合约 API
    }

    const targetUrl = `https://${targetHost}${path}${search}`;

    // === 伪装策略 ===
    const proxyHeaders = new Headers();

    // 1. 只透传必要的业务头部
    const headersToKeep = ["accept", "accept-language", "content-type", "x-mbx-apikey"];
    for (const [key, value] of request.headers) {
        if (headersToKeep.includes(key.toLowerCase())) {
            proxyHeaders.set(key, value);
        }
    }

    // 2. 设置目标 Host
    proxyHeaders.set("Host", targetHost);

    // 3. 注入真实浏览器 User-Agent
    proxyHeaders.set(
        "User-Agent",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    );

    try {
        const response = await fetch(targetUrl, {
            method: request.method,
            headers: proxyHeaders,
            body: request.method !== "GET" && request.method !== "HEAD" ? request.body : null,
        });

        // 创建新响应，附加 CORS 头
        const responseHeaders = new Headers(response.headers);
        responseHeaders.set("Access-Control-Allow-Origin", "*");

        return new Response(response.body, {
            status: response.status,
            statusText: response.statusText,
            headers: responseHeaders,
        });

    } catch (e) {
        return new Response(JSON.stringify({ error: (e as Error).message }), {
            status: 502,
            headers: { "Content-Type": "application/json" },
        });
    }
});
