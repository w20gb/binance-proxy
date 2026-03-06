// 更强大的伪装版 Binance Proxy Worker (带 IP 欺骗，防 451 拦截)
addEventListener('fetch', event => {
  event.respondWith(handleRequest(event.request))
})

async function handleRequest(request) {
  const url = new URL(request.url)
  const path = url.pathname
  const search = url.search

  // 智能路由
  let targetHost = 'api.binance.com'
  if (path.startsWith('/fapi')) {
    targetHost = 'fapi.binance.com'
  } else if (path.startsWith('/dapi')) {
    targetHost = 'dapi.binance.com'
  }

  const targetUrl = `https://${targetHost}${path}${search}`

  // === 核心伪装逻辑 ===
  const newHeaders = new Headers()

  // 1. 只保留必要的头部，坚决不转发原有的 Host 和 CF 特有头部
  const headersToKeep = ['accept', 'accept-language', 'content-type', 'x-mbx-apikey'];
  for (let [key, value] of request.headers) {
    if (headersToKeep.includes(key.toLowerCase())) {
      newHeaders.set(key, value)
    }
  }

  // 2. 强行设置 Host 为币安的真实域名，突破 CDN 走私校验
  newHeaders.set('Host', targetHost)

  // 3. 模拟一个非常真实的人类浏览器 User-Agent
  newHeaders.set('User-Agent', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')

  // 4. IP 欺骗降维打击：清除云端服务器可能导致的 451 封锁，并注入随机的台湾/日本 IP
  newHeaders.delete('cf-connecting-ip')
  newHeaders.delete('x-real-ip')
  // 随机生成一个中华电信 (台湾省) 的合法合规 IP 段
  newHeaders.set('X-Forwarded-For', '210.61.12.' + Math.floor(Math.random() * 255))

  const newRequest = new Request(targetUrl, {
    method: request.method,
    headers: newHeaders,
    body: request.method !== 'GET' ? request.body : null,
    redirect: 'follow'
  })

  try {
    const response = await fetch(newRequest)
    // 获取返回数据的载体
    const newResponse = new Response(response.body, response)

    // 5. 跨域豁免：允许您未来在本地任何 Web 前端程序中无障碍跨域调用这个接口
    newResponse.headers.set('Access-Control-Allow-Origin', '*')
    return newResponse
  } catch (e) {
    return new Response(JSON.stringify({ error: e.message }), { status: 500 })
  }
}
