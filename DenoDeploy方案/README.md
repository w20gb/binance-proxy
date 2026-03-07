# Deno Deploy 币安反代部署指南

## 原理

Cloudflare Worker 的出口 IP 被币安全局封杀（451），但 Deno Deploy 的日本东京节点使用的是
不同的 IP 池（非 Cloudflare 基础设施），大概率不在币安的封锁名单内。

代码逻辑与 CF Worker 完全相同：接收请求 → 智能路由到 `fapi/dapi/api.binance.com` → 返回结果。

---

## 部署步骤

### Step 1：注册 Deno Deploy 账号

1. 访问 https://dash.deno.com/
2. 点击 **Sign in with GitHub**（用您的 GitHub 账号 `w20gb` 登录）
3. 授权完成后进入控制台

### Step 2：创建新项目

1. 点击 **New Project**
2. 选择 **Play** 模式（在线编辑器，最简单）
3. 把 `main.ts` 文件的全部内容粘贴进去
4. 点击右上角 **Save & Deploy**
5. Deno Deploy 会自动给您一个域名，格式为：`https://您的项目名.deno.dev`

### Step 3：验证

在浏览器（关掉VPN用中国裸连）访问：

```
https://您的项目名.deno.dev/fapi/v1/time
```

- 如果返回 `{"serverTime": 1234567890}` → ✅ **大功告成！**
- 如果仍然 451 → 说明 Deno Deploy 的 IP 也被封了，转 Vercel Edge 方案

### Step 4：接入横盘监控

验证通过后，只需在 `binance_gateway.py` 中将 URL 改为：

```python
FAPI_BASE = "https://您的项目名.deno.dev"
```

所有脚本即可通过 Deno Deploy 东京节点访问币安数据，无需 Tor。

---

## 备选：通过 GitHub 仓库关联自动部署

如果您希望代码改动后自动重新部署（而不是手动粘贴），可以：

1. 在 Deno Deploy 控制台选择 **New Project → Deploy from GitHub**
2. 关联您的 GitHub 仓库 `w20gb/binance-proxy`
3. 设置入口文件为 `DenoDeploy方案/main.ts`
4. 每次 push 到 main 分支，Deno Deploy 会自动重新部署

---

## 文件清单

| 文件 | 说明 |
|---|---|
| `main.ts` | 核心反代代码（Deno Deploy 入口） |
| `README.md` | 本文件 |
