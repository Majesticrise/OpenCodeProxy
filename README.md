<div align="center">

# Zen Proxy

**一个用 Rust 编写的高性能 OpenCode Zen 反向代理**

让 VS Code Copilot、Cline、Continue 或任意 OpenAI 兼容客户端，无需安装 OpenCode CLI 即可使用 Zen 免费模型。

[![Rust](https://img.shields.io/badge/rust-1.75%2B-orange.svg)](https://www.rust-lang.org)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](#许可证)
[![Platform](https://img.shields.io/badge/platform-Windows%20%7C%20Linux%20%7C%20macOS-lightgrey.svg)](#)

</div>

---

## ✨ 特性

| | 特性 | 说明 |
|---|---|---|
| 🔐 | **免费层身份伪装** | 完整模拟 OpenCode CLI 的 UA、Session ID、项目 ID、工具契约 |
| 🎭 | **Session ID 精确对齐** | 复刻 `Identifier.descending("ses")` 编码，counter 严格落在 0–63 窗口 |
| 🛠️ | **6 大官方工具注入** | 自动补全 `bash` / `edit` / `glob` / `grep` / `read` / `write`，字母序排列 |
| 🔄 | **代理池自管理** | 从 10 个公共源自动抓取 → 并发验证 → 失败冷却 → 成功复位 |
| 🚦 | **智能失败分级** | 429 短冷却 / 连接失败长冷却 / 403 风控 15 分钟冷却 |
| 🧭 | **模型路由与 fallback** | 客户端模型名 → 上游候选列表，模型不可用时自动切换下一个 |
| ⚡ | **流式 / 非流式自适应** | 上游永远流式（伪装要求），下游非流式时网关内自动聚合 |
| 📊 | **实时指标持久化** | 总请求数、成功率、Token 用量跨重启累计，P95 延迟实时计算 |
| 🗄️ | **SQLite 数据库** | 代理信誉（成功/失败/风控/冷却/上次使用）跨重启保留；请求明细（token 计量/TTFT/代理归属）落库 |
| 🌐 | **IP 情报富化** | ip-api.com 批量查国家/ISP/ASN/机房识别，可选 proxycheck.io 风险评分（iplark 同类信息），参与冷启动选优 |
| 🖥️ | **内置 WebUI** | 仪表盘 / 实时日志 / Playground / 路由配置，零额外部署 |
| 🔌 | **连接池复用** | `DashMap` 按代理 URL 缓存 `wreq::Client`（Chrome131 TLS 指纹模拟），超量自动清理 |

---

## 🚀 快速开始

### 前置要求

- **Rust** 1.75 或更高（`rustup update stable`）
- **Windows** 需安装 [Visual Studio Build Tools](https://visualstudio.microsoft.com/downloads/)（C++ 生成工具）
- **Linux/macOS** 需 `pkg-config` 与 OpenSSL 开发包

### 编译

```bash
git clone https://github.com/yourname/zen-proxy.git
cd zen-proxy
cargo build --release
```

产物位于 `target/release/zen-proxy`（Windows 下为 `zen-proxy.exe`）。

### 运行

```bash
./target/release/zen-proxy
```

首次启动会自动抓取免费代理池并验证（约 60–90 秒）。日志输出：

```
INFO zen_proxy: Listen:    http://0.0.0.0:8910
INFO zen_proxy: WebUI:     http://0.0.0.0:8910/
INFO zen_proxy: Upstream:  https://opencode.ai/inference/openai/v1
INFO zen_proxy::proxy: Proxy pool updated: 8 high-speed verified proxies ready.
```

打开浏览器访问 <http://localhost:8910> 即可看到仪表盘。

---

## 🔌 客户端配置

### VS Code Copilot

在 VS Code 的 `settings.json` 或对应的 `chatLanguageModels.json` 中添加：

```json
{
  "name": "Zen Proxy",
  "vendor": "customendpoint",
  "apiKey": "zen-proxy",
  "apiType": "chat-completions",
  "models": [
    {
      "id": "glm-5.3-flash-free",
      "name": "GLM 5.3 Flash (Free)",
      "url": "http://localhost:8910/v1",
      "toolCalling": true,
      "maxInputTokens": 128000,
      "maxOutputTokens": 16000
    }
  ]
}
```

### Cline / Continue / OpenAI SDK

```python
from openai import OpenAI

client = OpenAI(
    base_url="http://localhost:8910/v1",
    api_key="zen-proxy",  # 任意值，网关会剥离
)

resp = client.chat.completions.create(
    model="glm-5.3-flash-free",
    messages=[{"role": "user", "content": "你好"}],
    stream=True,
)
for chunk in resp:
    print(chunk.choices[0].delta.content or "", end="")
```

### curl 验证

```bash
curl http://localhost:8910/v1/chat/completions \
  -H "Content-Type: application/json" \
  -d '{
    "model": "glm-5.3-flash-free",
    "messages": [{"role": "user", "content": "hi"}],
    "stream": true
  }'
```

---

## 🎛️ 配置

所有配置通过环境变量注入，重启生效。WebUI 内的「模型路由」支持**运行时热更新**。

### 基础配置

| 变量 | 默认值 | 说明 |
|---|---|---|
| `LISTEN_HOST` | `0.0.0.0` | 监听地址 |
| `LISTEN_PORT` | `8910` | 监听端口 |
| `ZEN_UPSTREAM` | `https://opencode.ai/inference/openai/v1` | 上游基础 URL |
| `LOG_LEVEL` | `info` | 日志级别（`error`/`warn`/`info`/`debug`/`trace`） |

### 代理池

| 变量 | 默认值 | 说明 |
|---|---|---|
| `PROXY_FILE` | `proxies.txt` | 本地代理列表（存在时优先使用，跳过网络抓取） |
| `VALIDATE_TARGET` | `https://opencode.ai/zen/v1/models` | 代理验证 / 上游模型探测的目标 |
| `MIN_POOL_SIZE` | `10` | 池内代理低于该阈值时自动补货 |

`proxies.txt` 格式（每行一条，`#` 开头为注释）：

```
# 支持 http / https / socks5
1.2.3.4:8080
http://5.6.7.8:3128
socks5://9.10.11.12:1080
```

### 免费层伪装

| 变量 | 默认值 | 说明 |
|---|---|---|
| `OPENCODE_UA` | `opencode/1.18.34 ai-sdk/provider-utils/4.0.23 runtime/bun/1.3.14` | User-Agent（版本必须 ≥ 1.18.0） |
| `INJECT_FREE_TIER_HEADERS` | `true` | 是否注入 `x-opencode-*` 身份头 |
| `INJECT_FREE_TIER_TOOLS` | `true` | 是否注入 6 大官方工具契约 |

### 模型路由

| 变量 | 示例 | 说明 |
|---|---|---|
| `MODEL_OVERRIDE` | `glm-5.3-flash-free` | 全局覆盖：所有请求都改发该模型 |
| `MODEL_MAPPING` | `a:b\|c,d:e` | 按客户端模型名映射，`\|` 分隔候选，`,` 分隔规则 |

示例：

```bash
# 所有请求都转 glm-5.3-flash-free
MODEL_OVERRIDE=glm-5.3-flash-free ./zen-proxy

# 客户端请求 my-model 时优先 glm-5.3-flash-free，失败切 nemotron-3.5-lightning-free
MODEL_MAPPING="my-model:glm-5.3-flash-free|nemotron-3.5-lightning-free" ./zen-proxy
```

### 运行模式（匿名 / 实名）

WebUI 顶栏滑动开关热切换，无需重启：

- **匿名（默认）**：代理池 + 免费层身份伪装，不带 API Key。
- **实名**：跳过代理池直连 + API Key 鉴权（`Authorization: Bearer`），保留伪装头。
  - Key 在 WebUI「API Key」页配置，明文存于本地 `settings.json`（仅限本机使用）
  - 可选环境变量 `OPENCODE_ORG_ID` 附加 `x-opencode-org-id` 头（不落盘，优先级高于文件）

### 指标持久化

| 变量 | 默认值 | 说明 |
|---|---|---|
| `PERSIST_METRICS` | `true` | 是否把累计指标写入磁盘 |
| `METRICS_FILE` | `metrics_totals.json` | 持久化文件路径 |
| `ZEN_DB` | `zen.db` | SQLite 路径（代理信誉 + 请求明细），空串禁用 |
| `REQUEST_LOG_DAYS` | `90` | 请求明细保留天数（0 = 永久保留） |
| `PROXYCHECK_KEY` | 空 | proxycheck.io 免费 key，启用 IP 风险评分（免费 1000 次/天） |

> 持久化只存**累计总量**（总请求数、成功率、Token 用量）。P95 延迟、每分钟图表只反映最近 60 分钟的实时窗口，重启后从空开始。

---

## 🖥️ WebUI 功能

访问 <http://localhost:8910> 打开仪表盘。

### 总览（Dashboard）

- **指标卡**：总请求数、成功率、P95 延迟、活跃请求（流式/同步分离）
- **Token 使用**：总量、缓存命中、推理、覆盖率
- **代理池状态**：节点数、平均延迟、健康度
- **每分钟 Token 图表**：可视化用量曲线
- **按模型统计**：各模型 prompt/completion/total 分布
- **HTTP 状态分布**：2xx/4xx/5xx 比例

### 实时日志

- 按级别过滤（INFO / WARN / ERROR / DEBUG）
- 关键字搜索
- 暂停 / 自动滚动
- 本地清空显示（不影响服务端缓冲）

### Playground

- 模型下拉（自动从 `/v1/models` 拉取）
- 流式 / 非流式切换
- SSE 增量渲染
- `Ctrl+Enter` 快捷发送

### 模型路由

- 一键探测上游可用模型
- 可视化编辑「客户端模型名 → 上游候选列表」
- 支持 `*` 通配兜底
- 保存后**热生效**，无需重启

### 代理详情

- 代理信誉表：成功 / 失败 / 风控次数（SQLite 跨重启累计）
- IP 情报：国家 / ISP / ASN / 使用场景 / 风险评分（来自 ip-api.com 与 proxycheck.io）
- 实时状态：延迟、冷却倒计时、上次使用时间

### 用量报表

- 按天 Token 用量图表（SQLite 请求明细聚合，不再只有 1 小时窗口）
- 按模型统计：请求量、成功率、Token 三项、平均延迟、平均首字延迟（TTFT）

### API Key 与运行模式

- 顶栏匿名/实名滑动开关（实名需先配置 Key）
- API Key 的设置 / 清除（脱敏展示，落盘 `settings.json`）

### 配置

- 只读运行配置与客户端接入说明

---

## 🏗️ 项目结构

```
zen-proxy/
├── src/
│   ├── main.rs                  # 入口：日志初始化、状态组装、路由注册
│   ├── config.rs                # 环境变量 → 静态配置 + 运行时路由
│   ├── state.rs                 # AppState：共享状态的聚合
│   ├── logger.rs                # 自定义 tracing Layer，写入 Web 缓冲
│   ├── db.rs                    # ⭐ SQLite：代理信誉 + 请求明细 + IP 情报
│   ├── metrics.rs               # 指标采集 + 持久化 + MetricsGuard RAII
│   ├── middleware.rs            # 剥离 Authorization
│   ├── settings.rs              # 运行时设置（匿名/实名模式 + API Key）
│   ├── opencode_identity.rs     # ⭐ 免费层身份伪装（Session / UA / Tools）
│   ├── proxy.rs                 # ⭐ 代理池：抓取、验证、冷却、重选
│   └── handlers/
│       ├── mod.rs               # 静态资源 + 状态端点
│       ├── forward.rs           # ⭐ 核心转发 + fallback chain + SSE 聚合
│       ├── models.rs            # /v1/models
│       ├── routing.rs           # /api/routing 读写
│       ├── settings.rs          # /api/settings/* 模式与密钥管理
│       ├── stats.rs             # /api/proxies 与 /api/usage 报表
│       └── upstream.rs          # /api/upstream/* 探测与缓存
└── web/
    ├── index.html               # 单页多视图
    ├── style.css                # 深色主题
    └── app.js                   # 轮询 + SSE 渲染 + 路由编辑
```

### 关键模块说明

#### `opencode_identity.rs` — 免费层伪装核心

模拟 OpenCode CLI 需要同时满足 **4 层校验**：

| 层 | 条件 | 实现 |
|---|---|---|
| **UA** | 匹配 `opencode/<ver>`，ver ≥ 1.18.0 | 可环境变量覆盖 |
| **Session** | `ses_` + 12 hex + 14 Base62，counter ∈ 0–63 | `Identifier.descending()` 复刻 |
| **Tools** | 6 大工具齐全，`parameters` 非空，字母序 | 自动补全 + 修复 |
| **Stream** | 上游必须 `stream: true` | 强制注入 |

#### `handlers/forward.rs` — 请求编排

```
接收请求
  ├─ 检查服务开关
  ├─ 解析 JSON
  ├─ 模型路由 → 候选列表
  ├─ 工具契约注入 + 强制 stream
  ├─ 重试循环（代理 × 候选）
  │   ├─ 2xx → 成功
  │   ├─ 429 → 短冷却，重试
  │   ├─ 403 FreeTier → 长冷却代理，换 session 重试
  │   ├─ Model unavailable → 切下一候选
  │   └─ 5xx/网络错误 → 长冷却，重试
  ├─ 直连兜底
  └─ 下游适配
      ├─ 流式 → SSE 直接转发
      └─ 非流式 → SSE 聚合成 JSON
```

#### `proxy.rs` — 代理池状态机

```
可用 ←─ report_success ←─ 请求成功
  │
  ├─ report_rate_limit → 冷却 30s，不计失败
  ├─ report_failure    → 冷却 300s，累计失败 5 次移除
  └─ report_banned     → 冷却 900s，累计 10 次移除
```

---

## 📡 API 端点

### 网关代理（OpenAI 兼容）

| 方法 | 路径 | 说明 |
|---|---|---|
| `POST` | `/v1/chat/completions` | 核心转发 |
| `GET` | `/v1/models` | 客户端可见模型列表 |

### 管理 API

| 方法 | 路径 | 说明 |
|---|---|---|
| `GET` | `/health` | 健康检查 |
| `GET` | `/api/status` | 服务状态 + 代理池概况 |
| `GET` | `/api/metrics` | 指标快照 |
| `GET` | `/api/logs` | 日志缓冲 |
| `POST` | `/api/toggle` | 启停代理服务 |
| `GET/POST` | `/api/routing` | 读取/写入模型路由配置 |
| `GET` | `/api/settings` | 读取运行模式与密钥状态（脱敏） |
| `POST` | `/api/settings/mode` | 切换匿名/实名模式 |
| `POST` | `/api/settings/apikey` | 设置/清除 API Key |
| `GET` | `/api/upstream/models` | 缓存的上游模型列表 |
| `POST` | `/api/upstream/refresh` | 主动探测上游并刷新缓存 |
| `GET` | `/api/proxies` | 代理池详情（信誉 / IP 情报 / 冷却状态） |
| `GET` | `/api/usage?days=14` | 用量报表（按天 / 按模型聚合，来自 SQLite 明细） |

---

## ❓ 常见问题

### 为什么会有 `403 FreeTierError`？

上游对免费层做了多层校验，**任一失败即 403**。常见原因与对策：

| 原因 | 对策 |
|---|---|
| Session ID 编码不合法 | 已实现 `counter & 0x3F` 对齐，重启即生效 |
| 工具集不完整 | 已自动补全 6 大官方工具 |
| UA 版本过低 | 设置 `OPENCODE_UA=opencode/1.18.34` |
| 代理 IP 被风控 | 等待代理池冷却（15 分钟）或换 IP |
| TLS 指纹识别 | 见下方「已知限制」 |

### 为什么代理池这么小？

公共免费代理质量普遍较差，`70000+` 原始代理只会有 `5–15` 个通过验证。这是**公开代理生态的固有特征**。

提高池子质量的方法：
1. 使用付费代理或自建代理，写入 `proxies.txt`
2. 降低 `VALIDATE_TARGET` 的响应时间要求
3. 提高 `MIN_POOL_SIZE` 触发更频繁的抓取

### 为什么 Token 数是估算的？

免费层通常**不返回 `usage` 字段**。网关的 fallback 逻辑按字符数估算（中文 ÷1.5，英文 ÷4，混合 ÷3），误差约 ±20–30%，用于趋势展示足够。

若上游返回 `usage`，会优先使用真实值（`real_usage=true`）。

### 如何关闭持久化？

```bash
PERSIST_METRICS=false ./zen-proxy
```

---

## ⚠️ 已知限制

1. **TLS 指纹**：已改用 `wreq`（Chrome 131 TLS/HTTP2 指纹模拟），浏览器指纹不一致的问题已解决；若上游未来针对指纹做更细的校验，需跟进 `wreq-util` 的 Profile。
2. **Session 一致性**：客户端可通过 `x-opencode-session` / `X-Session-Id` 头传入会话 ID 复用（格式不符时自动生成），网关同时维护 session → 代理/直连的亲和绑定。若上游要求"同一对话必须同一 session"，需要按对话 ID 维持亲和性。
3. **免费层规则变化**：Zen 的校验规则会随时间演进，本项目会持续跟进。
4. **代理 IP 暴露**：免费代理可能通过 `X-Forwarded-For` 泄露你的真实 IP。敏感场景请使用 VPN 前置或高匿代理。

---

## 🤝 贡献

欢迎提交 Issue 和 Pull Request。

提交 PR 前请确保：

```bash
cargo fmt
cargo clippy -- -D warnings
cargo build --release
```

均通过。

---

## 📜 许可证

MIT License. 详见 [LICENSE](LICENSE)。

---

## ⚖️ 免责声明

本项目**仅供学习与研究使用**。

- 使用者需自行承担因使用本项目而导致的一切后果
- 请勿将本项目用于违反 OpenCode Zen 服务条款的场景
- 上游服务规则随时可能变更，本项目不保证持续可用
- 请尊重上游服务商的商业决策，如有付费需求请购买正版

---

<div align="center">

**如果这个项目对你有帮助，请点一个 ⭐**

</div>