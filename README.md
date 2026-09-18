# OAI-Adversarial Plugin（O/对抗插件）

## 开发说明与声明

- 本插件由 **DeepSeek「大肥鱼」** 进行编程开发；
- 存在潜在的 bug 与问题属于正常现象，本项目主要用于提供实现思路；
- 欢迎在遵循开源许可（MIT）的前提下进行二次开发。

---

面向 [CLIProxyAPI](https://github.com/router-for-me/CLIProxyAPI) 的自研插件（内部 ID：`timezone-override`），聚焦 Codex / ChatGPT 上游请求链路的三类能力：**请求时区规范化、上游模型一致性监测、X-Codex-Turn-State 的观测与有效值覆写**。

> ⚠️ **重要提示（务必先读）**
> 本文中「**292 字节**」是本项目在特定账号与套餐下观察到的**有效状态长度基准**。不同账号、地区、套餐或上游策略下，有效字段长度可能不同——我们也曾观察到 **312 字节的风控降级态**（模型响应一致但状态被降级）。
> 因此：**探测验收规则（模型一致 + 长度）需要按你自己的实际观测调整**，请勿直接假设 292 就是唯一正确答案。调整位置见「验收标准」一节。

---

## 目录

- [功能特性](#功能特性)
- [架构总览](#架构总览)
- [快速开始](#快速开始)
- [配置详解](#配置详解)
- [工作原理](#工作原理)
- [验收标准与长度说明](#验收标准与长度说明)
- [管理面板](#管理面板)
- [常见问题](#常见问题)
- [开发与测试](#开发与测试)
- [合规与免责声明](#合规与免责声明)
- [License](#license)

---

## 功能特性

### 1. 请求时区规范化（原始功能）
对发往 Codex 上游的请求体做时区处理：检测 `<environment_context><timezone>…</timezone></environment_context>` 块，将原时区替换（或缺失时补入）为目标时区（默认 `America/Los_Angeles`），支持 Responses / Chat Completions / Claude 格式与 WebSocket 通道。

### 2. 上游模型一致性监测
捕获上游实际返回的模型名，与入站请求模型比对；不一致的记录在面板醒目打标（类 sub2api 的「模型不一致」提示）。

### 3. X-Codex-Turn-State 全链路观测
对每条请求记录该字段的**长度**、**来源**（请求 / 响应 / 流 / 探测轨）、**完整值**（4096 字节上限）与**短预览**，并通过 Fernet 内嵌时间戳计算剩余有效期。

### 4. 请求侧覆写（含健康值优先策略）
将匹配模型的请求头 `X-Codex-Turn-State` 替换为有效值：
- **探测值优先**：存在未过期的探测捕获值时使用它；
- **配置值兜底**：探测轨关闭或暂未产出时回退到配置的静态值；
- **强制 / 仅补充**两种模式（`force`）。

### 5. 探测轨（Probe Track）
后台协程独立运行（不经过 CPA 主链路）：
- **TTL 调度**：缓存有效期内不探测；剩余时间进入窗口（默认 5 分钟）后自动开始刷新；
- **代理池轮询**：`direct` / `socks5` / `http` 出口按序轮换；
- **验收规则**：模型一致 + 有效长度（默认 292 字节）才算成功；
- **状态路由台控制**：每个模型行提供暂停/启动图标按钮（暂停移出探测队列、启动立即后台探测并重新入队）；右上角提供「降智请求拒绝」开关（默认开启；开启后，当请求模型携带降智证据——状态长度异常、模型不一致或轮内累计失败达阈值——时，直接以 403 中文错误拦截请求，上游零调用；429 限流不计入降智；重新加载后恢复上次开关状态）。
- **失败冷却**：单轮达到上限（默认 30 次）后在面板标注失败并进入冷却（默认 20 分钟）；冷却到期自动开启新一轮，如此反复；成功时自动清除标注。

### 6. 状态路由台 UI
CPA 管理面板内两个标签页：请求记录、状态路由台（模型状态值 TTL 进度、探测轨迹、失败徽标、来源提示）。

---

## 架构总览

```
业务轨（正常流量）:
  Code Desktop / API Client
     └─► CPA (:8317)
           ├─ 时区规范化
           ├─ Turn-State 覆写（探测值 → 配置值兜底）
           └─► 既有代理出口 ──► Upstream (chatgpt.com/backend-api/codex)

探测轨（插件内后台协程，独立 egress 池）:
  Probe Engine
     ├─ 每 30s 扫描各模型 TTL
     ├─ 剩余 <5min → 激活探测（每 5s 一次）
     ├─ egress 池轮换：direct → socks5 → http proxy
     ├─ 验收：模型一致 + 长度 == 292B
     └─ 通过 → 原子热切换值表（供覆写使用）
```

**插件钩子**（CPA 插件宿主能力）：
`request.intercept_before` / `request.intercept_after`（请求覆写）、`response.intercept_after`（非流式观测）、`response.intercept_stream_chunk`（流式观测）、`websocket.response_event`（WS 观测）、`management_api`（面板路由）。

---

## 快速开始

### 前置条件
- Go 1.26+（含 CGO，需 `gcc`/`cc`）
- 已启用插件机制的 CLIProxyAPI v7.x（插件 `schema_version 6` 支持）
- Codex 账号的 auth JSON（含 `access_token` / `account_id`，由 CPA 的 OAuth 流程生成）
- 可选：一个或多个代理出口（socks5 / http）用于探测轨轮换

### 构建

```bash
make build          # 等价于：CGO_ENABLED=1 go build -buildmode=c-shared -o build/plugin.so ./src
```

### 安装

1. 将 `build/plugin.so` 放到 CPA 插件目录，命名为 `<plugin-id>.so`（本插件：`timezone-override.so`）：

```
<CPA plugins dir>/linux/amd64/timezone-override.so
```

2. 在 CPA `config.yaml` 中加入配置（参考 `config.example.yaml`）：

```yaml
plugins:
  enabled: true
  dir: "/CLIProxyAPI/logs/.plugins"
  configs:
    timezone-override:
      enabled: true
      priority: 100
      turn-state-override: { ... }   # 详见配置详解
```

3. 热加载：修改配置后 CPA 会自动重载插件（无需重启容器）。

4. 验证：CPA 管理页 → 插件菜单「O/对抗插件」→ 查看「状态路由台」。

---

## 配置详解

所有配置位于 `plugins.configs.timezone-override` 之下，均为热加载。

### `turn-state-override`

| 键 | 类型 | 默认 | 说明 |
|---|---|---|---|
| `enabled` | bool | false | 启用请求侧覆写 |
| `models` | []string | `[]` | 覆写目标模型（不区分大小写前缀匹配） |
| `value` | string | `""` | 兜底静态值（探测轨未产出时使用） |
| `force` | bool | false | true=总是替换；false=仅当客户端没有该头时补充 |

### `turn-state-override.probe`

| 键 | 类型 | 默认 | 说明 |
|---|---|---|---|
| `enabled` | bool | false | 探测轨总开关 |
| `models` | []string | astra→sol→luna→terra | 探测优先级（前一个刷新后才轮到下一个） |
| `cred-file` | string | — | Codex auth JSON 路径（容器内路径） |
| `proxies` | []string | `["direct"]` | 内联出口列表 |
| `proxies-file` | string | — | 独立出口文件（每行一个，`#` 注释；优先于内联） |
| `ttl-minutes` | int | 55 | 缓存有效期（由 token 内嵌时间戳计算） |
| `probe-window-minutes` | int | 5 | 进入该剩余窗口后开始探测 |
| `scan-interval-seconds` | int | 30 | TTL 扫描周期 |
| `probe-interval-seconds` | int | 5 | 探测重试间隔 |
| `attempts-per-proxy` | int | 3 | 单出口连续尝试次数（之后轮换） |
| `max-attempts-per-round` | int | 0（自动） | 单轮总尝试上限；`0` 表示按「出口数 × 每出口尝试次数」自动计算（默认 3 出口 × 3 次 = 9），超限标注失败 |
| `cooldown-minutes` | int | 20 | 单轮失败后的冷却时间；到期自动开启新一轮 |
| `suspect-threshold` | int | 3 | 轮内降智证据失败达到该次数后提前触发降智拒绝（打满后转正式判定） |
| `timeout-seconds` | int | 60 | 单次探测超时 |
| `prompt` | string | `"hi"` | 探测请求内容（最小化消耗） |
| `upstream-url` | string | chatgpt.com/backend-api/codex/responses | 上游探测端点 |

### 出口文件（`proxies-file`）示例

```
# one egress per line; 'direct' = 容器自身网络
direct
socks5://user:pass@host:port
http://user:pass@host:port
```

> 🔐 建议：该文件仅包含出口凭据，存放于仓库之外（例如插件目录旁），权限 `600`。

---

## 工作原理

### Turn-State 是什么

`X-Codex-Turn-State` 是上游在响应头下发、客户端在后续请求回传的**会话粘性状态**，用于连续性/缓存亲和。其值为 **Fernet 认证加密 token**：

```
[1B 版本 0x80][8B 大端 Unix 时间戳][16B IV][160B 密文][32B HMAC-SHA256] = 217 字节（base64url 后 292 字符）
```

- 内嵌时间戳可用于估算有效期（本项目默认 TTL 55 分钟）；
- HMAC 覆盖全部前置字段：**任何字节级改动（包括改时间戳）都会破坏签名**，密钥在上游侧，本地无法重新生成合法 token；
- 因此覆写策略是「**复用一个曾由上游签发的真实值**」，而不是改造/伪造。

### 探测轨状态机

```
idle ──(剩余 TTL ≤ 窗口)──► probing
probing ──(验收通过: 模型一致 + 292B)──► store（原子热切换，清除失败标注）──► idle
probing ──(单轮达到 max-attempts)──► failed（标注: 次数/轮数/最后错误/时间）
failed ──(下一次扫描/成功获取)──► idle / store
```

### 覆写决策链

```
请求命中 models 前缀匹配?
 ├─ 否 → 不处理
 └─ 是 → 有未过期探测值?
         ├─ 是 → 覆写为探测值 (记录 applied)
         └─ 否 → 有配置 value?
                 ├─ 是 → force? 覆写(applied-config) : 客户端已有则跳过(skipped-existing)
                 └─ 否 → 不处理
```

---

## 验收标准与长度说明

### 当前实现

探测成功 = **HTTP 200 + 模型响应一致 + 长度恰好 292 字节**（三条缺一不可）。
非 292（如 312）视为疑似风控降级态，判定失败并继续重试换 IP。

### ⚠️ 为什么 292 需要按你的环境调整

- 292 字节 = 217 字节 Fernet 结构（160 字节密文恰好是 16 的倍数），这是**本项目观测到的健康值形态**；
- 我们曾在同一模型上观察到 **312 字节**（224 字节解码、密文非 16 倍数，结构异常），且出现时上游行为异常——因此判断为**风控/降级态**；
- **不能保证所有账号/套餐的有效字段都是 292 字节**。如果你在自己的环境观测到不同长度始终一致地伴随正常响应，请调整验收规则。

### 如何调整

编辑 `src/probe.go` 中的常量并重新构建：

```go
const probeRequiredStateLength = 292   // 改为你的有效长度
```

---

## 管理面板

CPA 管理页 → 插件菜单「O/对抗插件」，两个标签页：

**请求记录**
- 每请求：时间（洛杉矶）、模型、上游响应模型（不一致打标）、时区处理、覆写状态徽标（已覆写 / 已覆写（配置值）/ 保留原值）、Turn-State 长度与来源。

**状态路由台**
- 模型状态值：长度彩色徽标（292 绿 / 非 292 橙）、来源、生成/到期时间、剩余 TTL 进度条；
- 失败标注：红色「重试 N 次失败」徽标（悬停查看完整错误）；
- 探测轨迹：时间 / 模型 / 出口 / 结果 / 耗时 / 说明（含上游模型名，便于核对一致性校验）；
- 顶部：探测轨状态胶囊、优先模型、探测成功数、最近活动。

---

## 常见问题

**Q1：为什么我固定了一个值，上游有时表现异常（延迟高、状态不再更新）？**
固定值可能与当前会话/时间窗不匹配。上游对「合法但过期」的值有时容忍、有时拒绝。建议开启探测轨以持续获得新鲜值。

**Q2：312 字节是什么？**
疑似风控降级态（模型响应一致但状态结构异常）。本插件的验收规则会拒绝它并继续重试。

**Q3：能自己改时间戳让它"更新鲜"吗？**
不能。HMAC 覆盖全部字段，任何改动都会验签失败。能通过校验的只有上游自己签发的 token。

**Q4：探测消耗什么？**
使用与业务相同的账号配额，单次为最小请求（几十 tokens 级）；仅在有效期窗口内按需触发。

**Q5：需要几个出口？**
一个也可以（默认 `direct`）。多出口的价值在于：某个出口拿到降级/无效状态时轮换到另一个出口重试。

**Q6：会不会影响已有业务流量？**
探测轨完全独立、不经 CPA 主链路；覆写逻辑只对配置的模型生效，可随时用 `enabled:false` 关闭。

**Q7：支持哪些平台？**
本插件为 Go c-shared 动态库，与平台无关：Linux `.so` / Darwin `.dylib` / Windows `.dll`（按目标平台构建）。

---

## 开发与测试

```bash
make vet            # go vet
make test           # 单元测试（无需 CPA）
make integration    # 隔离集成测试（需 CPA_INTEGRATION_BINARY + CPA_INTEGRATION_PLUGIN）
```

集成测试会启动一个真实的 CPA 进程与 mock 上游，覆盖：时区规范化、模型一致性、流式/非流式观测、覆写到达上游、面板鉴权等场景。

目录结构：

```
src/
├── plugin.go          # 插件注册、方法路由、管理 API
├── observer.go        # 观测扩展、覆写引擎、配置解析
├── probe.go           # 探测轨引擎（调度/代理池/验收/重试）
├── normalize.go       # 时区规范化（原功能）
├── abi.go             # CPA C ABI 桥接
├── *_test.go          # 单元 + 集成测试
├── web/index.html     # 管理面板（内嵌）
├── cmd/probetest/     # 一次性探测试验工具
└── scripts/           # 运维辅助脚本（状态查看/记录导出/token 分析）
```

---

### 状态快照（持久化）

- 插件将面板状态（探测值、失败标注、疑似计数、暂停列表、降智拒绝开关、探测计数、探测轨迹与请求记录）定期写入本地 JSON 快照（原子写、权限 0600），插件重载或容器重启后自动恢复，无需重新设置。
- 默认路径：`/CLIProxyAPI/logs/.plugins/timezone-override/state.json`（可用环境变量 `LKS_TZ_STATE_FILE` 覆盖，测试用）。
- 写入频率：状态变化后最迟 `15s` 落盘一次；`plugin.shutdown` 时立即终轮一次。
- 容量有界（200 条请求记录 + 200 条探测轨迹 + 每模型 4096 字节状态值），典型快照 < 1MB。

---

## 合规与免责声明

- 本项目仅供**技术研究与学习**，用于调试自有的网关与账号环境；
- 使用前请确认符合所在国家/地区法律法规及上游服务条款；
- 拼改、伪造或滥用会话状态可能违反上游条款并导致账号风控，所有风险由使用者自行承担；
- 请妥善保管你的凭据文件（auth JSON / 代理凭据），不要提交到任何仓库。

---

## License

[MIT](LICENSE) © 2026 FlashyyL
