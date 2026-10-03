# 本地按需预览环境控制面（Ephemeral Environment Control Plane）

一个**完全本地运行**、不依赖任何外部服务或真实凭据的“预览环境”控制面。
它用进程内状态模拟环境从申请、排队、启动、运行、停止到销毁/过期回收的完整
生命周期，目标是让**并发申请、租约到期、启动失败**等情况下的资源占用始终
可预期、可解释。

- 语言/运行时：Python ≥ 3.14，包管理用 [uv](https://github.com/astral-sh/uv)
- 框架：FastAPI（仅本地 HTTP 封装），状态全部在内存中
- 并发安全：所有状态变更在同一把 `RLock` 临界区内完成
- 供应（provision）是可注入的本地模拟函数，默认立即成功；测试中可令其失败/阻塞

---

## 1. 快速开始

```bash
uv sync                       # 安装依赖
uv run uvicorn main:app --port 8000
```

申请一个环境：

```bash
curl -s -X POST http://127.0.0.1:8000/environments \
  -H 'Content-Type: application/json' \
  -d '{"owner":"alice","lease_seconds":60,"idempotency_key":"k1","fingerprint":"fp"}'
```

响应里包含 `environment`（含仅展示一次的访问凭证 `token`）、`disposition`
（`admitted` / `queued` / `rejected`）、稳定可机读的 `reason` 以及当前容量快照。

---

## 2. 环境生命周期状态

| 状态 | 是否占槽 | 含义 |
| --- | --- | --- |
| `QUEUED` | 否 | 容量已满，在 FIFO 等待队列中排队 |
| `READY` | 是 | 已分配槽位、租约开始计时，等待显式 `start` |
| `STARTING` | 是 | 正在执行本地供应（模拟启动，可能耗时数秒）；启动在独立线程进行，**不阻塞控制面** |
| `RUNNING` | 是 | 供应成功，可访问 |
| `STOPPED` | 否 | 属主主动停止，释放计算资源，可再次 `start` |
| `FAILED` | 否 | 启动未成功（供应报错 / 超过启动时限 / 被主动中止），槽位已回滚释放，可重试 |
| `EXPIRED` | 终态 | 租约到期被回收（后台 reaper 或访问时惰性回收） |
| `CANCELED` | 终态 | 排队中被属主取消 |
| `REJECTED` | 终态 | 申请时槽位与队列均满，被拒绝（保留记录可查询） |
| `DESTROYED` | 终态 | 属主显式销毁 |

合法迁移（简化）：

```
apply ─► QUEUED ──(有空槽, FIFO 准入)──► READY ──start/provision ok──► RUNNING
           │                              ▲  │                         │
           │                              │  └─供应报错/超时/被中止──► FAILED
           │                              │  │   (三者 fail_reason 可区分，均可重试)
           │                              │  │                         │
           │                              │  └─(STOPPED/FAILED 重启)──┘
           │                              │
           ├─cancel─► CANCELED            └─READY/RUNNING──stop──► STOPPED
           └─(满)─► REJECTED
   READY/STARTING/RUNNING ──destroy──► DESTROYED
   READY/STARTING/RUNNING ──租约到期──► EXPIRED（槽位释放，队头自动补位）
```

被拒绝的非法迁移不会一律返回同一种错误，而是带**稳定错误码**（见第 5 节）。

### 幂等与非法迁移

- 相同 `idempotency_key` + 相同 `owner/lease_seconds/fingerprint` 的重复 `POST`
  是幂等的：返回同一条记录、`deduplicated=true`，**不会重复占槽或重复建记录**。
- 同一 `idempotency_key` 但请求体不同 → `409 illegal_transition`
  （`reason = idempotency key reused with a different request`）。
- 对 `RUNNING` 重复 `start` 是幂等的（返回 `already_running`，不重复供应）。
- 对终态环境再操作，按终态原因区分：`env_destroyed` / `env_expired` /
  `env_canceled`（HTTP 410，错误码不同）。
- 其它非法迁移返回 `409 illegal_transition`，并在 `details.reason` 说明具体原因，
  例如 `use_cancel_while_queued`、`only_queued_requests_can_cancel`、
  `provisioning_in_progress`。

---

## 3. 并发与容量策略

- **固定槽位上限**：`capacity`（默认 2）。仅 `READY/STARTING/RUNNING` 占槽。
- **准入规则（明确且稳定）**：
  1. 申请先进入 `QUEUED` 并登记单调递增的 `sequence`；
  2. 若当前有空槽则立即按 **FIFO（`sequence` 升序）** 准入为 `READY`；
  3. 无空槽则排队，等待队列长度上限 `queue_limit`（默认 4）；
  4. 槽位与队列都满时**立即拒绝**：返回 `503 capacity_exhausted`，同时落一条
     `REJECTED` 记录，可随时通过 `GET /environments/{id}/explain` 查询被拒原因。
- **不抢占、不饥饿**：只在有释放（destroy/stop/失败/过期）时让队头补位，
  后来者不能插队。
- **取消即时释放**：对 `QUEUED` 调 `cancel` 立即腾出队列名额；随后新的申请即可排队。
- **可解释性**：任何申请都能说明为什么被准入/排队/拒绝：
  - 排队：`waiting_for_slot (fifo position N, x/C slots in use)`
  - 拒绝：`all C slots in use and wait queue full (limit Q)`
- **并发保证**：所有*状态变更*（申请、准入、销毁、过期回收、超时/中止/失败
  回滚）都在同一把锁的临界区内完成；耗时的供应本身在锁外独立线程执行，仅在
  结算结果时短暂取锁。高并发 burst 下，“准入数 == capacity、排队数 ==
  queue_limit、其余拒绝”恒成立，槽位占用永远不会超过上限；慢启动期间其他
  请求也不会被拖住（见第 4 节“慢启动、启动时限与主动中止”）。

---

## 4. 租约、回收与访问凭证

### 租约与回收

- 每个环境有**独立租约** `lease_seconds`，在获得槽位（进入 `READY`）时开始计时，
  `expires_at = accepted_at + lease_seconds`。
- `STOPPED` / `FAILED` 不计时（`expires_at` 置空）；重新启动成功后获得**全新租约**。
- 到期回收途径：
  - 后台守护线程 **lease reaper**（FastAPI lifespan 启动/停止，默认每 0.1s 扫描）；
  - 或在任意写操作前、以及凭证访问发现租约已过时**惰性回收**。
- 回收后状态置 `EXPIRED`、释放槽位，并立刻让等待队列队头补位。

### 访问凭证

- 每个环境在申请时生成独立随机凭证 `token`（`secrets` 生成，非真实凭据）。
- 所有环境级操作都必须带 `X-Auth-Token`，凭证**只对所属环境有效**：
  - 缺失/无法识别 → `401 invalid_token`
  - 有效凭证但访问的不是自己的环境（含不存在的 env id）→ `403 env_access_denied`
  - env id 与 token 都未知 → `404 env_not_found`（避免用坏 token 探测 env id）
  - 租约到期**当次**使用 → `401 token_expired`（同时完成回收并释放槽位），
    之后再用 → `410 env_expired`
  - 已销毁环境的凭证 → `410 env_destroyed`

### 慢启动、启动时限与主动中止

真实环境启动可能要好几秒甚至卡住，因此**供应在独立守护线程中执行，
不持有控制面状态锁**：

- **慢启动期间控制面仍然可用**：某个环境处于 `STARTING` 时，其他属主的申请、
  `GET /capacity`、其他环境的状态查询/解释都在毫秒级返回；`STARTING` 的环境
  **如实占着它已拿到的槽位**——不会被当成空闲再次分配，队头也不会提前补位
  （`used_slots` 包含它，`free_slots` 仍为 0）。发起 `start` 的那个调用本身
  会阻塞到供应结束、超时或被中止，这是预期行为。
- **启动时间上限（start timeout）**：`ControlPlane(default_start_timeout_seconds=…)`
  可设全局限额（默认 `None`，不设限，兼容旧行为）；单次调用也可用
  `plane.start(env_id, token, timeout_seconds=…)` 覆盖（传 `None` 表示本次不设限）。
  超过上限仍未完成，本次启动按失败处理：环境置 `FAILED`、槽位释放、队头按原
  FIFO 顺序补位；`fail_reason` 记录稳定值 **`startup_timeout`**，错误码
  **`startup_timeout`**（HTTP 504），与供应过程自己抛错的
  `provisioning_failed`（HTTP 502）区分。
  - 即使发起 `start` 的客户端已经断开，后台 **lease reaper 与任意加锁写操作**
    也会在进入临界区时扫描超过时限的 `STARTING` 尝试并回收，所以“彻底卡死”
    的启动也不会永久占槽。
- **超时后可以重启**：超时只结束*这一次*启动，环境回到与普通启动失败相同的
  `FAILED`（不占槽、可 `start` 重试；槽满时按既有语义先排队）。
- **主动中止（abort）**：属主可对 `STARTING` 中的环境调用
  `POST /environments/{id}/abort`：立即释放槽位、队头补位，`fail_reason`
  记录稳定值 **`aborted_by_owner`**，错误码 **`startup_aborted`**（HTTP 409），
  与超时、供应报错三者互不混淆。中止后环境同样可重新 `start`。
  若供应器愿意配合，可声明第二个参数接收一个 `threading.Event`，中止时该事件
  会被 set（best-effort 协作式取消）；不配合也不影响槽位与状态的正确性。
- **中止的拒绝语义（按既有规则，绝不算作成功）**：
  - 对非 `STARTING` 环境 → `409 illegal_transition`，`details.reason` 按状态区分：
    `READY` → `not_starting_use_stop`、`RUNNING` → `already_running`、
    `STOPPED` → `not_starting`、`FAILED`（含之前已中止过）→ `already_failed`、
    `QUEUED` → `use_cancel_while_queued`；
  - 若本次启动恰好在 abort 到达前一刻越过时限，按超时处理 →
    `504 startup_timeout`，不会被记成一次成功的中止；
  - 凭证不属于该环境 → `403 env_access_denied`；无法识别/缺失凭证 →
    `401 invalid_token`；租约已过期 → `401 token_expired`（当次）/
    `410 env_expired`（之后）；已销毁 → `410 env_destroyed`。

### 启动失败 / 中断一致性

- 进入 `STARTING` 即占槽；供应抛错、超时、被中止都会在状态锁临界区内回滚为
  `FAILED`、累计 `failure_count`、记录可区分的 `fail_reason`、释放槽位并提升
  队头。三种失败原因在 `GET /environments/{id}/explain` 中同样可区分：
  - 供应报错：`provisioning_failed: <异常信息>`（错误码 `provisioning_failed`）
  - 启动超时：`startup_timeout: start exceeded the configured time limit`
    （错误码 `startup_timeout`）
  - 主动中止：`startup_aborted: start aborted by owner`
    （错误码 `startup_aborted`）
- 所有状态结算都在状态锁内按“环境是否仍为 `STARTING`”判定：超时/中止/销毁/
  过期之后才姗姗返回的供应线程结果会被丢弃，**终态不会被迟到的供应结果覆盖**，
  因此慢启动、异常中断、超时、中止、并发操作下不会残留占用，也不会出现状态与
  占用不一致。供应恢复后可对 `FAILED` 重试成功进入 `RUNNING`。

---

## 5. HTTP 接口

所有环境级接口需请求头 `X-Auth-Token: <token>`。

| 方法 & 路径 | 说明 |
| --- | --- |
| `POST /environments` | 申请（body: `owner`,`lease_seconds?`,`idempotency_key?`,`fingerprint?`） |
| `POST /environments/{id}/start` | 启动/供应；容量不足时返回 `disposition=queued`；启动超时返回 504 |
| `POST /environments/{id}/abort` | 主动中止 `STARTING` 中的启动（释放槽位，可重启） |
| `POST /environments/{id}/stop` | 停止（释放槽位，可重启） |
| `POST /environments/{id}/destroy` | 销毁（终态） |
| `POST /environments/{id}/cancel` | 取消排队（仅 `QUEUED`） |
| `GET  /environments/{id}` | 查询属主视角状态 |
| `GET  /environments/{id}/explain` | 无需凭证，解释为何准入/排队/拒绝 |
| `GET  /capacity` | 容量与队列快照 |

错误响应统一形如：

```json
{
  "error": {
    "code": "illegal_transition",
    "message": "illegal transition: stop from QUEUED (use_cancel_while_queued)",
    "details": {"from_state": "QUEUED", "action": "stop",
                "reason": "use_cancel_while_queued"}
  }
}
```

错误码一览：`env_not_found` / `invalid_token` / `token_expired` /
`env_access_denied` / `illegal_transition` / `env_destroyed` / `env_expired` /
`env_canceled` / `capacity_exhausted` / `provisioning_failed` /
`startup_timeout` / `startup_aborted`。

三种启动失败的区分：

| 触发原因 | HTTP / 错误码 | `fail_reason` | 环境状态 | 是否占槽 |
| --- | --- | --- | --- | --- |
| 供应过程自己抛异常 | 502 `provisioning_failed` | 原始异常信息 | `FAILED` | 否 |
| 超过启动时限 | 504 `startup_timeout` | `startup_timeout` | `FAILED` | 否 |
| 属主主动中止 | 409 `startup_aborted` | `aborted_by_owner` | `FAILED` | 否 |

---

## 6. 本地验证方法

### 运行测试（无需任何外部服务/真实凭据）

```bash
uv run pytest -s
```

`-s` 会打印每个场景的**判定依据与最终状态**。覆盖：

- `tests/test_concurrent.py`：25 并发抢 3 槽/队列 5，断言准入=3、排队=5、拒绝=17，
  FIFO 顺序稳定、拒绝可解释；以及 16 线程用同一幂等键只产生 1 条记录/1 个槽位。
- `tests/test_idempotency.py`：重复申请幂等、同键不同体冲突、终态/非法迁移错误码区分。
- `tests/test_lease.py`：到期释放并提升队头、停止期间不计时、后台 reaper 自动回收。
- `tests/test_credentials.py`：跨环境越权、伪造凭证、过期凭证、销毁后凭证失效。
- `tests/test_startup_failure.py`：失败不占槽且队头补位、恢复后可重试、
  供应被异常中断过程中占用一致。
- `tests/test_start_timeout_abort.py`：慢启动期间控制面持续快速可用且
  `STARTING` 槽位不被重复分配；启动超时自动回收（含无等待方时的后台扫描）、
  队头补位、超时后可重启；主动中止无残留占用且原因与超时/报错可区分；
  非启动中/越权/凭证过期/已销毁的中止全部按既有语义拒绝；启动过程中销毁、
  租约过期也能唤醒等待方并正确释放。
- `tests/test_queue_cancel.py`：取消排队即时释放名额。
- `tests/test_http_api.py`：通过 FastAPI TestClient 走完整 HTTP 生命周期与 503 拒绝。

测试用可注入的 `FakeClock`/`SystemClock` 与可注入 `provisioner` 做确定性模拟，
不访问网络、不使用真实凭据；仅有一个后台 reaper 用例使用真实单调时钟。

### 手工验证示例

```bash
uv run uvicorn main:app --port 8000

# 申请（记下 token 与 env_id）
curl -s -X POST localhost:8000/environments -H 'Content-Type: application/json' \
  -d '{"owner":"alice","lease_seconds":60}'

# 启动
curl -s -X POST localhost:8000/environments/$ID/start -H "X-Auth-Token: $TOKEN"

# 启动耗时较长时：另一个终端可以立即中止本次启动
curl -s -X POST localhost:8000/environments/$ID/abort -H "X-Auth-Token: $TOKEN"
# -> {"environment": {"state": "FAILED", "fail_reason": "aborted_by_owner", ...}}

# 查看为什么排队/被拒绝/失败（含 startup_timeout / startup_aborted 前缀）
curl -s localhost:8000/environments/$ID/explain

# 容量快照
curl -s localhost:8000/capacity
```

---

## 7. 代码结构

```
controlplane/
  errors.py    # 每种失败原因一个稳定错误码（含 HTTP 状态）
  models.py    # 状态枚举、Environment 实体、结果对象、时钟抽象
  store.py     # 内存状态容器：锁、token/幂等索引、FIFO 队列
  service.py   # 生命周期/准入 FIFO/租约回收/凭证校验/锁外异步供应、启动超时扫描与中止 + 后台 reaper
  api.py       # FastAPI 路由与统一错误响应
main.py        # uvicorn 入口：main:app
tests/         # 上述场景测试（判定与最终状态打印到日志）
```
