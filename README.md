# 本地按需预览环境控制面（Ephemeral Environment Control Plane）

一个**完全本地运行**、不依赖任何外部服务或真实凭据的“预览环境”控制面。
它用进程内状态模拟环境从申请、排队、启动、运行、停止到销毁/过期回收的完整
生命周期，目标是让**并发申请、租约到期、启动失败**等情况下的资源占用始终
可预期、可解释。

- 语言/运行时：Python ≥ 3.14，包管理用 [uv](https://github.com/astral-sh/uv)
- 框架：FastAPI（仅本地 HTTP 封装），状态全部在内存中
- 并发安全：所有**状态变更**在同一把 `RLock` 临界区内完成；唯独「供应（启动）」
  这一步在**锁外**的独立工作线程上执行，因此一次耗时数秒甚至卡死的启动
  **不会拖住整个控制面**
- 供应（provision）是可注入的本地模拟函数，默认立即成功；测试中可令其失败/阻塞/慢启动
- 启动有**时间上限** `start_timeout_seconds`：超时判定由发起启动的调用自身在
  上限到点时完成（不依赖是否有别的请求恰好先跑过、也不依赖后台 reaper），
  按失败回收槽位（原因可区分）、队头按 FIFO 补位，环境之后仍可重新启动；
  即使供应**卡住永不返回**，调用方也会在上限到点时拿到 `start_timeout`，
  不会被一直拖住；属主也可在启动进行中**主动中止**

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
| `STARTING` | 是 | 正在执行本地供应（模拟启动，可能耗时数秒）；启动在锁外进行，期间控制面对其它请求仍快速可用 |
| `RUNNING` | 是 | 供应成功，可访问 |
| `STOPPED` | 否 | 属主主动停止，释放计算资源，可再次 `start` |
| `FAILED` | 否 | 启动未成功，槽位已回滚释放，可重试；`fail_kind` 区分 `provisioning_error` / `start_timeout` / `start_aborted` |
| `EXPIRED` | 终态 | 租约到期被回收（后台 reaper 或访问时惰性回收） |
| `CANCELED` | 终态 | 排队中被属主取消 |
| `REJECTED` | 终态 | 申请时槽位与队列均满，被拒绝（保留记录可查询） |
| `DESTROYED` | 终态 | 属主显式销毁 |

合法迁移（简化）：

```
apply ─► QUEUED ──(有空槽, FIFO 准入)──► READY ──start/provision ok──► RUNNING
           │                              ▲  │                         │
           │                              │  ├─provision 报错──────► FAILED
           │                              │  ├─超过启动时间上限(超时)─► FAILED
           │                              │  └─属主 abort 主动中止──► FAILED
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
- **并发保证**：申请、准入、销毁、过期回收、启动失败/超时/中止回滚都在同一把锁内
  完成；只有供应（启动）这一可能很慢的步骤在锁外执行，但进入/提交启动仍在锁内，
  因此高并发 burst 下，“准入数 == capacity、排队数 == queue_limit、其余拒绝”恒成立，
  槽位占用永远不会超过上限；慢启动期间控制面对其他请求依旧快速可用。

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

### 慢启动、启动时间上限与主动中止

**慢启动不拖垮控制面。** 真实环境启动可能耗时数秒甚至卡住。供应这一步在
**全局锁之外**的独立工作线程上执行：一旦进入 `STARTING`，该环境就
**如实占着自己拿到的槽位**（不会被当成空闲再分给别人），但供应本身不持有锁。
因此启动进行期间，其他用户：

- 照样能 `apply`（容量满则进 FIFO 队列）；
- 照样能立刻 `GET /capacity` 看到当前占用（该 `STARTING` 槽位计入 `used_slots`）；
- 照样能快速查询其他环境的状态（`GET /environments/{id}`）。

这些请求都在很短时间内返回，不会与那一次慢启动互相等待。

**启动时间上限（`start_timeout_seconds`，默认 30s）。** 每次 `start` 进入
`STARTING` 时记录截止时间 `start_deadline`。超过上限仍未完成的启动按**失败**处理：

- 环境置 `FAILED`、`fail_kind = start_timeout`、`fail_reason` 以 `startup_timeout:`
  开头；槽位立即释放，等待队列队头按原 FIFO 顺序补位；
- **保证范围**：是否判超时、是否回收槽位，**不取决于有没有别的并发请求恰好
  先跑过、也不取决于后台 reaper 是否在跑**。发起启动的调用自身会等到上限到点：
  供应若仍未结束，这次调用当场把该次启动判为超时（释放槽位、队头补位）并向
  调用方抛出 `start_timeout`；供应若在上限之后才结束，提交时同样按超时处理，
  绝不会被当成成功。后台 reaper 周期扫描与其它请求前的惰性回收只是额外的
  兜底途径（例如调用方自己消失了），测试用 `FakeClock` 拨钟即可确定性触发；
- **启动卡住时调用方的表现**：即使供应函数永不返回，`start` 调用也会在上限
  到点时返回 `start_timeout`（HTTP 504），不会被那次卡死的启动一直拖着；
  卡住的供应线程只是锁外的一个守护线程，不占锁、不占槽位，控制面在此期间
  对其它申请/容量查询/状态查询照常快速响应；
- 超时只是一次**启动失败**而非环境报废：供应恢复、重新拿到槽位后可再次
  `start` 成功进入 `RUNNING`。
- 说明：本地模拟的供应函数本身不可被强杀，因此超时回收的是**槽位与状态**；
  即使那个卡住的供应线程稍后才返回，其结果也会因为「启动代号（`start_attempt`）
  已过期」而被丢弃，绝不会把新状态覆盖错。

**主动中止启动（`POST /environments/{id}/abort`）。** 属主在 `STARTING` 期间
（比如发现申请填错了）可主动掐掉本次启动：

- 置 `FAILED`、`fail_kind = start_aborted`、`fail_reason` 以 `start_aborted:`
  开头；同样立即释放全部槽位占用并让队头补位；
- 正在等待的那个 `start()` 调用会收到 `409 start_aborted`，而不是超时或 502；
- 之后该环境仍可重新启动。

**三种启动失败明确可区分：**

| 情况 | `fail_kind` | `fail_reason` 前缀 | 对 start 调用方的错误码 | HTTP |
| --- | --- | --- | --- | --- |
| 供应过程自身报错 | `provisioning_error` | 原始错误信息 | `provisioning_failed` | 502 |
| 超过启动时间上限 | `start_timeout` | `startup_timeout:` | `start_timeout` | 504 |
| 属主主动中止 | `start_aborted` | `start_aborted:` | `start_aborted` | 409 |

**对 abort 的拒绝语义（与现有状态/凭证语义一致，且原因可区分，不算成功）：**

- 目标不在 `STARTING`：`409 illegal_transition`，`details.from_state` 给出实际状态，
  `details.reason = only_starting_requests_can_abort`（排队中请用 `cancel`，
  已运行请用 `stop`）；
- 越权（用别人的 token）→ `403 env_access_denied`；伪造/缺失 token → `401 invalid_token`；
- 凭证租约恰好在中止前到期的**当次**调用 → `401 token_expired`（环境被回收为
  `EXPIRED`），与「中止成功」区分开；
- 已销毁 → `410 env_destroyed`；已过期/已取消 → `410 env_expired` / `env_canceled`。

### 启动失败 / 中断一致性（保留语义）

- 进入 `STARTING` 即占槽；供应抛错时回滚为 `FAILED`、累计 `failure_count`、
  记录 `fail_reason`、释放槽位并提升队头。
- 因此「启动报错 / 超时 / 被中止」都不会残留占用，也不会出现状态与占用不一致；
  供应恢复后可对 `FAILED` 重试成功进入 `RUNNING`。

---

## 5. HTTP 接口

所有环境级接口需请求头 `X-Auth-Token: <token>`。

| 方法 & 路径 | 说明 |
| --- | --- |
| `POST /environments` | 申请（body: `owner`,`lease_seconds?`,`idempotency_key?`,`fingerprint?`） |
| `POST /environments/{id}/start` | 启动/供应；容量不足时返回 `disposition=queued`；供应在锁外执行，慢启动不阻塞控制面 |
| `POST /environments/{id}/abort` | 主动中止进行中（`STARTING`）的启动；释放槽位、原因可区分 |
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
`env_canceled` / `capacity_exhausted` / `provisioning_failed` / `start_timeout`
(504) / `start_aborted` (409)。

`FAILED` 环境的响应体 `environment.fail_kind` 进一步区分
`provisioning_error` / `start_timeout` / `start_aborted`，
`environment.fail_reason` 带对应的人类可读前缀。

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
  供应被异常中断**过程中**控制面仍可即时读取且占用一致。
- `tests/test_slow_start.py`：慢启动期间控制面仍快速可用（申请/容量/他人状态）
  且 `STARTING` 槽位不被重复分配；**真实墙钟**下单次启动超过上限（期间没有任何
  其它请求或触发介入）即被判超时、释放槽位、FIFO 队头补位且之后能重启；
  启动**卡住永不返回**时调用方在上限到点拿到 `start_timeout` 而不会一直阻塞，
  期间其它请求照常快速返回、卡住供应的迟到结果被丢弃；超过启动时间上限也会被
  维护扫描/reaper 回收并让队头补位；属主主动中止后无残留占用且三种失败原因
  （报错/超时/中止）可区分；对非启动中、越权、伪造凭证、凭证过期、已销毁等
  中止请求按既有语义拒绝。
- `tests/test_queue_cancel.py`：取消排队即时释放名额。
- `tests/test_http_api.py`：通过 FastAPI TestClient 走完整 HTTP 生命周期与 503 拒绝，
  以及慢启动期间 `POST .../abort` 的 200/409 行为。

测试用可注入的 `FakeClock`/`SystemClock` 与可注入 `provisioner` 做确定性模拟，
不访问网络、不使用真实凭据；慢启动用线程 + `threading.Event` 制造真实阻塞。
超时路径有两类用例：真实墙钟（`SystemClock`）下验证「单次启动超上限即被判超时、
卡死启动的调用方在上限到点返回」，不依赖任何其它请求或 reaper；以及拨
`FakeClock` + `maintain()` 确定性验证维护扫描/reaper 这条兜底回收途径。

### 手工验证示例

```bash
uv run uvicorn main:app --port 8000

# 申请（记下 token 与 env_id）
curl -s -X POST localhost:8000/environments -H 'Content-Type: application/json' \
  -d '{"owner":"alice","lease_seconds":60}'

# 启动
curl -s -X POST localhost:8000/environments/$ID/start -H "X-Auth-Token: $TOKEN"

# 启动进行中（慢启动）主动中止
curl -s -X POST localhost:8000/environments/$ID/abort -H "X-Auth-Token: $TOKEN"

# 查看为什么排队/被拒绝/启动失败（含 fail_kind / fail_reason）
curl -s localhost:8000/environments/$ID/explain

# 容量快照
curl -s localhost:8000/capacity
```

慢启动 + 超时回收可用一小段 Python 直观验证（真实墙钟；不开 reaper、不发任何
其它请求，超时判定由 `start` 调用自身在上限到点时完成）：

```python
import threading, time
from controlplane.service import ControlPlane
from controlplane.errors import StartTimeoutError

hang = threading.Event()  # 永不置位：模拟卡死的启动
plane = ControlPlane(capacity=1, start_timeout_seconds=0.3,
                     provisioner=lambda env: hang.wait())

a = plane.apply("alice")
w = plane.apply("bob")                      # 容量满 -> 排队
t0 = time.perf_counter()
try:
    plane.start(a.environment.env_id, a.environment.token)
except StartTimeoutError:
    pass
print(round(time.perf_counter() - t0, 2))   # ≈0.3：上限到点即返回，不被卡死拖住
env = plane.store.get_locked(a.environment.env_id)
print(env.state, env.fail_kind)             # EnvState.FAILED start_timeout
print(plane.capacity_view().used_slots)     # 1：槽位已让给补位的 bob
print(plane.store.get_locked(w.environment.env_id).state)  # EnvState.READY
```

---

## 7. 代码结构

```
controlplane/
  errors.py    # 每种失败原因一个稳定错误码（含 HTTP 状态）
  models.py    # 状态枚举、Environment 实体、结果对象、时钟抽象
  store.py     # 内存状态容器：锁、token/幂等索引、FIFO 队列
  service.py   # 生命周期/准入 FIFO/租约回收/启动超时与中止/凭证校验；
               #   状态变更在锁内，供应在锁外的守护工作线程上执行，
               #   start 调用自身在上限到点时判超时，靠 start_attempt 代号提交结果
  api.py       # FastAPI 路由（含 POST .../abort）与统一错误响应
main.py        # uvicorn 入口：main:app
tests/         # 上述场景测试（判定与最终状态打印到日志）
```
