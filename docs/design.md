# 本地按需预览环境控制面 — 设计说明

一套**纯本地内存状态**模拟的预览环境控制面：支持环境申请、启动、停止、
销毁，受固定槽位上限约束，带独立租约与访问凭证，在并发申请、租约到期、
启动失败等情况下保证"状态—占用"始终一致。**不依赖任何外部服务或真实凭据。**

## 1. 环境生命周期状态

| 状态 | 是否占槽 | 含义 | 可执行操作 |
|------|:---:|------|------|
| `PENDING` | 否 | 申请已受理，但槽位满，进入 FIFO 等待队列 | 取消（cancel） |
| `ALLOCATED` | 是 | 已占槽位，尚未启动 | start / destroy |
| `STARTING` | 是 | 启动副作用执行中（锁外执行） | 仅查询；stop/destroy 被拒 |
| `RUNNING` | 是 | 运行中，持有有效租约 | stop / destroy |
| `STOPPED` | 是 | 已停止但**保留槽位**，租约暂停 | start（重启）/ destroy |
| `FAILED` | 否 | 启动失败或被中断，占用已回滚 | start（重试，需重新抢槽）/ destroy |
| `DESTROYED` | 否 | 终态：主动销毁，凭证立即失效 | 无 |
| `EXPIRED` | 否 | 终态：租约到期被自动回收 | 无 |
| `CANCELLED` | 否 | 终态：排队等待期间被取消 | 无 |

合法迁移图：

```
PENDING ──cancel──► CANCELLED
PENDING ──(队首补位)──► ALLOCATED
ALLOCATED ──start──► STARTING ──成功──► RUNNING
                          └──失败/中断──► FAILED
RUNNING ──stop──► STOPPED ──start──► STARTING
RUNNING ──destroy──► DESTROYED
STOPPED/ALLOCATED/FAILED ──destroy──► DESTROYED
RUNNING ──租约到期(惰性/reap)──► EXPIRED
FAILED ──start(拿到空槽)──► STARTING
```

非法迁移一律拒绝，并按**不同原因返回不同错误码**（见第 5 节），不会笼统归为一种失败。

### 幂等性

- **申请**：以 `request_id` 为幂等键。重复提交相同 `(request_id, owner)`
  返回同一环境与同一结论，**绝不重复占用槽位或产生重复记录**。
- 相同 `request_id` 但不同 `owner` → `idempotency_conflict`（409）。
- 已排队的申请被重复提交，返回相同的队列位置。
- 被容量拒绝的申请重试同一 `request_id`，得到**相同的拒绝结论**。
- `start`/`stop` 在已是目标状态时幂等返回。

## 2. 并发与容量策略

- 固定槽位上限 `capacity`（默认 3，可用环境变量 `ECP_CAPACITY` 覆盖）。
- 固定等待队列上限 `max_queue`（默认等于 capacity，`ECP_MAX_QUEUE` 覆盖）。
- 申请处理顺序：
  1. 有空槽 → 立即 `ALLOCATED`，返回槽位号；
  2. 槽满但队列未满 → 进入 **FIFO 队列**，状态 `PENDING`，返回稳定的排队位置；
  3. 槽满且队列满 → **明确拒绝** `capacity_exhausted`（HTTP 503），
     并在拒绝台账中留痕，可随时通过 `GET /requests/{request_id}` 解释"为何被拒绝"。
- 槽位一旦被释放（销毁、租约回收、启动失败回滚），**队首立即补位**为
  `ALLOCATED`，严格 FIFO，不会插队。
- 排队取消（PENDING→CANCELLED）**立即释放队列位置**，使后续新申请可以入队；
  PENDING 不占槽，因此取消本身不改变槽位占用。
- 所有状态变更在单一 `threading.RLock` 下完成，因此并发申请下
  `occupied_slots ≤ capacity`、`queue_length ≤ max_queue` 是硬不变量，
  且恒有 `占用 + 排队 + 拒绝 = 提交总数`（测试中逐条断言）。
- 启动的外部副作用（模拟 provision）在锁外执行以避免长临界区；进入
  `STARTING` 前在锁内完成状态/槽位登记，因此并发重复启动同一环境时只有
  一个能成功，其余得到 `invalid_state_transition`，不会重复占槽或产生多个租约。

## 3. 租约与回收规则

- 每个环境在**成功进入 RUNNING 时**获得独立租约，TTL 默认 60s
  （`ECP_LEASE_TTL` 覆盖），响应中返回 `expires_at`。
- 租约**仅在 RUNNING 期间计时**：`STOPPED` 时租约清除并暂停计时，
  重新 `start` 时签发新租约；因此停止期间不会被回收。
- 到期回收有两条路径，结果一致：
  - **主动回收**：后台 reaper（`ECP_REAPER_INTERVAL>0` 时启动守护线程）或
    `POST /internal/reap`；
  - **惰性回收**：任何对该环境的访问/操作发现租约到期时立即回收。
- 回收动作：`RUNNING → EXPIRED`、清除租约、**释放槽位**、触发队首补位。
- 到期环境的凭证访问返回 `lease_expired`（HTTP 410），与"伪造凭证/越权"明确区分。

## 4. 凭证与访问控制

- 每个环境在申请受理时生成独立随机凭证（`cred_` + 12 字节十六进制，
  仅本地随机，非真实凭据），凭证与环境 **1:1 绑定**。
- 鉴权顺序（统一在管理器内执行）：
  1. 缺少凭证 / 凭证无法识别 → `invalid_credential`（401）；
  2. 凭证属于**另一个环境**（越权操作他人环境）→ `unauthorized`（403）；
  3. 环境已 DESTROYED/CANCELLED（凭证随终态失效）→ `credential_expired`/`invalid_credential`；
  4. 环境已 EXPIRED → `lease_expired`（410）；
  5. 状态不允许该操作 → `invalid_state_transition`（409）。
- 销毁时凭证映射立即删除，旧凭证无法再访问。

## 5. 错误码对照表

| code | HTTP | 触发场景 |
|------|:---:|------|
| `environment_not_found` | 404 | 环境/申请不存在 |
| `invalid_credential` | 401 | 缺凭证、伪造、销毁后旧凭证 |
| `unauthorized` | 403 | 凭证有效但操作他人环境 |
| `credential_expired` | 401 | 凭证随终态失效 |
| `lease_expired` | 410 | 租约到期被回收后访问 |
| `invalid_state_transition` | 409 | 当前状态不允许该操作 |
| `capacity_exhausted` | 503 | 槽与队列均满 |
| `already_cancelled` | 409 | 重复取消 |
| `startup_failed` | 502 | 启动失败/中断（占用已回滚） |
| `idempotency_conflict` | 409 | 相同 request_id 不同 owner |

## 6. 启动失败与中断的一致性

- provision 抛异常（模拟镜像拉取失败）或被中断（模拟执行中断）时：
  `STARTING → FAILED`、记录 `fail_reason`、**释放已占槽位**、触发队首补位，
  对外抛 `startup_failed`（502）。保证不残留占用、不出现状态/占用不一致。
- FAILED 环境可重试 `start`：重试需重新获取空槽；无空槽时得到
  `capacity_exhausted` 且仍保持 FAILED、不占槽；槽位释放后重试成功。

## 7. HTTP 接口

| 方法 | 路径 | 说明 |
|------|------|------|
| POST | `/environments` | 申请（body: `request_id`, `owner`），返回凭证 |
| POST | `/environments/{id}/cancel` | 取消排队（头 `X-Owner`） |
| POST | `/environments/{id}/start` | 启动（头 `X-Credential`） |
| POST | `/environments/{id}/stop` | 停止 |
| DELETE | `/environments/{id}` | 销毁 |
| GET | `/environments/{id}` | 查询（可带凭证做鉴权） |
| GET | `/capacity` | 容量/占用/排队视图 |
| GET | `/requests/{request_id}` | 解释申请当前状态与原因 |
| POST | `/internal/reap` | 主动回收到期环境 |
| GET | `/health` | 健康检查 |

错误统一形如：`{"error": {"code": "...", "message": "...", "env_id": "..."}}`。

## 8. 本地验证方法

```bash
# 安装依赖（uv）
uv sync

# 运行全部测试（判定依据与最终状态用 -s 打印到日志）
uv run pytest -s

# 只跑某类场景
uv run pytest tests/test_capacity.py -s           # 并发申请 / 幂等 / 排队取消
uv run pytest tests/test_lifecycle.py -s          # 非法迁移 / 越权
uv run pytest tests/test_lease_and_recovery.py -s # 租约到期 / 启动失败恢复
uv run pytest tests/test_api.py -s                # HTTP 端到端
```

手动起服务并用 curl 走一遍：

```bash
ECP_CAPACITY=2 ECP_LEASE_TTL=30 uv run uvicorn main:app --port 8000

curl -s -X POST localhost:8000/environments \
  -H 'Content-Type: application/json' \
  -d '{"request_id":"req-1","owner":"alice"}'
# 记下返回的 env_id 与 credential
curl -s -X POST localhost:8000/environments/<env_id>/start -H 'X-Credential: <cred>'
curl -s localhost:8000/capacity
curl -s -X DELETE localhost:8000/environments/<env_id> -H 'X-Credential: <cred>'
```

测试全部使用 `ManualClock`（时间只在显式推进时前进）与 `SimulatedProvisioner`
（可注入失败/中断/延迟），因此结果确定、可重复，不依赖时钟、网络或真实凭据。
