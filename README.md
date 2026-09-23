# 本地按需预览环境控制面（Python / FastAPI）

用**本地内存状态**模拟预览环境从申请到销毁的完整生命周期。纯本地运行，
**不依赖任何外部服务或真实凭据**；在并发申请、租约到期、启动失败/中断等
情况下，资源占用始终可预期、可解释。

## 能力概览

- **生命周期**：申请 → 排队/分配 → 启动 → 运行 → 停止/重启 → 销毁；
  非法状态迁移按不同原因拒绝（独立错误码，而非笼统的同一种失败）。
- **幂等申请**：以 `request_id` 为幂等键，重复提交不重复占槽、不产生重复记录。
- **固定容量**：槽位上限 + FIFO 有界等待队列；满了明确拒绝并留痕；
  并发申请下占用绝不越上限；排队取消立即释放位置；任何申请都能解释
  "为何排队 / 为何被拒绝"。
- **独立租约与凭证**：每个环境独立随机凭证，仅对本环境有效；越权、伪造、
  过期凭证均被区分拒绝；租约到期自动回收并释放槽位。
- **失败一致性**：启动失败或执行中断后自动回滚占用、触发队列补位，
  不留残留、不出现状态与占用不一致。

## 快速开始

```bash
uv sync

# 运行测试（-s 打印每个判定的依据与最终状态）
uv run pytest -s

# 启动本地服务
uv run uvicorn main:app --port 8000
# 可调容量/租约：
ECP_CAPACITY=2 ECP_MAX_QUEUE=2 ECP_LEASE_TTL=30 \
  ECP_REAPER_INTERVAL=1 uv run uvicorn main:app --port 8000
```

一次典型调用：

```bash
curl -s -X POST localhost:8000/environments -H 'Content-Type: application/json' \
  -d '{"request_id":"req-1","owner":"alice"}'
curl -s -X POST localhost:8000/environments/<env_id>/start -H 'X-Credential: <cred>'
curl -s localhost:8000/capacity
curl -s -X DELETE localhost:8000/environments/<env_id> -H 'X-Credential: <cred>'
```

## 代码结构

```
control_plane/
  models.py    # 状态枚举、Environment、Lease（纯类型）
  errors.py    # 每类失败原因一个独立异常 + 稳定 code
  manager.py   # 线程安全核心：槽位/队列/状态机/租约/凭证，模拟 provisioner
  api.py       # FastAPI HTTP 层，统一错误映射
main.py        # 服务入口（main:app）
tests/         # 并发/幂等/租约/越权/失败恢复/HTTP 端到端
docs/design.md # 状态机、容量策略、租约规则、错误码与本地验证方法
```

## 测试场景

- 并发申请不越上限、占用/排队/拒绝守恒；
- 重复申请幂等、同键不同 owner 冲突；
- 槽满排队、队列满拒绝、排队取消释放、槽位释放 FIFO 补位；
- 租约到期（主动/惰性）回收并补位、STOPPED 租约暂停；
- 越权访问、伪造/缺失/过期凭证的差异化拒绝；
- 启动失败与执行中断回滚无残留、无空槽重试被拒、释放后重试成功；
- 上述场景的 HTTP 端到端验证（TestClient，无外部依赖）。

详细状态迁移、错误码对照与设计依据见 [docs/design.md](docs/design.md)。
