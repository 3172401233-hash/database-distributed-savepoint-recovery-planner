# database-distributed-savepoint-recovery-planner

跨分片事务 savepoint 恢复规划器：在部分分片已回滚、另一些分片已提交、
协调器可能崩溃的情况下，依据统一的 savepoint 版本计算每个分片应恢复到
的位置，输出恢复顺序、冲突分支和不可恢复范围。

## 本地模型

**所有事务均在本地模型（local model）下运行**：

- 协调器、分片资源管理器、日志与提交记录全部是同一进程内的本地内存对象，
  不经过网络协议、远程锁服务或外部事务管理器。
- `build_recovery_plan` 是纯函数式计算，只读取传入的本地日志快照，
  相同输入必然得到相同的恢复计划，便于重复恢复与单元测试。
- 幂等回放器 `IdempotentReplayer` 的已应用集合（幂等键
  `(shard_id, op_id)`）也只保存在本地内存中，无外部依赖。
- 代码仅使用 Python 3.8.2 标准库（`dataclasses`、`enum`、`typing`）。

## 目录结构

- `savepoint_recovery/model.py`：日志条目、分支状态、回滚范围、提交记录、
  协调器决定与冲突常量。
- `savepoint_recovery/planner.py`：恢复规划核心，输出 `RecoveryPlan`
  （恢复顺序、每分片 UNDO/REDO 位置、冲突与不可恢复范围）。
- `savepoint_recovery/replay.py`：按规划顺序执行的幂等回放器。
- `tests/test_recovery.py`：部分回滚、同名 savepoint、协调器崩溃、
  重复恢复及不可恢复边界测试。

## 输入与输出

`build_recovery_plan` 接收六类输入：

1. `savepoint_id`：本次恢复锚定的 savepoint 标识。
2. `shard_logs`：每个分片的本地日志（`LogEntry` 列表，含 savepoint、
   操作、父事务提交、分支提交四类条目）。
3. `branch_states`：恢复开始时各分片分支的观测状态
   （active / committed / aborted 及已应用 LSN）。
4. `rollback_ranges`：已记录的回滚范围，`undone_lsns` 标注崩溃前
   已经撤销的操作，用于重复恢复。
5. `commit_records`：分片上已持久化的分支提交记录。
6. `coordinator_decisions`：协调器决定，支持分片专属/全局、
   持久化/非持久化，解析优先级为 分片持久化 > 全局持久化 >
   分片非持久化 > 全局非持久化。

输出 `RecoveryPlan`：

- `order`：恢复执行顺序，UNDO 分片先于 REDO 分片，先统一回到 savepoint
  基线再重放提交缺口；同阶段按分片名排序保证确定性。
- `shards`：每分片的阶段（UNDO/REDO/NONE）、目标 LSN、待撤销与待重放
  LSN 列表、冲突及是否可恢复。
- `conflicts`：冲突分支（决定与观测状态不一致、in-doubt 等）。
- `unrecoverable_ranges`：不可自动恢复的日志区间及原因。

## 关键恢复规则

- **统一 savepoint 版本**：同名 savepoint 以日志中最新一次出现为准；
  回滚范围若指向旧版本，报 `savepoint_version_mismatch` 并标记不可恢复。
- **父事务提交边界**：savepoint 之后若存在父事务提交日志，回滚会越过
  提交边界，报 `rollback_crosses_parent_commit`，禁止自动回滚。
- **提交与回滚冲突**：已持久化提交的分片不能按 abort 决定回滚，
  报 `commit_crossed_rollback` 并给出不可恢复区间。
- **协调器崩溃**：无任何有效决定时分支处于 in-doubt 状态，已提交的
  保留提交，未提交的保守回滚，并在冲突中显式标注 `in_doubt`。
- **幂等回放**：每个操作以 `(shard_id, op_id)` 去重，重复恢复时已应用
  的操作跳过；`undone_lsns` 保证崩溃后续跑不会重复撤销。

## 运行测试

```bash
python -m unittest
```

## 使用示例

```python
from savepoint_recovery import (
    BranchState, BranchStatus, CommitRecord, CoordinatorDecision,
    EntryKind, LogEntry, build_recovery_plan, IdempotentReplayer,
)

logs = {"s1": [
    LogEntry(10, EntryKind.SAVEPOINT, savepoint_id="sp-1"),
    LogEntry(11, EntryKind.OPERATION, op_id="a"),
]}
states = {"s1": BranchState("s1", BranchStatus.ACTIVE, 11)}
decisions = [CoordinatorDecision("abort", "sp-1", durable=True)]

plan = build_recovery_plan("sp-1", logs, states, [], [], decisions)
print(plan.order, plan.shards["s1"].undo_lsns)

report = IdempotentReplayer(logs).run(plan)
```
