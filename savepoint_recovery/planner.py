"""根据日志与协调器决定计算每个分片的恢复位置。

规划器只做纯计算（本地模型）：读取分片日志、分支状态、回滚范围、
提交记录和协调器决定，输出确定性的恢复顺序、冲突分支与不可恢复范围。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional, Sequence, Set

from .model import (
    COMMIT_CROSSED_ROLLBACK,
    DECISION_MISMATCH,
    IN_DOUBT,
    ROLLBACK_CROSSES_PARENT_COMMIT,
    SAVEPOINT_NOT_FOUND,
    SAVEPOINT_VERSION_MISMATCH,
    BranchState,
    BranchStatus,
    CommitRecord,
    Conflict,
    CoordinatorDecision,
    EntryKind,
    LogEntry,
    RecoveryPhase,
    RollbackRange,
    ShardRecovery,
)


@dataclass(frozen=True)
class UnrecoverableRange:
    """无法自动恢复的日志区间。"""

    shard_id: str
    start_lsn: int
    end_lsn: int
    reason: str


@dataclass(frozen=True)
class RecoveryPlan:
    """一次 savepoint 恢复的完整规划结果。"""

    savepoint_id: str
    order: List[str]
    shards: Dict[str, ShardRecovery]
    conflicts: List[Conflict]
    unrecoverable_ranges: List[UnrecoverableRange]

    @property
    def recoverable(self) -> bool:
        return not self.unrecoverable_ranges


def build_recovery_plan(
    savepoint_id: str,
    shard_logs: Dict[str, Sequence[LogEntry]],
    branch_states: Dict[str, BranchState],
    rollback_ranges: Sequence[RollbackRange],
    commit_records: Sequence[CommitRecord],
    coordinator_decisions: Sequence[CoordinatorDecision],
) -> RecoveryPlan:
    """计算每个分片应恢复到的位置。

    参数即题目要求的六类输入；返回的 ``order`` 给出实际执行顺序，
    ``shards`` 按分片给出 UNDO/REDO 细节，冲突与不可恢复范围单列。
    """

    ranges_by_shard: Dict[str, List[RollbackRange]] = {}
    for rng in rollback_ranges:
        ranges_by_shard.setdefault(rng.shard_id, []).append(rng)

    commits_by_shard: Dict[str, CommitRecord] = {
        rec.shard_id: rec for rec in commit_records
    }

    recoveries: List[ShardRecovery] = []
    all_conflicts: List[Conflict] = []
    unrecoverable: List[UnrecoverableRange] = []

    for shard_id in sorted(shard_logs):
        log = list(shard_logs[shard_id])
        state = branch_states.get(
            shard_id,
            BranchState(shard_id, BranchStatus.ACTIVE, None),
        )
        recovery, shard_conflicts, shard_unrecoverable = _plan_shard(
            savepoint_id,
            shard_id,
            log,
            state,
            ranges_by_shard.get(shard_id, []),
            commits_by_shard.get(shard_id),
            coordinator_decisions,
        )
        recoveries.append(recovery)
        all_conflicts.extend(shard_conflicts)
        unrecoverable.extend(shard_unrecoverable)

    # 先回滚（统一回到 savepoint 基线）再重放，同阶段按分片名排序保证确定性。
    phase_rank = {
        RecoveryPhase.UNDO: 0,
        RecoveryPhase.REDO: 1,
        RecoveryPhase.NONE: 2,
    }
    ordered = sorted(
        recoveries,
        key=lambda item: (phase_rank[item.phase], item.shard_id),
    )

    return RecoveryPlan(
        savepoint_id=savepoint_id,
        order=[item.shard_id for item in ordered],
        shards={item.shard_id: item for item in recoveries},
        conflicts=all_conflicts,
        unrecoverable_ranges=unrecoverable,
    )


def _plan_shard(
    savepoint_id: str,
    shard_id: str,
    log: List[LogEntry],
    state: BranchState,
    rollback_ranges: List[RollbackRange],
    commit_record: Optional[CommitRecord],
    decisions: Sequence[CoordinatorDecision],
):
    conflicts: List[Conflict] = []
    unrecoverable: List[UnrecoverableRange] = []

    savepoint_entries = [
        entry
        for entry in log
        if entry.kind == EntryKind.SAVEPOINT
        and entry.savepoint_id == savepoint_id
    ]
    if not savepoint_entries:
        conflict = Conflict(
            shard_id,
            SAVEPOINT_NOT_FOUND,
            "分片日志中找不到 savepoint %s，缺少统一恢复版本" % savepoint_id,
        )
        conflicts.append(conflict)
        unrecoverable.append(
            UnrecoverableRange(shard_id, -1, -1, conflict.code)
        )
        return (
            ShardRecovery(
                shard_id, RecoveryPhase.NONE, -1, [], [], conflicts, False
            ),
            conflicts,
            unrecoverable,
        )

    # 同名 savepoint 以最新一次出现为准，旧版本视为已被释放。
    savepoint_entry = savepoint_entries[-1]
    target_lsn = savepoint_entry.lsn

    parent_commit = next(
        (
            entry
            for entry in log
            if entry.kind == EntryKind.PARENT_COMMIT
            and entry.lsn > target_lsn
        ),
        None,
    )
    if parent_commit is not None:
        conflict = Conflict(
            shard_id,
            ROLLBACK_CROSSES_PARENT_COMMIT,
            (
                "savepoint %s 的回滚会越过父事务提交边界 lsn=%d"
                % (savepoint_id, parent_commit.lsn)
            ),
        )
        conflicts.append(conflict)
        unrecoverable.append(
            UnrecoverableRange(
                shard_id, target_lsn, parent_commit.lsn, conflict.code
            )
        )
        return (
            ShardRecovery(
                shard_id,
                RecoveryPhase.NONE,
                target_lsn,
                [],
                [],
                conflicts,
                False,
            ),
            conflicts,
            unrecoverable,
        )

    # 回滚范围若指向同名 savepoint 的旧版本，说明恢复版本不统一。
    stale_range = next(
        (rng for rng in rollback_ranges if rng.target_lsn != target_lsn),
        None,
    )
    if stale_range is not None:
        conflict = Conflict(
            shard_id,
            SAVEPOINT_VERSION_MISMATCH,
            (
                "回滚范围指向同名 savepoint 的旧版本 lsn=%d，当前版本 lsn=%d"
                % (stale_range.target_lsn, target_lsn)
            ),
        )
        conflicts.append(conflict)
        unrecoverable.append(
            UnrecoverableRange(
                shard_id,
                stale_range.target_lsn,
                stale_range.end_lsn,
                conflict.code,
            )
        )
        return (
            ShardRecovery(
                shard_id,
                RecoveryPhase.NONE,
                target_lsn,
                [],
                [],
                conflicts,
                False,
            ),
            conflicts,
            unrecoverable,
        )

    decision = _effective_decision(shard_id, decisions)
    committed = (
        commit_record is not None
        or state.status == BranchStatus.COMMITTED
    )

    operations_after = [
        entry
        for entry in log
        if entry.kind == EntryKind.OPERATION and entry.lsn > target_lsn
    ]

    if decision is None:
        # 协调器崩溃且无任何持久化决定：in-doubt 分支采用保守启发式，
        # 已提交的保留提交，其余按 abort 回到 savepoint，并显式标注冲突。
        conflicts.append(
            Conflict(
                shard_id,
                IN_DOUBT,
                "协调器崩溃且无持久化决定，分片 %s 处于 in-doubt 状态" % shard_id,
            )
        )
        effective = "commit" if committed else "abort"
    else:
        effective = decision.decision.lower()
        observed = state.status
        if (
            effective == "commit"
            and observed == BranchStatus.ABORTED
        ) or (
            effective == "abort"
            and observed == BranchStatus.COMMITTED
        ):
            conflicts.append(
                Conflict(
                    shard_id,
                    DECISION_MISMATCH,
                    (
                        "协调器决定 %s 与分支观测状态 %s 不一致"
                        % (effective, observed.value)
                    ),
                )
            )

    if effective == "abort":
        if committed:
            commit_lsn = (
                commit_record.commit_lsn
                if commit_record is not None
                else state.last_applied_lsn
            )
            conflict = Conflict(
                shard_id,
                COMMIT_CROSSED_ROLLBACK,
                (
                    "分片已在 lsn=%s 持久化提交，无法按 savepoint %s 回滚"
                    % (commit_lsn, savepoint_id)
                ),
            )
            conflicts.append(conflict)
            unrecoverable.append(
                UnrecoverableRange(
                    shard_id, target_lsn, commit_lsn or -1, conflict.code
                )
            )
            return (
                ShardRecovery(
                    shard_id,
                    RecoveryPhase.NONE,
                    target_lsn,
                    [],
                    [],
                    conflicts,
                    False,
                ),
                conflicts,
                unrecoverable,
            )

        undone: Set[int] = set()
        for rng in rollback_ranges:
            undone.update(rng.undone_lsns)
        undo_lsns = sorted(
            entry.lsn for entry in operations_after if entry.lsn not in undone
        )
        phase = RecoveryPhase.UNDO if undo_lsns else RecoveryPhase.NONE
        return (
            ShardRecovery(
                shard_id,
                phase,
                target_lsn,
                [],
                undo_lsns,
                conflicts,
                True,
            ),
            conflicts,
            unrecoverable,
        )

    # commit：重放 savepoint 之后、尚未应用的操作，幂等回放保证可安全重试。
    if commit_record is not None:
        end_lsn = commit_record.commit_lsn
    elif operations_after:
        end_lsn = max(entry.lsn for entry in operations_after)
    else:
        end_lsn = target_lsn

    applied = state.last_applied_lsn
    redo_lsns = sorted(
        entry.lsn
        for entry in operations_after
        if entry.lsn <= end_lsn and (applied is None or entry.lsn > applied)
    )
    phase = RecoveryPhase.REDO if redo_lsns else RecoveryPhase.NONE
    return (
        ShardRecovery(
            shard_id,
            phase,
            target_lsn,
            redo_lsns,
            [],
            conflicts,
            True,
        ),
        conflicts,
        unrecoverable,
    )


def _effective_decision(
    shard_id: str,
    decisions: Sequence[CoordinatorDecision],
) -> Optional[CoordinatorDecision]:
    """解析分片的有效协调器决定。

    优先级：分片专属持久化 > 全局持久化 > 分片专属非持久化 >
    全局非持久化；同级别取最后一条，模拟崩溃前的最新决定。
    """

    specific = [d for d in decisions if d.shard_id == shard_id]
    global_decisions = [d for d in decisions if d.shard_id is None]

    for pool in (specific, global_decisions):
        durable = [d for d in pool if d.durable]
        if durable:
            return durable[-1]
    for pool in (specific, global_decisions):
        if pool:
            return pool[-1]
    return None
