"""按规划结果执行恢复的幂等回放器。

所有状态保存在本地内存中：每个操作以 ``(shard_id, op_id)`` 作为幂等键，
重复恢复时已应用的操作直接跳过，保证分片重复回放不产生副作用。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Sequence, Set, Tuple

from .model import (
    EntryKind,
    LogEntry,
    RecoveryPhase,
)
from .planner import RecoveryPlan


@dataclass(frozen=True)
class ReplayReport:
    """一次回放的执行明细。"""

    executed: List[Tuple[str, str, str]]
    skipped: List[Tuple[str, str, str]]
    failed_shards: List[str]

    @property
    def ok(self) -> bool:
        return not self.failed_shards


class IdempotentReplayer:
    """按 ``RecoveryPlan`` 顺序执行 UNDO/REDO 的本地回放器。

    ``applied`` 记录已生效的幂等键；同一个 replayer 实例可安全地
    对同一计划重复调用 ``run``，第二次调用全部命中跳过路径。
    """

    def __init__(self, shard_logs: Dict[str, Sequence[LogEntry]]):
        self._logs = {
            shard_id: {entry.lsn: entry for entry in log}
            for shard_id, log in shard_logs.items()
        }
        self.applied: Set[Tuple[str, str]] = set()

    def run(self, plan: RecoveryPlan) -> ReplayReport:
        executed: List[Tuple[str, str, str]] = []
        skipped: List[Tuple[str, str, str]] = []
        failed_shards: List[str] = []

        for shard_id in plan.order:
            recovery = plan.shards[shard_id]
            if not recovery.recoverable:
                failed_shards.append(shard_id)
                continue

            if recovery.phase == RecoveryPhase.UNDO:
                self._apply(
                    shard_id,
                    recovery.undo_lsns,
                    "undo",
                    executed,
                    skipped,
                )
            elif recovery.phase == RecoveryPhase.REDO:
                self._apply(
                    shard_id,
                    recovery.redo_lsns,
                    "redo",
                    executed,
                    skipped,
                )

        return ReplayReport(executed, skipped, failed_shards)

    def _apply(
        self,
        shard_id: str,
        lsns: Sequence[int],
        action: str,
        executed: List[Tuple[str, str, str]],
        skipped: List[Tuple[str, str, str]],
    ) -> None:
        entries = self._logs.get(shard_id, {})
        for lsn in lsns:
            entry = entries.get(lsn)
            if entry is None or entry.kind != EntryKind.OPERATION:
                continue
            op_id = entry.op_id or "op@%d" % lsn
            key = (shard_id, op_id)
            if key in self.applied:
                skipped.append((shard_id, action, op_id))
                continue
            self.applied.add(key)
            executed.append((shard_id, action, op_id))
