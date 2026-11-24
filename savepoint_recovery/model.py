"""恢复规划的输入/输出数据模型。

所有类型均为本地内存对象，不依赖任何远程协调服务。
Python 3.8 兼容（只用 dataclasses + typing，不用 PEP 585 泛型语法）。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Optional, Set


class EntryKind(str, Enum):
    """分片日志条目的种类。"""

    SAVEPOINT = "savepoint"
    OPERATION = "operation"
    PARENT_COMMIT = "parent_commit"
    BRANCH_COMMIT = "branch_commit"


class BranchStatus(str, Enum):
    """恢复开始时各分片分支的观测状态。"""

    ACTIVE = "active"
    COMMITTED = "committed"
    ABORTED = "aborted"


class RecoveryPhase(str, Enum):
    """单分片恢复阶段。"""

    UNDO = "undo"
    REDO = "redo"
    NONE = "none"


@dataclass(frozen=True)
class LogEntry:
    """单条分片日志。

    lsn 在单个分片内严格单调递增；op_id 用于幂等去重，
    同一条操作在重复回放时 op_id 不变。
    """

    lsn: int
    kind: EntryKind
    op_id: Optional[str] = None
    savepoint_id: Optional[str] = None
    payload: Optional[str] = None


@dataclass(frozen=True)
class BranchState:
    """恢复开始时某分片上事务分支的状态。"""

    shard_id: str
    status: BranchStatus
    last_applied_lsn: Optional[int] = None


@dataclass(frozen=True)
class RollbackRange:
    """协调器/分支已记录的回滚范围。

    target_lsn 为 savepoint 所在 lsn，回滚要求撤销 (target_lsn, end_lsn]
    之间的操作。undone_lsns 是已经实际撤销过的操作，用于崩溃后
    重复恢复时识别已完成的工作。
    """

    shard_id: str
    savepoint_id: str
    target_lsn: int
    end_lsn: int
    undone_lsns: Set[int] = field(default_factory=set)


@dataclass(frozen=True)
class CommitRecord:
    """分片上已持久化的分支提交记录。"""

    shard_id: str
    commit_lsn: int


@dataclass(frozen=True)
class CoordinatorDecision:
    """协调器对某分片分支的决定；shard_id 为 None 表示全局决定。"""

    decision: str  # "commit" 或 "abort"
    savepoint_id: Optional[str] = None
    shard_id: Optional[str] = None
    durable: bool = False


@dataclass(frozen=True)
class Conflict:
    """恢复过程中发现的冲突或不可恢复范围。"""

    shard_id: Optional[str]
    code: str
    message: str


@dataclass(frozen=True)
class ShardRecovery:
    """单个分片的恢复结论。"""

    shard_id: str
    phase: RecoveryPhase
    target_lsn: int
    redo_lsns: List[int]
    undo_lsns: List[int]
    conflicts: List[Conflict]
    recoverable: bool


# 冲突原因常量，便于调用方按 code 分支处理。
SAVEPOINT_NOT_FOUND = "savepoint_not_found"
SAVEPOINT_DESTROYED = "savepoint_destroyed"
DECISION_MISMATCH = "decision_mismatch"
COMMIT_CROSSED_ROLLBACK = "commit_crossed_rollback"
ROLLBACK_CROSSES_PARENT_COMMIT = "rollback_crosses_parent_commit"
SAVEPOINT_VERSION_MISMATCH = "savepoint_version_mismatch"
IN_DOUBT = "in_doubt"
