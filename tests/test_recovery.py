import unittest

from savepoint_recovery.model import (
    COMMIT_CROSSED_ROLLBACK,
    IN_DOUBT,
    ROLLBACK_CROSSES_PARENT_COMMIT,
    SAVEPOINT_NOT_FOUND,
    SAVEPOINT_VERSION_MISMATCH,
    BranchState,
    BranchStatus,
    CommitRecord,
    CoordinatorDecision,
    EntryKind,
    LogEntry,
    RecoveryPhase,
    RollbackRange,
)
from savepoint_recovery.planner import build_recovery_plan
from savepoint_recovery.replay import IdempotentReplayer


def op(lsn, op_id, payload=None):
    return LogEntry(lsn, EntryKind.OPERATION, op_id=op_id, payload=payload)


def sp(lsn, savepoint_id):
    return LogEntry(
        lsn, EntryKind.SAVEPOINT, savepoint_id=savepoint_id
    )


def parent_commit(lsn):
    return LogEntry(lsn, EntryKind.PARENT_COMMIT)


class PartialRollbackTest(unittest.TestCase):
    """部分分片回滚、另一些分片提交的混合恢复场景。"""

    def setUp(self):
        self.savepoint_id = "sp-1"
        self.shard_logs = {
            "s1": [sp(10, "sp-1"), op(11, "a"), op(12, "b")],
            "s2": [sp(20, "sp-1"), op(21, "c"), op(22, "d")],
            "s3": [sp(30, "sp-1"), op(31, "e"), op(32, "f")],
        }
        self.branch_states = {
            "s1": BranchState("s1", BranchStatus.ACTIVE, 12),
            "s2": BranchState("s2", BranchStatus.ACTIVE, 21),
            # s3 已提交但只应用到 31，日志中 32 是崩溃后待重放的缺口。
            "s3": BranchState("s3", BranchStatus.COMMITTED, 31),
        }
        self.rollback_ranges = [
            RollbackRange("s1", "sp-1", 10, 12, undone_lsns={11}),
        ]
        self.commit_records = [CommitRecord("s3", 32)]
        self.decisions = [
            CoordinatorDecision("abort", "sp-1", shard_id="s1", durable=True),
            CoordinatorDecision("abort", "sp-1", shard_id="s2", durable=True),
            CoordinatorDecision("commit", "sp-1", shard_id="s3", durable=True),
        ]

    def test_target_positions_and_order(self):
        plan = build_recovery_plan(
            self.savepoint_id,
            self.shard_logs,
            self.branch_states,
            self.rollback_ranges,
            self.commit_records,
            self.decisions,
        )

        self.assertTrue(plan.recoverable)
        # UNDO 分片统一排在 REDO 分片之前，先建立一致的 savepoint 基线。
        self.assertEqual(plan.order, ["s1", "s2", "s3"])

        self.assertEqual(plan.shards["s1"].phase, RecoveryPhase.UNDO)
        self.assertEqual(plan.shards["s1"].target_lsn, 10)
        # lsn 11 已经撤销，重复恢复只补做 lsn 12。
        self.assertEqual(plan.shards["s1"].undo_lsns, [12])

        self.assertEqual(plan.shards["s2"].phase, RecoveryPhase.UNDO)
        self.assertEqual(plan.shards["s2"].target_lsn, 20)
        self.assertEqual(plan.shards["s2"].undo_lsns, [21, 22])

        self.assertEqual(plan.shards["s3"].phase, RecoveryPhase.REDO)
        self.assertEqual(plan.shards["s3"].target_lsn, 30)
        # 已应用到 31，但提交记录覆盖到 32，存在一个待重放缺口。
        self.assertEqual(plan.shards["s3"].redo_lsns, [32])

    def test_idempotent_replay(self):
        plan = build_recovery_plan(
            self.savepoint_id,
            self.shard_logs,
            self.branch_states,
            self.rollback_ranges,
            self.commit_records,
            self.decisions,
        )
        replayer = IdempotentReplayer(self.shard_logs)

        first = replayer.run(plan)
        self.assertTrue(first.ok)
        # 首次执行：s1 补撤销 b，s2 撤销 c/d，s3 重放缺口 f。
        self.assertEqual(
            sorted(first.executed),
            [
                ("s1", "undo", "b"),
                ("s2", "undo", "c"),
                ("s2", "undo", "d"),
                ("s3", "redo", "f"),
            ],
        )

        # 对同一计划重复恢复：所有操作都应命中幂等键并跳过。
        second = replayer.run(plan)
        self.assertTrue(second.ok)
        self.assertEqual(second.executed, [])
        self.assertEqual(
            sorted(second.skipped),
            [
                ("s1", "undo", "b"),
                ("s2", "undo", "c"),
                ("s2", "undo", "d"),
                ("s3", "redo", "f"),
            ],
        )


class DuplicateSavepointNameTest(unittest.TestCase):
    def setUp(self):
        self.savepoint_id = "sp-dup"
        # savepoint 建立两次：lsn 10 是已释放的旧版本，lsn 30 是当前版本。
        self.logs = {
            "s1": [
                sp(10, "sp-dup"),
                op(11, "a"),
                sp(30, "sp-dup"),
                op(31, "b"),
            ],
        }
        self.states = {
            "s1": BranchState("s1", BranchStatus.ACTIVE, 31),
        }

    def test_latest_version_is_used(self):
        plan = build_recovery_plan(
            self.savepoint_id,
            self.logs,
            self.states,
            [],
            [],
            [CoordinatorDecision("abort", "sp-dup", durable=True)],
        )
        self.assertTrue(plan.recoverable)
        self.assertEqual(plan.shards["s1"].target_lsn, 30)
        # 只撤销当前 savepoint 之后的操作，旧版本下的 a 不受影响。
        self.assertEqual(plan.shards["s1"].undo_lsns, [31])

    def test_stale_rollback_range_is_unrecoverable(self):
        stale_range = RollbackRange(
            "s1", "sp-dup", target_lsn=10, end_lsn=11
        )
        plan = build_recovery_plan(
            self.savepoint_id,
            self.logs,
            self.states,
            [stale_range],
            [],
            [CoordinatorDecision("abort", "sp-dup", durable=True)],
        )
        self.assertFalse(plan.recoverable)
        codes = {conflict.code for conflict in plan.conflicts}
        self.assertIn(SAVEPOINT_VERSION_MISMATCH, codes)
        self.assertEqual(
            plan.unrecoverable_ranges[0].start_lsn, 10
        )


class CoordinatorCrashTest(unittest.TestCase):
    def setUp(self):
        self.savepoint_id = "sp-crash"
        self.logs = {
            "s1": [sp(10, "sp-crash"), op(11, "a")],
            "s2": [sp(20, "sp-crash"), op(21, "b")],
        }
        self.states = {
            "s1": BranchState("s1", BranchStatus.ACTIVE, 11),
            "s2": BranchState("s2", BranchStatus.COMMITTED, 21),
        }

    def test_in_doubt_heuristic_without_decisions(self):
        plan = build_recovery_plan(
            self.savepoint_id,
            self.logs,
            self.states,
            [],
            [CommitRecord("s2", 21)],
            [],
        )
        self.assertTrue(plan.recoverable)
        codes = {conflict.code for conflict in plan.conflicts}
        self.assertEqual(codes, {IN_DOUBT})
        # 未提交的 in-doubt 分支保守回滚。
        self.assertEqual(plan.shards["s1"].phase, RecoveryPhase.UNDO)
        self.assertEqual(plan.shards["s1"].undo_lsns, [11])
        # 已持久化提交的分支保留提交，不做重放。
        self.assertEqual(plan.shards["s2"].phase, RecoveryPhase.NONE)
        self.assertEqual(plan.shards["s2"].redo_lsns, [])

    def test_durable_decision_outranks_lost_nondurable(self):
        # 崩溃前只有 s1 的 abort 决定落盘，s2 的决定随内存丢失。
        plan = build_recovery_plan(
            self.savepoint_id,
            self.logs,
            self.states,
            [],
            [CommitRecord("s2", 21)],
            [
                CoordinatorDecision(
                    "abort", "sp-crash", shard_id="s1", durable=True
                ),
            ],
        )
        self.assertEqual(plan.shards["s1"].phase, RecoveryPhase.UNDO)
        s2_codes = {
            conflict.code
            for conflict in plan.shards["s2"].conflicts
        }
        self.assertIn(IN_DOUBT, s2_codes)
        self.assertEqual(plan.shards["s2"].phase, RecoveryPhase.NONE)


class RepeatedRecoveryTest(unittest.TestCase):
    def test_second_run_plans_no_pending_undo(self):
        logs = {"s1": [sp(10, "sp-1"), op(11, "a"), op(12, "b")]}
        states = {"s1": BranchState("s1", BranchStatus.ACTIVE, 12)}

        first_plan = build_recovery_plan(
            "sp-1",
            logs,
            states,
            [RollbackRange("s1", "sp-1", 10, 12, undone_lsns={11})],
            [],
            [CoordinatorDecision("abort", "sp-1", durable=True)],
        )
        self.assertEqual(first_plan.shards["s1"].undo_lsns, [12])

        # 恢复完成后回滚范围包含全部操作，重新规划应得到空 UNDO。
        second_plan = build_recovery_plan(
            "sp-1",
            logs,
            states,
            [
                RollbackRange(
                    "s1", "sp-1", 10, 12, undone_lsns={11, 12}
                )
            ],
            [],
            [CoordinatorDecision("abort", "sp-1", durable=True)],
        )
        self.assertEqual(second_plan.shards["s1"].phase, RecoveryPhase.NONE)
        self.assertEqual(second_plan.shards["s1"].undo_lsns, [])

    def test_replay_is_idempotent_across_instances(self):
        logs = {"s1": [sp(10, "sp-1"), op(11, "a"), op(12, "b")]}
        states = {"s1": BranchState("s1", BranchStatus.ACTIVE, 12)}
        plan = build_recovery_plan(
            "sp-1",
            logs,
            states,
            [],
            [],
            [CoordinatorDecision("abort", "sp-1", durable=True)],
        )

        first = IdempotentReplayer(logs).run(plan)
        second = IdempotentReplayer(logs).run(plan)
        self.assertEqual(len(first.executed), 2)
        # 新实例没有历史应用记录时仍按计划重放；同一实例内重复调用才跳过。
        self.assertEqual(len(second.executed), 2)

        shared = IdempotentReplayer(logs)
        shared.run(plan)
        again = shared.run(plan)
        self.assertEqual(again.executed, [])
        self.assertEqual(len(again.skipped), 2)


class UnrecoverableBoundaryTest(unittest.TestCase):
    def test_rollback_cannot_cross_parent_commit(self):
        logs = {
            "s1": [
                sp(10, "sp-1"),
                op(11, "a"),
                parent_commit(12),
                op(13, "b"),
            ],
        }
        states = {"s1": BranchState("s1", BranchStatus.ACTIVE, 13)}
        plan = build_recovery_plan(
            "sp-1",
            logs,
            states,
            [],
            [],
            [CoordinatorDecision("abort", "sp-1", durable=True)],
        )
        self.assertFalse(plan.recoverable)
        codes = {conflict.code for conflict in plan.conflicts}
        self.assertIn(ROLLBACK_CROSSES_PARENT_COMMIT, codes)
        self.assertEqual(plan.shards["s1"].phase, RecoveryPhase.NONE)

    def test_committed_shard_cannot_be_rolled_back(self):
        logs = {"s1": [sp(10, "sp-1"), op(11, "a")]}
        states = {"s1": BranchState("s1", BranchStatus.COMMITTED, 11)}
        plan = build_recovery_plan(
            "sp-1",
            logs,
            states,
            [],
            [CommitRecord("s1", 11)],
            [CoordinatorDecision("abort", "sp-1", durable=True)],
        )
        self.assertFalse(plan.recoverable)
        codes = {conflict.code for conflict in plan.conflicts}
        self.assertIn(COMMIT_CROSSED_ROLLBACK, codes)

    def test_missing_savepoint_is_unrecoverable(self):
        logs = {"s1": [op(1, "a")]}
        states = {"s1": BranchState("s1", BranchStatus.ACTIVE, 1)}
        plan = build_recovery_plan(
            "sp-x",
            logs,
            states,
            [],
            [],
            [CoordinatorDecision("abort", "sp-x", durable=True)],
        )
        self.assertFalse(plan.recoverable)
        self.assertEqual(plan.conflicts[0].code, SAVEPOINT_NOT_FOUND)


if __name__ == "__main__":
    unittest.main()
