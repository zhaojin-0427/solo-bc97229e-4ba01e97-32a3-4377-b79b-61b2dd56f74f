"""临时写锁容错回归测试（仅依赖标准库，无需 Flask / 浏览器）。

可重复覆盖两处锁冲突与解锁接续：
1. 领取冲突：外部连接持有写锁期间，worker 领取任务只会等待、线程不退出，
   网页/状态查询（只读连接 + WAL）始终可响应；解锁后自动领取原任务，且
   「领取状态提交成功前不得操作门户」。
2. 发布冲突：CSV 已下载校验并写入暂存区后，发布状态提交撞上写锁时，
   任务保持执行中、文件不可下载、绝不重新导出；解锁后巡检自动接续发布，
   状态与完整文件一致，后续队列继续推进。

每个场景连续跑两轮，验证可重复覆盖而不是一次性巧合。
"""
from __future__ import annotations

import shutil
import sqlite3
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "collector"))

from app import config as collector_config  # noqa: E402
from app.models import LockTimeout, TaskStore  # noqa: E402
from app.worker import Worker  # noqa: E402

CSV_TEMPLATE = (
    "﻿产线,日期,样品号,测量值\n"
    "{line},{date},{line}-001,12.34\n"
    "{line},{date},{line}-002,56.78\n"
)


class FakeBot:
    """PortalBot 测试替身：不启动浏览器，直接在工作目录落一份合法 CSV。"""

    active = 0
    max_active = 0
    invocations = 0
    _lock = threading.Lock()

    def __init__(self, task, work_dir, cancel_event):
        self.task = task
        self.work_dir = Path(work_dir)
        self.cancel_event = cancel_event

    def run(self):
        with FakeBot._lock:
            FakeBot.invocations += 1
            FakeBot.active += 1
            FakeBot.max_active = max(FakeBot.max_active, FakeBot.active)
        try:
            path = self.work_dir / "export.csv"
            path.write_text(
                CSV_TEMPLATE.format(line=self.task["line"], date=self.task["date"]),
                encoding="utf-8",
            )
            return path
        finally:
            with FakeBot._lock:
                FakeBot.active -= 1

    @classmethod
    def reset(cls):
        cls.active = 0
        cls.max_active = 0
        cls.invocations = 0


class ExternalWriteLock:
    """模拟「另一连接持有写锁」：在独立连接里开启 IMMEDIATE 写事务。"""

    def __init__(self, db_path: str):
        self._db_path = db_path
        self._conn = None
        self._held = threading.Event()
        self._release = threading.Event()
        self._thread = threading.Thread(target=self._hold, daemon=True)

    def _hold(self):
        # 连接必须在使用它的线程内创建。
        conn = sqlite3.connect(self._db_path, timeout=0.5, check_same_thread=False)
        self._conn = conn
        conn.execute("PRAGMA busy_timeout=200")
        conn.execute("BEGIN IMMEDIATE")
        self._held.set()
        self._release.wait(30)
        conn.commit()
        conn.close()

    def __enter__(self):
        self._thread.start()
        if not self._held.wait(5):
            raise RuntimeError("外部写锁未能在 5 秒内获取")
        return self

    def __exit__(self, *exc):
        self._release.set()
        self._thread.join(5)


class LockRecoveryTest(unittest.TestCase):
    def setUp(self):
        self.tmpdir = Path(tempfile.mkdtemp(prefix="collector-lock-"))
        collector_config.DATA_DIR = self.tmpdir / "data"
        collector_config.DB_PATH = collector_config.DATA_DIR / "collector.db"
        collector_config.PUBLISHED_DIR = collector_config.DATA_DIR / "published"
        collector_config.STAGING_DIR = collector_config.DATA_DIR / "staging"
        collector_config.WORK_DIR = collector_config.DATA_DIR / "work"
        # 缩短锁等待，让用例快速进入 LockTimeout 路径。
        collector_config.DB_BUSY_TIMEOUT_SECONDS = 0.2
        collector_config.DB_LOCK_RETRIES = 1
        collector_config.POLL_INTERVAL = 0.05
        collector_config.RECONCILE_INTERVAL = 0.2
        FakeBot.reset()

        self.store = TaskStore()
        self.worker = self._build_worker(self.store)
        self.worker.start()

    def _build_worker(self, store):
        """构造带发布钩子的 worker：提交前等待测试把外部写锁准备好。"""
        worker = Worker(store, bot_factory=FakeBot)

        def _hook(task_id, published_path):
            ready = getattr(worker, "_hook_ready", None)
            if ready is not None:
                ready.set()
            gate = getattr(worker, "_hook_gate", None)
            if gate is not None:
                gate.wait(10)

        worker.before_publish_commit = _hook
        return worker

    def tearDown(self):
        self.worker.shutdown()
        self.worker.join(timeout=5)
        self.store.close()
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    # ------------------------------------------------------------------
    def _create(self, line, day):
        return self.store.create_task(line, day)

    def _wait(self, task_id, status, timeout=20):
        deadline = time.time() + timeout
        last = None
        while time.time() < deadline:
            task = self.store.get_task(task_id)
            last = task["status"]
            if task["status"] == status:
                return task
            time.sleep(0.05)
        raise AssertionError(f"任务 {task_id} 未在 {timeout}s 内到达 {status}，当前 {last}")

    @staticmethod
    def _wait_until(condition, timeout=5):
        deadline = time.time() + timeout
        while time.time() < deadline:
            if condition():
                return
            time.sleep(0.02)
        raise AssertionError("条件在超时时间内未满足")

    def _published_path(self, task):
        return collector_config.PUBLISHED_DIR / (
            f"{task['line']}_{task['date']}_{task['id']}.csv"
        )

    # ------------------------------------------------------------------
    def test_claim_lock_conflict_then_auto_resume(self):
        """领取处锁冲突，重复两轮：等待期可读、不碰门户，解锁后自动接续。"""
        for round_no in (1, 2):
            with self.subTest(round=round_no):
                task = self._create(f"L{round_no}", "2026-11-10")
                before = FakeBot.invocations

                with ExternalWriteLock(str(collector_config.DB_PATH)):
                    # 留时间让 worker 至少撞一次锁。
                    time.sleep(1.0)

                    # worker 线程仍然存活，没有因锁异常退出。
                    self.assertTrue(
                        self.worker.is_alive(), "worker 在写锁等待期间退出"
                    )
                    # 任务仍是排队中，门户替身一次都没被调用：
                    # 领取状态提交成功前不得操作门户。
                    self.assertEqual(self.store.get_task(task["id"])["status"], "queued")
                    self.assertEqual(FakeBot.invocations, before)

                    # 网页/状态查询在写锁占用期间必须保持响应（WAL + 只读连接）。
                    started = time.monotonic()
                    listing = self.store.list_tasks()
                    single = self.store.get_task(task["id"])
                    self.assertLess(time.monotonic() - started, 2.0,
                                    "写锁期间状态查询被阻塞")
                    self.assertTrue(any(t["id"] == task["id"] for t in listing))
                    self.assertIsNotNone(single)

                # 解锁后无需人工干预，worker 自动领取并发布原任务。
                done = self._wait(task["id"], "published")
                self.assertEqual(FakeBot.invocations, before + 1)
                self.assertTrue(done["published_file"])
                self.assertTrue(self._published_path(done).exists())

    def test_publish_lock_conflict_keeps_file_and_resumes(self):
        """发布处锁冲突，重复两轮：不重导、不可下载，解锁后状态与文件一致。"""
        for round_no in (1, 2):
            with self.subTest(round=round_no):
                task = self._create(f"P{round_no}", "2026-11-11")
                before = FakeBot.invocations

                # worker 走到「文件已暂存、即将提交发布状态」时会触发钩子。
                ready = threading.Event()
                gate = threading.Event()
                self.worker._hook_ready = ready
                self.worker._hook_gate = gate
                try:
                    self.assertTrue(ready.wait(10), "未进入发布提交阶段")
                    # 此刻状态尚未提交，任务保持执行中。
                    current = self.store.get_task(task["id"])
                    self.assertEqual(current["status"], "running")
                    self.assertEqual(FakeBot.invocations, before + 1)
                    published_path = self._published_path(current)
                    staged = collector_config.STAGING_DIR / f"{task['id']}.csv"
                    # 提交前发布目录不得有文件，已校验 CSV 静卧暂存区。
                    self.assertFalse(published_path.exists())
                    self.assertTrue(staged.exists())

                    # 在提交瞬间占住外部写锁，再放行 worker，提交必然撞锁。
                    with ExternalWriteLock(str(collector_config.DB_PATH)):
                        gate.set()
                        # 等待 worker 真正经历锁冲突（日志外的可观察结果是
                        # 状态仍 running 且文件仍在暂存区），用轮询替代固定 sleep。
                        self._wait_until(
                            lambda: self.store.get_task(task["id"])["status"] == "running"
                        )
                        time.sleep(0.6)  # 覆盖多轮发布提交/巡检尝试

                        current = self.store.get_task(task["id"])
                        # 发布状态提交不成功就一直是执行中，不得提前显示已发布。
                        self.assertEqual(current["status"], "running")
                        self.assertIsNone(current["published_file"])
                        # 锁内发布目录始终不可下载，文件保留在暂存区。
                        self.assertFalse(published_path.exists())
                        self.assertTrue(staged.exists(), "已校验 CSV 应保留在暂存区")
                        # 门户替身没有被再次调用：已下载校验的 CSV 不得重新导出。
                        self.assertEqual(FakeBot.invocations, before + 1)
                        # worker 依旧存活，状态查询即时可响应。
                        self.assertTrue(self.worker.is_alive())
                        self.assertIsNotNone(self.store.get_task(task["id"]))
                finally:
                    gate.set()  # 防止异常路径下 worker 永久阻塞
                    self.worker._hook_ready = None
                    self.worker._hook_gate = None

                # 解锁后巡检自动接续发布，全程无第二次门户操作。
                done = self._wait(task["id"], "published")
                self.assertEqual(FakeBot.invocations, before + 1)
                published = self._published_path(done)
                self.assertTrue(published.exists())
                # 状态与完整文件一致：文件内容就是当初下载并校验的那份数据。
                content = published.read_text(encoding="utf-8-sig")
                self.assertIn("产线,日期,样品号,测量值", content)
                self.assertTrue(
                    all(
                        line.startswith(f"{done['line']},{done['date']},")
                        for line in content.splitlines()[1:]
                        if line.strip()
                    )
                )
                self.assertFalse(
                    (collector_config.STAGING_DIR / f"{task['id']}.csv").exists()
                )

    def test_queue_continues_after_unlock_and_stays_serial(self):
        """锁释放后原任务与后续队列自动接续，且保持单会话串行。"""
        first = self._create("Q1", "2026-11-12")
        second = self._create("Q2", "2026-11-12")
        with ExternalWriteLock(str(collector_config.DB_PATH)):
            time.sleep(1.0)
            self.assertEqual(self.store.get_task(first["id"])["status"], "queued")
            self.assertEqual(self.store.get_task(second["id"])["status"], "queued")

        done1 = self._wait(first["id"], "published")
        done2 = self._wait(second["id"], "published")
        # 两个任务都完成，门户替身各执行一次，全程无并发（单会话串行）。
        self.assertEqual(FakeBot.invocations, 2)
        self.assertEqual(FakeBot.max_active, 1)
        # FIFO 顺序：先创建的先开始。
        self.assertLessEqual(done1["started_at"], done2["started_at"])

    def test_lock_timeout_is_retried_not_raised_to_worker_loop(self):
        """直接验证写操作在锁冲突下抛 LockTimeout，且 worker 循环不被打断。"""
        task = self._create("R1", "2026-11-13")
        with ExternalWriteLock(str(collector_config.DB_PATH)):
            with self.assertRaises(LockTimeout):
                self.store.mark_published(task["id"], "x.csv")
            # 锁内只读仍可用。
            self.assertEqual(self.store.get_task(task["id"])["status"], "queued")
        # 解锁后队列照常推进，线程健康。
        self._wait(task["id"], "published")
        self.assertTrue(self.worker.is_alive())

    def test_restart_resumes_staged_publish_without_reexport(self):
        """重启后保留 running 暂存任务，新 worker 自动接续且不重新导出。"""
        task = self._create("S1", "2026-11-14")
        done = self._wait(task["id"], "published")
        # 此刻门户替身恰好被调用一次；记录重启前的调用总数。
        calls_at_restart = FakeBot.invocations
        # 先取一份发布文件作为「已下载校验」的基准，再模拟发布前进程崩溃：
        # 状态回退 running、文件移回暂存区、删除发布文件与工作目录。
        published = self._published_path(done)
        content = published.read_bytes()
        self.worker.shutdown()
        self.worker.join(timeout=5)
        published.unlink()
        shutil.rmtree(collector_config.WORK_DIR / task["id"], ignore_errors=True)
        staged = collector_config.STAGING_DIR / f"{task['id']}.csv"
        staged.write_bytes(content)
        self.store.update(task["id"], status="running", published_file=None,
                          finished_at=None)

        # 启动恢复：有暂存产物的 running 任务不得被打成 failed。
        recovered = self.store.recover_stale()
        self.assertEqual(recovered, 0)
        self.assertEqual(self.store.get_task(task["id"])["status"], "running")

        # 新 worker（仍注入 FakeBot）巡检接续发布；替身调用次数不再增加。
        new_worker = self._build_worker(self.store)
        new_worker.start()
        try:
            resumed = self._wait(task["id"], "published")
            self.assertEqual(
                FakeBot.invocations, calls_at_restart,
                "恢复时不应重新导出门户 CSV",
            )
            self.assertEqual(resumed["published_file"], published.name)
            self.assertEqual(published.read_bytes(), content, "发布文件须与暂存原件一致")
            self.assertFalse(staged.exists())
        finally:
            new_worker.shutdown()
            new_worker.join(timeout=5)
            self.worker = new_worker  # 避免 tearDown 重复关闭旧线程


if __name__ == "__main__":
    unittest.main(verbosity=2)
