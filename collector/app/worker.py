"""单线程 worker：串行执行任务，保证门户单会话不被交叉占用。

锁容错要点：
- 领取（queued→running）与发布（running→published）的状态提交都可能遇到其他
  连接的写锁；这类冲突只触发等待与重试，worker 线程不会退出，队列在解锁后
  自动接续，无需重启或人工重试。
- 门户操作只在领取状态成功提交之后开始；已下载并校验通过的 CSV 先落入暂存区，
  发布状态提交成功后才对外可下载，提交前任何中断都不会重复导出。
"""
from __future__ import annotations

import logging
import os
import shutil
import threading
import time
from pathlib import Path

from . import config
from .models import LockTimeout, TaskStore
from .validation import validate_csv

log = logging.getLogger("collector.worker")


class Worker(threading.Thread):
    def __init__(self, store: TaskStore, bot_factory=None):
        super().__init__(name="collector-worker", daemon=True)
        self.store = store
        # 默认延迟导入真实 PortalBot（避免仅测试存储层时强依赖 Playwright）；
        # 测试可注入替身工厂。
        self._bot_factory = bot_factory
        self._wake = threading.Event()
        self._stop_event = threading.Event()
        # 状态提交被写锁挡住时的待办，巡检轮解锁后自动补提交。
        self._pending_failures: dict[str, str] = {}
        self._pending_cancels: set[str] = set()
        # 正在执行发布流程的任务，巡检跳过以免并发移动同一暂存文件。
        self._publishing: set[str] = set()

    def notify(self) -> None:
        """有新任务或状态变化时唤醒 worker。"""
        self._wake.set()

    def shutdown(self) -> None:
        self._stop_event.set()
        self._wake.set()

    # ------------------------------------------------------------------
    def run(self) -> None:
        log.info("采集 worker 已启动，串行调度门户任务")
        last_reconcile = 0.0
        while not self._stop_event.is_set():
            self._wake.wait(config.POLL_INTERVAL)
            self._wake.clear()

            # 周期巡检：接续进程/线程遗留的「已下载待发布」等中间进度。
            now = time.monotonic()
            if now - last_reconcile >= config.RECONCILE_INTERVAL:
                last_reconcile = now
                try:
                    self._reconcile()
                except Exception:
                    log.exception("进度巡检失败，下轮继续")

            try:
                task = self.store.claim_next()
            except LockTimeout:
                # 写锁仍被占用：保持所有任务原样，等待解锁后下一轮自动接续。
                log.warning("领取任务时数据库写锁未释放，本轮跳过稍后自动重试")
                continue
            except Exception:
                log.exception("读取待执行任务失败")
                continue
            if task is None:
                continue
            self._run_task(task)

    # ------------------------------------------------------------------
    def _run_task(self, task: dict) -> None:
        task_id = task["id"]
        cancel_event = self.store._event(task_id)  # 复用 models 中的事件

        fresh = self.store.get_task(task_id)
        if fresh is None or fresh["status"] != "running":
            return  # 已被取消或并发改动，放弃本轮
        log.info("任务 %s 开始执行：line=%s date=%s", task_id, task["line"], task["date"])

        work_dir = config.WORK_DIR / task_id
        if work_dir.exists():
            shutil.rmtree(work_dir, ignore_errors=True)
        work_dir.mkdir(parents=True, exist_ok=True)

        # 阶段 1：门户操作（领取状态已提交，此刻起才允许触碰门户）。
        downloaded = self._acquire_download(fresh, work_dir, cancel_event)
        if downloaded is None:
            return  # 取消或失败已在内部处理

        # 阶段 2：校验 + 暂存 + 发布（不做任何门户交互，锁冲突可无限接续）。
        self._publish_guarded(fresh, work_dir, downloaded, cancel_event)

    def _publish_guarded(self, task, work_dir, downloaded, cancel_event) -> None:
        """串行化同一任务的发布流程，避免与周期巡检并发操作暂存文件。"""
        task_id = task["id"]
        if task_id in self._publishing:
            return
        self._publishing.add(task_id)
        try:
            self._publish(task, work_dir, downloaded, cancel_event)
        finally:
            self._publishing.discard(task_id)

    # ---- 阶段 1：门户下载 ------------------------------------------------
    def _acquire_download(
        self, task: dict, work_dir: Path, cancel_event: threading.Event
    ) -> Path | None:
        """完成门户会话并拿到落盘 CSV，返回下载路径；取消/失败返回 None。"""
        task_id = task["id"]

        # 恢复场景：工作目录里已有完整下载（例如上次发布阶段被写锁卡住），
        # 直接复用，绝不重新导出。
        existing = self._find_download(work_dir)
        if existing is not None:
            log.info("任务 %s 复用已下载文件 %s，跳过门户重复导出", task_id, existing.name)
            return existing

        if self._bot_factory is not None:
            # 测试替身路径：不导入 Playwright，也不处理 TaskCancelled。
            try:
                downloaded = self._bot_factory(task, work_dir, cancel_event).run()
            except Exception as exc:
                log.exception("任务 %s 浏览器操作失败", task_id)
                self._handle_failure(
                    task_id, f"门户操作失败：{exc.__class__.__name__}: {exc}"
                )
                return None
        else:
            # 真实门户路径：延迟导入，存储层/测试场景不必安装 Playwright。
            from .portal_bot import PortalBot, TaskCancelled

            bot = PortalBot(task, work_dir, cancel_event)
            try:
                downloaded = bot.run()
            except TaskCancelled:
                self._handle_cancel(task_id, work_dir)
                return None
            except Exception as exc:  # 浏览器/门户侧任何异常都归为失败，可重试
                log.exception("任务 %s 浏览器操作失败", task_id)
                self._handle_failure(task_id, f"门户操作失败：{exc.__class__.__name__}: {exc}")
                return None

        # 下载完成后再次核对：取消信号可能在落盘瞬间到达，
        # 这类「迟到下载」一律不得发布。
        if cancel_event.is_set() or self.store.get_task(task_id)["status"] != "running":
            log.warning("任务 %s 下载已完成但任务已被取消，丢弃迟到下载", task_id)
            self._handle_cancel(task_id, work_dir)
            return None
        return downloaded

    # ---- 阶段 2：校验、暂存与发布 ----------------------------------------
    def _publish(
        self, task: dict, work_dir: Path, downloaded: Path,
        cancel_event: threading.Event,
    ) -> None:
        task_id = task["id"]

        # 已暂存（上次通过校验但发布提交被锁中断）则跳过重复校验与复制。
        staged = self._staged_path(task_id)
        if not staged.exists():
            result = validate_csv(downloaded, task["line"], task["date"])
            if not result.ok:
                log.warning("任务 %s 校验未通过：%s", task_id, result.message)
                self._handle_failure(task_id, f"数据校验失败：{result.message}")
                return
            shutil.copy2(downloaded, staged)
            log.info("任务 %s 校验通过（%s），CSV 已暂存等待发布提交",
                     task_id, result.message)

        # 发布前最后一次状态复核，消除竞态窗口。
        if cancel_event.is_set() or self.store.get_task(task_id)["status"] != "running":
            staged.unlink(missing_ok=True)
            log.warning("任务 %s 发布前被取消，已删除暂存文件", task_id)
            self._handle_cancel(task_id, work_dir)
            return

        published_name = f"{task['line']}_{task['date']}_{task_id}.csv"
        published_path = config.PUBLISHED_DIR / published_name

        # 发布状态提交：写锁冲突时等待重试。文件先留在暂存区，只有提交成功
        # 后才原子移动到发布目录——保证「状态与完整文件一致」，且提交前
        # 发布目录始终没有可下载文件（下载接口返回 409），绝不重新导出。
        # 钩子便于测试在提交瞬间注入外部写锁，生产环境为空操作。
        self.before_publish_commit(task_id, published_path)
        try:
            self.store.mark_published(task_id, published_name)
        except LockTimeout:
            # 文件保留在暂存区、任务保持 running，巡检在解锁后自动接续，
            # 全程不会重新导出。
            log.warning(
                "任务 %s 发布状态提交遇到持续写锁，保留暂存文件，解锁后自动接续",
                task_id,
            )
            return
        os.replace(staged, published_path)

        shutil.rmtree(work_dir, ignore_errors=True)
        log.info("任务 %s 已发布：%s", task_id, published_name)

    # ---- 巡检与恢复 ------------------------------------------------------
    def _reconcile(self) -> None:
        """接续被写锁中断的状态提交与发布，无需重启或人工重试。

        触发场景：发布/失败/取消状态提交期间写锁长期占用，或进程重启后
        暂存 CSV 已就绪。已校验的暂存文件只做移动与状态提交，不重新操作
        门户、不重新导出。
        """
        # 1) 先补提交失败/取消终态（解锁后立刻生效，任务不再卡在执行中）。
        for task_id in list(self._pending_cancels):
            try:
                self.store.mark_cancelled(
                    task_id, "已停止浏览器操作并删除未发布文件，门户会话已释放"
                )
                self._pending_cancels.discard(task_id)
                log.info("任务 %s 取消状态补提交成功", task_id)
            except LockTimeout:
                # 该任务的锁仍未释放，跳过它继续尝试其余待办，下轮再来。
                continue
        for task_id, message in list(self._pending_failures.items()):
            try:
                self.store.mark_failed(task_id, message)
                self._pending_failures.pop(task_id, None)
                log.info("任务 %s 失败状态补提交成功", task_id)
            except LockTimeout:
                continue

        # 2) 接续「文件已就绪、状态仍 running」的发布。
        for task in self.store.list_tasks(limit=500):
            if task["status"] != "running":
                continue
            task_id = task["id"]
            if task_id in self._publishing:
                continue  # 主流程正在发布同一任务，巡检不插手。
            published_name = f"{task['line']}_{task['date']}_{task_id}.csv"
            published_path = config.PUBLISHED_DIR / published_name
            staged = self._staged_path(task_id)
            if published_path.exists():
                # 文件已在发布目录但状态未提交成功：补提交即可，不重复移动。
                try:
                    self.store.mark_published(task_id, published_name)
                    shutil.rmtree(config.WORK_DIR / task_id, ignore_errors=True)
                    log.info("任务 %s 检测到已发布文件，补提交发布状态", task_id)
                except LockTimeout:
                    continue
            elif staged.exists():
                log.info("巡检发现任务 %s 存在待发布暂存文件，自动接续发布", task_id)
                work_dir = config.WORK_DIR / task_id
                cancel_event = self.store._event(task_id)
                self._publish_guarded(task, work_dir, staged, cancel_event)
            else:
                # 进程重启后仅有工作目录下载：复用它完成校验与发布，不重新导出。
                work_dir = config.WORK_DIR / task_id
                downloaded = self._find_download(work_dir)
                if downloaded is not None:
                    log.info("巡检发现任务 %s 存在已下载 CSV，复用并接续发布", task_id)
                    cancel_event = self.store._event(task_id)
                    self._publish_guarded(task, work_dir, downloaded, cancel_event)

    # ---- 工具 ------------------------------------------------------------
    def before_publish_commit(self, task_id: str, published_path: Path) -> None:
        """发布状态提交前的钩子，默认空操作；测试可覆写以注入写锁等场景。"""

    @staticmethod
    def _staged_path(task_id: str) -> Path:
        return config.STAGING_DIR / f"{task_id}.csv"

    @staticmethod
    def _find_download(work_dir: Path) -> Path | None:
        csvs = sorted(work_dir.glob("*.csv"))
        return csvs[0] if csvs else None

    def _handle_cancel(self, task_id: str, work_dir: Path) -> None:
        # 先清掉未发布的工作/暂存文件，再让下一个任务使用门户会话。
        shutil.rmtree(work_dir, ignore_errors=True)
        self._staged_path(task_id).unlink(missing_ok=True)
        task = self.store.get_task(task_id)
        if task and task["status"] != "cancelled":
            # 若文件已进入发布目录但取消抢先，删除孤儿文件，保证不可下载。
            if task["status"] != "published":
                orphan = config.PUBLISHED_DIR / (
                    f"{task['line']}_{task['date']}_{task_id}.csv"
                )
                orphan.unlink(missing_ok=True)
            try:
                self.store.mark_cancelled(
                    task_id, "已停止浏览器操作并删除未发布文件，门户会话已释放"
                )
            except LockTimeout:
                self._pending_cancels.add(task_id)
                log.warning("任务 %s 取消状态提交被写锁阻塞，巡检将自动接续", task_id)
        log.info("任务 %s 取消完成，允许下一个任务接管会话", task_id)

    def _handle_failure(self, task_id: str, message: str) -> None:
        # 工作目录保留以便排查，但不会对外提供下载。
        try:
            self.store.mark_failed(task_id, message)
        except LockTimeout:
            self._pending_failures[task_id] = message
            log.warning("任务 %s 失败状态提交被写锁阻塞，巡检将自动接续", task_id)
        else:
            log.info("任务 %s 失败（可重试）：%s", task_id, message)
