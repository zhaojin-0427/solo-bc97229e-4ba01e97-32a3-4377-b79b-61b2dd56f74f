"""任务模型与 SQLite 存储。

任务状态机：
    queued -> running -> published（成功发布）
                         -> failed（可重试）
              queued/running -> cancelled（可重试）
重试会新建一条任务（retry_of 指向上一条），保证历史可追溯。

锁策略（修复临时写锁导致采集中断）：
- 数据库开启 WAL：读写不互相阻塞，网页与状态查询在其他连接持有写锁时仍可响应；
- 读连接与写连接分离，读连接以只读模式打开，永远不参与写锁竞争；
- 写连接设置 busy_timeout：遇到其他连接的写锁时在驱动层等待而非立刻抛
  ``database is locked``，锁释放后自动接续；
- 超过 busy_timeout 仍未拿到锁时抛出 :class:`LockTimeout`，由 worker 捕获后
  保持任务原状态、稍后重试，绝不让采集线程退出，也不把任务误判为失败。
"""
from __future__ import annotations

import logging
import sqlite3
import threading
import time
import uuid
from datetime import datetime, timezone
from typing import Any

from . import config

log = logging.getLogger("collector.models")

# 终态之外的状态
ACTIVE_STATES = ("queued", "running", "cancelling")
FINAL_STATES = ("published", "failed", "cancelled")
RETRYABLE_STATES = ("failed", "cancelled")

_SCHEMA = """
CREATE TABLE IF NOT EXISTS tasks (
    id              TEXT PRIMARY KEY,
    line            TEXT NOT NULL,
    date            TEXT NOT NULL,
    status          TEXT NOT NULL,
    message         TEXT NOT NULL DEFAULT '',
    created_at      TEXT NOT NULL,
    started_at      TEXT,
    finished_at     TEXT,
    attempts        INTEGER NOT NULL DEFAULT 1,
    retry_of        TEXT,
    published_file  TEXT
);
CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status);
CREATE INDEX IF NOT EXISTS idx_tasks_created ON tasks(created_at);
"""


class LockTimeout(Exception):
    """等待其他连接释放写锁超过配置上限，调用方应保持现状稍后重试。"""


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


class TaskStore:
    """SQLite 存储。

    - 所有写操作经进程内 ``_lock`` 串行化（单 worker + Flask 请求线程）；
    - 读操作走独立只读连接，WAL 下不与写操作互斥，锁等待期间网页照常响应；
    - 每个写操作都带锁重试，临时写锁只会造成等待，不会中断采集。
    """

    def __init__(self, db_path: str | None = None):
        config.DATA_DIR.mkdir(parents=True, exist_ok=True)
        config.PUBLISHED_DIR.mkdir(parents=True, exist_ok=True)
        config.STAGING_DIR.mkdir(parents=True, exist_ok=True)
        config.WORK_DIR.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        path = db_path or str(config.DB_PATH)

        # 写连接：WAL + busy_timeout，写锁竞争时在驱动层排队等待。
        self._conn = sqlite3.connect(
            path, check_same_thread=False, timeout=config.DB_BUSY_TIMEOUT_SECONDS
        )
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute(
            f"PRAGMA busy_timeout={int(config.DB_BUSY_TIMEOUT_SECONDS * 1000)}"
        )
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.executescript(_SCHEMA)
        self._conn.commit()

        # 读连接：只读打开，WAL 下永远不被写锁阻塞，专门服务网页与状态查询。
        self._read_conn = sqlite3.connect(
            f"file:{path}?mode=ro",
            uri=True,
            check_same_thread=False,
            timeout=config.DB_BUSY_TIMEOUT_SECONDS,
        )
        self._read_conn.row_factory = sqlite3.Row
        self._read_conn.execute(
            f"PRAGMA busy_timeout={int(config.DB_BUSY_TIMEOUT_SECONDS * 1000)}"
        )

        # 进程内取消事件：worker 轮询它来中断浏览器操作。
        self._cancel_events: dict[str, threading.Event] = {}

    # ---- 基础工具 -------------------------------------------------------
    @property
    def lock(self) -> threading.RLock:
        return self._lock

    def _event(self, task_id: str) -> threading.Event:
        event = self._cancel_events.get(task_id)
        if event is None:
            event = threading.Event()
            self._cancel_events[task_id] = event
        return event

    def is_cancel_set(self, task_id: str) -> bool:
        return self._event(task_id).is_set()

    def _write(self, operation: str, fn):
        """在进程锁内执行写操作，并对外部写锁做重试。

        锁等待期间不改变任何任务状态；始终拿不到锁时抛 :class:`LockTimeout`，
        由 worker 保留现场、下一轮继续，而不是让线程退出或判任务失败。
        """
        with self._lock:
            attempts = max(1, config.DB_LOCK_RETRIES + 1)
            last_exc: Exception | None = None
            for attempt in range(1, attempts + 1):
                try:
                    return fn(self._conn)
                except sqlite3.OperationalError as exc:
                    if "locked" not in str(exc).lower():
                        raise
                    last_exc = exc
                    log.warning(
                        "数据库写锁占用，%s 等待后重试（第 %s/%s 次）",
                        operation, attempt, attempts,
                    )
                    # busy_timeout 已在驱动层等过一轮；这里短暂退避后再试。
                    time.sleep(min(1.0, 0.2 * attempt))
            raise LockTimeout(f"{operation} 持续锁等待超时") from last_exc

    def create_task(self, line: str, date: str, retry_of: str | None = None,
                    attempts: int = 1) -> dict[str, Any]:
        task_id = uuid.uuid4().hex

        def _insert(conn):
            conn.execute(
                "INSERT INTO tasks (id, line, date, status, created_at, attempts, retry_of)"
                " VALUES (?, ?, ?, 'queued', ?, ?, ?)",
                (task_id, line, date, _now(), attempts, retry_of),
            )
            conn.commit()

        self._write("创建任务", _insert)
        self._event(task_id)  # 预建事件
        return self.get_task(task_id)

    def get_task(self, task_id: str) -> dict[str, Any] | None:
        # 只读连接：即便另一连接正在写（WAL），也能立即读到已提交快照。
        row = self._read_conn.execute(
            "SELECT * FROM tasks WHERE id = ?", (task_id,)
        ).fetchone()
        return dict(row) if row else None

    def list_tasks(self, limit: int = 100) -> list[dict[str, Any]]:
        rows = self._read_conn.execute(
            "SELECT * FROM tasks ORDER BY created_at DESC, id DESC LIMIT ?", (limit,)
        ).fetchall()
        return [dict(r) for r in rows]

    def claim_next(self) -> dict[str, Any] | None:
        """原子领取：单条事务把最早的 queued 任务置为 running 并返回它。

        事务被写锁挡住时只会等待/重试，不会出现「领取成功但状态没提交
        就去操作门户」的窗口——领取状态提交成功前 worker 不会触碰门户。
        """
        def _claim(conn):
            row = conn.execute(
                "SELECT * FROM tasks WHERE status = 'queued'"
                " ORDER BY created_at ASC, id ASC LIMIT 1"
            ).fetchone()
            if row is None:
                conn.commit()
                return None
            task = dict(row)
            conn.execute(
                "UPDATE tasks SET status = 'running', started_at = ?,"
                " message = '浏览器已接管门户会话，开始筛选' WHERE id = ?",
                (_now(), task["id"]),
            )
            conn.commit()
            return task

        return self._write("领取任务", _claim)

    def update(self, task_id: str, **fields: Any) -> None:
        if not fields:
            return
        columns = ", ".join(f"{k} = ?" for k in fields)

        def _update(conn):
            conn.execute(
                f"UPDATE tasks SET {columns} WHERE id = ?",
                (*fields.values(), task_id),
            )
            conn.commit()

        self._write("更新任务", _update)

    def mark_published(self, task_id: str, file_name: str) -> None:
        self.update(task_id, status="published", finished_at=_now(),
                    published_file=file_name, message="校验通过，已发布到质量库")

    def mark_failed(self, task_id: str, message: str) -> None:
        self.update(task_id, status="failed", finished_at=_now(), message=message)

    def mark_cancelled(self, task_id: str, message: str = "任务已取消") -> None:
        self.update(task_id, status="cancelled", finished_at=_now(), message=message)

    def request_cancel(self, task_id: str) -> tuple[bool, str]:
        """请求取消。返回 (是否成功, 说明)。"""
        with self._lock:
            task = self.get_task(task_id)
            if task is None:
                return False, "任务不存在"
            if task["status"] in FINAL_STATES:
                return False, f"任务已处于终态 {task['status']}，无法取消"
            self._event(task_id).set()
            if task["status"] == "queued":
                # 排队中：直接终结，不必等 worker。
                self.update(task_id, status="cancelled", finished_at=_now(),
                            message="排队中取消，未占用门户会话")
            else:
                self.update(task_id, status="cancelling",
                            message="取消请求已下发，正在停止浏览器操作")
            return True, "取消请求已受理"

    def retry(self, task_id: str) -> dict[str, Any] | None:
        with self._lock:
            task = self.get_task(task_id)
            if task is None or task["status"] not in RETRYABLE_STATES:
                return None
            return self.create_task(
                task["line"], task["date"],
                retry_of=task_id, attempts=task["attempts"] + 1,
            )

    def recover_stale(self) -> int:
        """进程重启后的状态恢复。

        - queued/cancelling 以及没有任何落盘产物的 running 任务标记为 failed
          （可重试）；
        - 已有暂存/发布文件的 running 任务保留 running，由 worker 巡检自动接续
          发布，已校验的 CSV 不会被要求重新导出。
        """
        def _recover(conn):
            rows = conn.execute(
                "SELECT id, line, date FROM tasks"
                " WHERE status IN ('queued', 'running', 'cancelling')"
            ).fetchall()
            recovered = 0
            for row in rows:
                task_id = row["id"]
                staged = config.STAGING_DIR / f"{task_id}.csv"
                published = config.PUBLISHED_DIR / (
                    f"{row['line']}_{row['date']}_{task_id}.csv"
                )
                work_download = next(
                    (config.WORK_DIR / task_id).glob("*.csv"), None
                )
                if staged.exists() or published.exists() or work_download is not None:
                    log.info("任务 %s 存在落盘产物，保留 running 等待巡检接续", task_id)
                    continue
                conn.execute(
                    "UPDATE tasks SET status = 'failed', finished_at = ?,"
                    " message = '采集服务重启，任务中断，可重试' WHERE id = ?",
                    (_now(), task_id),
                )
                recovered += 1
            conn.commit()
            return recovered

        return self._write("启动恢复", _recover)

    def close(self) -> None:
        with self._lock:
            self._conn.close()
            self._read_conn.close()
