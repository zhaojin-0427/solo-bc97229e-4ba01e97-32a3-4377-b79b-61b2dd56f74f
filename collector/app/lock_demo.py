"""演示用「外部写锁」注入器。

质检工程师可在网页上模拟「另一连接持有任务库写锁」的场景，用于现场验证：
写锁占用期间网页与状态查询照常响应、worker 不退出，解锁后队列自动接续。

实现方式与真实的第三方连接完全一致：打开一个独立 SQLite 连接并在后台线程
里开启 IMMEDIATE 写事务，到时自动提交释放。
"""
from __future__ import annotations

import logging
import sqlite3
import threading
import time

from . import config

log = logging.getLogger("collector.lock_demo")

MAX_HOLD_SECONDS = 30.0


class LockDemo:
    def __init__(self, db_path: str | None = None):
        self._db_path = db_path or str(config.DB_PATH)
        self._lock = threading.Lock()
        self._holder: threading.Thread | None = None
        self._release = threading.Event()
        self._until = 0.0

    @property
    def active(self) -> bool:
        with self._lock:
            return self._holder is not None and self._holder.is_alive()

    def remaining(self) -> float:
        return max(0.0, self._until - time.monotonic()) if self.active else 0.0

    def acquire(self, seconds: float) -> float:
        """占用写锁指定时长；已占用时返回剩余秒数而不是重复加锁。"""
        seconds = max(0.5, min(MAX_HOLD_SECONDS, float(seconds)))
        with self._lock:
            if self.active:
                return self.remaining()
            self._release = threading.Event()
            self._until = time.monotonic() + seconds
            holder = threading.Thread(
                target=self._hold, args=(seconds,), name="demo-write-lock", daemon=True
            )
            self._holder = holder
        holder.start()
        log.warning("演示写锁已开启，将持续 %.1f 秒", seconds)
        return seconds

    def release(self) -> None:
        with self._lock:
            if self._holder is not None:
                self._release.set()

    def _hold(self, seconds: float) -> None:
        # 独立连接、独立线程，模拟进程外的另一连接。
        conn = sqlite3.connect(self._db_path, timeout=1.0, check_same_thread=False)
        try:
            conn.execute("PRAGMA busy_timeout=500")
            conn.execute("BEGIN IMMEDIATE")
            log.info("演示连接已取得写锁")
            self._release.wait(seconds)
            conn.commit()
            log.info("演示写锁已释放，worker 应自动接续队列")
        except sqlite3.OperationalError as exc:
            log.warning("演示写锁获取失败：%s", exc)
        finally:
            conn.close()
