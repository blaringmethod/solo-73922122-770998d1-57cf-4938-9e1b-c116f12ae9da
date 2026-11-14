"""SQLite 存取层。

两个 API 进程共用同一个 SQLite 库文件：
- WAL 模式：读写互不阻塞；
- 写事务一律 BEGIN IMMEDIATE：跨进程串行化写入与重算，保证两进程查询结论一致；
- busy_timeout + 重试：消除瞬时写锁冲突。
"""
from __future__ import annotations

import os
import sqlite3
import time
from contextlib import contextmanager

DEFAULT_DB_PATH = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "data", "confluence.db")
)


def db_path() -> str:
    """每次连接时读取环境变量，便于测试与部署切换库文件。"""
    return os.environ.get("DB_PATH", DEFAULT_DB_PATH)


SCHEMA = """
CREATE TABLE IF NOT EXISTS revisions (
    revision_id      TEXT PRIMARY KEY,          -- 全局唯一修订号
    observation_id   TEXT NOT NULL,             -- 观测号
    species          TEXT,                      -- 物种（删除修订为空）
    individual_count INTEGER,                   -- 数量（删除修订为空）
    is_delete        INTEGER NOT NULL DEFAULT 0,-- 删除标记
    content          TEXT NOT NULL,             -- 规范化内容（幂等比对用）
    received_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);
CREATE INDEX IF NOT EXISTS idx_revisions_obs ON revisions(observation_id);

CREATE TABLE IF NOT EXISTS revision_parents (
    revision_id TEXT NOT NULL,                  -- 子修订
    parent_id   TEXT NOT NULL,                  -- 直接父修订
    PRIMARY KEY (revision_id, parent_id)
);
CREATE INDEX IF NOT EXISTS idx_revision_parents_parent ON revision_parents(parent_id);

CREATE TABLE IF NOT EXISTS observations (
    observation_id   TEXT PRIMARY KEY,
    status           TEXT NOT NULL,             -- pending | valid | conflict
    tips             TEXT NOT NULL,             -- JSON 数组：当前分支（无后继修订）
    missing          TEXT NOT NULL,             -- JSON 数组：未齐祖先
    eff_revision_id  TEXT,                      -- 有效值来源修订（单分支时）
    eff_species      TEXT,
    eff_count        INTEGER,
    eff_deleted      INTEGER NOT NULL DEFAULT 0,
    version          INTEGER NOT NULL           -- 状态版本，每次变化 +1
);

-- 交付切片：一次发布时冻结的全部内容；写入后不再随观测修订变化。
CREATE TABLE IF NOT EXISTS deliveries (
    delivery_id     TEXT PRIMARY KEY,           -- 交付号（全局唯一）
    observation_ids TEXT NOT NULL,              -- JSON 数组：提交时的观测号及顺序
    created_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);

CREATE TABLE IF NOT EXISTS delivery_items (
    delivery_id        TEXT NOT NULL,
    position           INTEGER NOT NULL,        -- 保持提交时的观测号顺序
    observation_id     TEXT NOT NULL,
    revision_id        TEXT NOT NULL,           -- 固化时的有效修订号（当前分支）
    tips               TEXT NOT NULL,           -- JSON 数组：固化时的当前分支
    species            TEXT,
    individual_count   INTEGER,
    is_delete          INTEGER NOT NULL DEFAULT 0,
    obs_version        INTEGER NOT NULL,        -- 固化时观测状态版本
    PRIMARY KEY (delivery_id, position),
    UNIQUE (delivery_id, observation_id),
    FOREIGN KEY (delivery_id) REFERENCES deliveries(delivery_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_delivery_items_obs ON delivery_items(observation_id);
"""


def connect() -> sqlite3.Connection:
    conn = sqlite3.connect(db_path(), timeout=30, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout = 30000")
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA synchronous = NORMAL")
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def init_db() -> None:
    path = db_path()
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    last: Exception | None = None
    for _ in range(50):  # 两个进程同时首启建库时的短暂互斥
        try:
            conn = connect()
            try:
                conn.executescript(SCHEMA)
            finally:
                conn.close()
            return
        except sqlite3.OperationalError as exc:
            last = exc
            time.sleep(0.1)
    raise last  # type: ignore[misc]


@contextmanager
def write_tx():
    """写事务：BEGIN IMMEDIATE 立即取写锁，异常即回滚，不留部分记录。"""
    conn = connect()
    try:
        for attempt in range(50):
            try:
                conn.execute("BEGIN IMMEDIATE")
                break
            except sqlite3.OperationalError as exc:
                if "locked" not in str(exc).lower() or attempt == 49:
                    raise
                time.sleep(min(0.05 * (attempt + 1), 1.0))
        try:
            yield conn
            conn.execute("COMMIT")
        except BaseException:
            conn.execute("ROLLBACK")
            raise
    finally:
        conn.close()


@contextmanager
def read_tx():
    """只读事务：显式 BEGIN 固定一个一致性快照。

    WAL 模式下读快照不阻塞其他进程写入；同一事务内的多次查询看到同一个
    已提交状态（交付切片据此从同一状态读取全部所选观测）。
    """
    conn = connect()
    try:
        conn.execute("BEGIN")
        try:
            yield conn
            conn.execute("COMMIT")
        except BaseException:
            conn.execute("ROLLBACK")
            raise
    finally:
        conn.close()
