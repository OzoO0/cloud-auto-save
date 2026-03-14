# -*- coding: utf-8 -*-
"""SQLite 数据库操作层 - 管理同步记录和任务状态"""

import os
import time
import sqlite3
import threading
import logging

logger = logging.getLogger("sync.db")


class SyncDB:
    """线程安全的 SQLite 数据库操作类"""

    def __init__(self, db_path):
        self.db_path = db_path
        self._lock = threading.Lock()
        self._init_db()

    def _get_conn(self):
        """获取数据库连接（每次调用新建，确保线程安全）"""
        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        return conn

    def _init_db(self):
        """初始化数据库表和索引"""
        db_dir = os.path.dirname(self.db_path)
        if db_dir and not os.path.exists(db_dir):
            os.makedirs(db_dir)

        with self._lock:
            conn = self._get_conn()
            try:
                conn.executescript("""
                    CREATE TABLE IF NOT EXISTS sync_records (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        task_id TEXT NOT NULL,
                        file_path TEXT NOT NULL,
                        file_name TEXT NOT NULL,
                        file_name_no_ext TEXT,
                        file_size INTEGER,
                        file_mtime REAL,
                        file_md5 TEXT,
                        dest_path TEXT,
                        sync_time REAL NOT NULL,
                        status TEXT DEFAULT 'success',
                        message TEXT,
                        UNIQUE(task_id, file_path)
                    );

                    CREATE INDEX IF NOT EXISTS idx_task_id
                        ON sync_records(task_id);
                    CREATE INDEX IF NOT EXISTS idx_task_file_name
                        ON sync_records(task_id, file_name);
                    CREATE INDEX IF NOT EXISTS idx_task_file_no_ext
                        ON sync_records(task_id, file_name_no_ext);
                    CREATE INDEX IF NOT EXISTS idx_task_md5
                        ON sync_records(task_id, file_md5);

                    CREATE TABLE IF NOT EXISTS file_md5_cache (
                        file_path TEXT PRIMARY KEY,
                        file_size INTEGER NOT NULL,
                        file_mtime REAL NOT NULL,
                        md5 TEXT NOT NULL,
                        md5_type TEXT NOT NULL DEFAULT 'full',
                        updated_at REAL NOT NULL
                    );

                    CREATE TABLE IF NOT EXISTS sync_task_status (
                        task_id TEXT PRIMARY KEY,
                        status TEXT DEFAULT 'idle',
                        last_run_time REAL,
                        last_run_result TEXT,
                        files_synced INTEGER DEFAULT 0,
                        files_skipped INTEGER DEFAULT 0,
                        files_failed INTEGER DEFAULT 0,
                        lock_time REAL
                    );
                """)
                self._ensure_task_status_columns(conn)
                conn.commit()
                logger.info(f"同步数据库已初始化: {self.db_path}")
            except Exception as e:
                logger.error(f"初始化数据库失败: {e}")
                raise
            finally:
                conn.close()

    def _ensure_task_status_columns(self, conn):
        cols = set()
        for row in conn.execute("PRAGMA table_info(sync_task_status)").fetchall():
            cols.add(row[1])
        required = {
            "last_run_start": "REAL",
            "last_run_end": "REAL",
            "last_run_trigger": "TEXT",
            "last_run_error": "TEXT",
            "data": "TEXT",
        }
        for name, typ in required.items():
            if name not in cols:
                conn.execute(f"ALTER TABLE sync_task_status ADD COLUMN {name} {typ}")

    # ========== 任务锁管理 ==========

    def cleanup_stale_locks(self):
        """
        清理所有残留的任务锁（仅在启动时调用）。
        程序刚启动时不可能有任何任务在运行，所有 running 状态都是
        上次进程异常中断遗留的，无条件全部重置为 idle。
        """
        with self._lock:
            conn = self._get_conn()
            try:
                cursor = conn.execute(
                    """UPDATE sync_task_status
                       SET status = 'idle', lock_time = NULL
                       WHERE status = 'running'"""
                )
                released = cursor.rowcount
                conn.commit()
                if released:
                    logger.warning(f"启动清理：释放了 {released} 个残留任务锁")
                return released
            except Exception as e:
                logger.error(f"清理残留锁失败: {e}")
                return 0
            finally:
                conn.close()

    def acquire_lock(self, task_id, timeout=1800):
        """
        获取任务锁。
        如果任务已在运行且未超时则拒绝，超时则强制释放后重新获取。
        返回 True 表示成功获取锁。
        """
        with self._lock:
            conn = self._get_conn()
            try:
                row = conn.execute(
                    "SELECT status, lock_time FROM sync_task_status WHERE task_id = ?",
                    (task_id,),
                ).fetchone()

                now = time.time()

                if row and row["status"] == "running":
                    lock_time = row["lock_time"] or 0
                    if now - lock_time < timeout:
                        logger.warning(f"任务 {task_id} 正在运行中，拒绝重复执行")
                        return False
                    else:
                        logger.warning(f"任务 {task_id} 锁已超时({timeout}s)，强制释放")

                conn.execute(
                    """INSERT INTO sync_task_status (task_id, status, lock_time)
                       VALUES (?, 'running', ?)
                       ON CONFLICT(task_id) DO UPDATE SET
                           status = 'running',
                           lock_time = ?""",
                    (task_id, now, now),
                )
                conn.commit()
                logger.info(f"任务 {task_id} 已获取锁")
                return True
            except Exception as e:
                logger.error(f"获取锁失败: {e}")
                return False
            finally:
                conn.close()

    def release_lock(self, task_id):
        """释放任务锁"""
        with self._lock:
            conn = self._get_conn()
            try:
                conn.execute(
                    """UPDATE sync_task_status
                       SET status = 'idle', lock_time = NULL
                       WHERE task_id = ?""",
                    (task_id,),
                )
                conn.commit()
                logger.info(f"任务 {task_id} 已释放锁")
            except Exception as e:
                logger.error(f"释放锁失败: {e}")
            finally:
                conn.close()

    def force_release_lock(self, task_id):
        """强制释放任务锁（用于手动解锁）"""
        self.release_lock(task_id)
        logger.warning(f"任务 {task_id} 锁已被强制释放")

    # ========== 任务状态管理 ==========

    def append_task_sse_data(self, task_id, sse_chunk):
        if not task_id or not sse_chunk:
            return
        with self._lock:
            conn = self._get_conn()
            try:
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "INSERT OR IGNORE INTO sync_task_status (task_id) VALUES (?)",
                    (task_id,),
                )
                conn.execute(
                    """UPDATE sync_task_status
                       SET data = COALESCE(data, '') || ?
                       WHERE task_id = ?""",
                    (sse_chunk, task_id),
                )
                conn.commit()
            except Exception as e:
                try:
                    conn.rollback()
                except Exception:
                    pass
                logger.warning(f"追加任务 SSE 数据失败: {e}")
            finally:
                conn.close()

    def update_task_result(self, task_id, result, synced=0, skipped=0, failed=0):
        """更新任务执行结果"""
        with self._lock:
            conn = self._get_conn()
            try:
                conn.execute(
                    """UPDATE sync_task_status
                       SET last_run_time = ?,
                           last_run_result = ?,
                           files_synced = ?,
                           files_skipped = ?,
                           files_failed = ?
                       WHERE task_id = ?""",
                    (time.time(), result, synced, skipped, failed, task_id),
                )
                conn.commit()
            except Exception as e:
                logger.error(f"更新任务结果失败: {e}")
            finally:
                conn.close()

    def update_task_progress(self, task_id, synced, skipped, failed):
        """更新任务实时进度"""
        with self._lock:
            conn = self._get_conn()
            try:
                conn.execute(
                    """UPDATE sync_task_status
                       SET files_synced = ?,
                           files_skipped = ?,
                           files_failed = ?
                       WHERE task_id = ?""",
                    (synced, skipped, failed, task_id),
                )
                conn.commit()
            except Exception as e:
                logger.warning(f"更新任务进度失败: {e}")
            finally:
                conn.close()

    def update_task_start(self, task_id, start_ts, trigger):
        with self._lock:
            conn = self._get_conn()
            try:
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    """UPDATE sync_task_status
                       SET last_run_start = ?,
                           last_run_trigger = ?,
                           last_run_error = NULL,
                           data = NULL
                       WHERE task_id = ?""",
                    (start_ts, trigger, task_id),
                )
                conn.commit()
            except Exception as e:
                try:
                    conn.rollback()
                except Exception:
                    pass
                logger.warning(f"更新任务开始信息失败: {e}")
            finally:
                conn.close()

    def update_task_snapshot(self, task_id, snapshot, sse_record, error_summary=None):
        with self._lock:
            conn = self._get_conn()
            try:
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    """UPDATE sync_task_status
                       SET last_run_time = ?,
                           last_run_end = ?,
                           last_run_result = ?,
                           files_synced = ?,
                           files_skipped = ?,
                           files_failed = ?,
                           last_run_error = ?,
                           data = ?
                       WHERE task_id = ?""",
                    (
                        snapshot.get("ended_at"),
                        snapshot.get("ended_at"),
                        snapshot.get("result"),
                        snapshot.get("synced", 0),
                        snapshot.get("skipped", 0),
                        snapshot.get("failed", 0),
                        error_summary,
                        sse_record,
                        task_id,
                    ),
                )
                conn.commit()
            except Exception as e:
                try:
                    conn.rollback()
                except Exception:
                    pass
                logger.warning(f"更新任务快照失败: {e}")
            finally:
                conn.close()

    def get_task_status(self, task_id, include_data=False):
        """获取指定任务状态"""
        conn = self._get_conn()
        try:
            if include_data:
                row = conn.execute(
                    "SELECT * FROM sync_task_status WHERE task_id = ?",
                    (task_id,),
                ).fetchone()
            else:
                row = conn.execute(
                    """SELECT task_id, status, last_run_time, last_run_result,
                              files_synced, files_skipped, files_failed, lock_time,
                              last_run_start, last_run_end, last_run_trigger, last_run_error
                       FROM sync_task_status
                       WHERE task_id = ?""",
                    (task_id,),
                ).fetchone()
            return dict(row) if row else None
        finally:
            conn.close()

    def get_all_task_status(self, include_data=False):
        """获取所有任务状态，返回 {task_id: status_dict}"""
        conn = self._get_conn()
        try:
            if include_data:
                rows = conn.execute("SELECT * FROM sync_task_status").fetchall()
            else:
                rows = conn.execute(
                    """SELECT task_id, status, last_run_time, last_run_result,
                              files_synced, files_skipped, files_failed, lock_time,
                              last_run_start, last_run_end, last_run_trigger, last_run_error
                       FROM sync_task_status"""
                ).fetchall()
            return {row["task_id"]: dict(row) for row in rows}
        finally:
            conn.close()

    # ========== 同步记录管理 ==========

    def is_file_synced(self, task_id, file_path, file_name, match_mode, file_md5=None):
        """
        判断文件是否已同步。
        match_mode: full_name / name_only / md5
        所有模式均按 file_path（含子目录）精确匹配，避免不同子目录下同名文件互相阻塞。
        """
        conn = self._get_conn()
        try:
            if match_mode == "full_name":
                row = conn.execute(
                    """SELECT id FROM sync_records
                       WHERE task_id = ? AND file_path = ? AND status = 'success'
                       LIMIT 1""",
                    (task_id, file_path),
                ).fetchone()

            elif match_mode == "name_only":
                # name_only 按路径检查：同路径下去扩展名匹配
                name_no_ext = os.path.splitext(file_name)[0]
                # 提取当前文件所在目录前缀
                dir_prefix = file_path.rsplit(os.sep, 1)[0] + os.sep if os.sep in file_path else ""
                dir_prefix_fwd = file_path.rsplit("/", 1)[0] + "/" if "/" in file_path else ""
                row = conn.execute(
                    """SELECT id FROM sync_records
                       WHERE task_id = ? AND file_name_no_ext = ?
                       AND (file_path LIKE ? OR file_path LIKE ?)
                       AND status = 'success'
                       LIMIT 1""",
                    (task_id, name_no_ext, dir_prefix + "%", dir_prefix_fwd + "%"),
                ).fetchone()

            elif match_mode == "md5":
                if not file_md5:
                    return False
                row = conn.execute(
                    """SELECT id FROM sync_records
                       WHERE task_id = ? AND file_path = ? AND file_md5 = ? AND status = 'success'
                       LIMIT 1""",
                    (task_id, file_path, file_md5),
                ).fetchone()
            else:
                return False

            return row is not None
        finally:
            conn.close()

    def add_sync_record(
        self, task_id, file_path, file_name, file_size=None,
        file_mtime=None, file_md5=None, dest_path=None,
        status="success", message=None
    ):
        """插入或更新同步记录（UPSERT）"""
        file_name_no_ext = os.path.splitext(file_name)[0]
        with self._lock:
            conn = self._get_conn()
            try:
                conn.execute(
                    """INSERT INTO sync_records
                       (task_id, file_path, file_name, file_name_no_ext,
                        file_size, file_mtime, file_md5, dest_path,
                        sync_time, status, message)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                       ON CONFLICT(task_id, file_path) DO UPDATE SET
                           file_name = excluded.file_name,
                           file_name_no_ext = excluded.file_name_no_ext,
                           file_size = excluded.file_size,
                           file_mtime = excluded.file_mtime,
                           file_md5 = excluded.file_md5,
                           dest_path = excluded.dest_path,
                           sync_time = excluded.sync_time,
                           status = excluded.status,
                           message = excluded.message""",
                    (
                        task_id, file_path, file_name, file_name_no_ext,
                        file_size, file_mtime, file_md5, dest_path,
                        time.time(), status, message,
                    ),
                )
                conn.commit()
            except Exception as e:
                logger.error(f"写入同步记录失败: {e}")
            finally:
                conn.close()

    def get_sync_records(self, task_id, page=1, page_size=20):
        """分页查询同步记录"""
        conn = self._get_conn()
        try:
            offset = (page - 1) * page_size
            rows = conn.execute(
                """SELECT * FROM sync_records
                   WHERE task_id = ?
                   ORDER BY sync_time DESC
                   LIMIT ? OFFSET ?""",
                (task_id, page_size, offset),
            ).fetchall()
            return [dict(row) for row in rows]
        finally:
            conn.close()

    def get_all_sync_records(self, task_id, limit=2000):
        """获取指定任务的所有同步记录（树状视图用，按路径排序，带上限保护）"""
        conn = self._get_conn()
        try:
            rows = conn.execute(
                """SELECT * FROM sync_records
                   WHERE task_id = ?
                   ORDER BY file_path ASC
                   LIMIT ?""",
                (task_id, limit),
            ).fetchall()
            return [dict(row) for row in rows]
        finally:
            conn.close()

    def get_records_count(self, task_id):
        """获取指定任务的同步记录总数"""
        conn = self._get_conn()
        try:
            row = conn.execute(
                "SELECT COUNT(*) as cnt FROM sync_records WHERE task_id = ?",
                (task_id,),
            ).fetchone()
            return row["cnt"] if row else 0
        finally:
            conn.close()

    def cleanup_records(self, task_id, before_days=None):
        """
        清理同步记录。
        before_days: 清理多少天前的记录，None 表示全部清理。
        返回清理的记录数。
        """
        deleted = 0
        with self._lock:
            conn = self._get_conn()
            try:
                if before_days is not None:
                    cutoff = time.time() - before_days * 86400
                    cursor = conn.execute(
                        "DELETE FROM sync_records WHERE task_id = ? AND sync_time < ?",
                        (task_id, cutoff),
                    )
                else:
                    cursor = conn.execute(
                        "DELETE FROM sync_records WHERE task_id = ?",
                        (task_id,),
                    )
                deleted = cursor.rowcount
                conn.commit()
                logger.info(f"已清理任务 {task_id} 的 {deleted} 条同步记录")
            except Exception as e:
                logger.error(f"清理记录失败: {e}")
            finally:
                conn.close()

        # 在锁外连带清理过期 MD5 缓存（避免死锁）
        try:
            self.cleanup_stale_md5_cache()
        except Exception:
            pass

        return deleted

    # ========== MD5 缓存管理 ==========

    def get_md5_cache(self, file_path, file_size, file_mtime):
        """查询单条 MD5 缓存（mtime+size 前置过滤）"""
        conn = self._get_conn()
        try:
            row = conn.execute(
                """SELECT md5, md5_type FROM file_md5_cache
                   WHERE file_path = ? AND file_size = ? AND file_mtime = ?""",
                (file_path, file_size, file_mtime),
            ).fetchone()
            return {"md5": row["md5"], "md5_type": row["md5_type"]} if row else None
        finally:
            conn.close()

    def batch_get_md5_cache(self, file_entries):
        """
        批量查询 MD5 缓存。
        file_entries: [{"full_path": ..., "size": ..., "mtime": ...}, ...]
        返回: {file_path: {"md5": ..., "md5_type": ...}}（仅包含命中的条目）
        """
        if not file_entries:
            return {}

        result = {}
        conn = self._get_conn()
        try:
            # 构建路径到 (size, mtime) 的快速查找表
            lookup = {fi["full_path"]: (fi["size"], fi["mtime"]) for fi in file_entries}
            paths = list(lookup.keys())

            # 分批查询，每批 500 条
            batch_size = 500
            for i in range(0, len(paths), batch_size):
                batch = paths[i:i + batch_size]
                placeholders = ",".join("?" * len(batch))
                rows = conn.execute(
                    f"""SELECT file_path, file_size, file_mtime, md5, md5_type
                        FROM file_md5_cache
                        WHERE file_path IN ({placeholders})""",
                    batch,
                ).fetchall()

                for row in rows:
                    fp = row["file_path"]
                    expected_size, expected_mtime = lookup.get(fp, (None, None))
                    # Python 侧严格校验 size + mtime
                    if (expected_size is not None
                            and row["file_size"] == expected_size
                            and row["file_mtime"] == expected_mtime):
                        result[fp] = {"md5": row["md5"], "md5_type": row["md5_type"]}

            return result
        except Exception as e:
            logger.error(f"批量查询 MD5 缓存失败: {e}")
            return result
        finally:
            conn.close()

    def batch_set_md5_cache(self, entries):
        """
        批量写入 MD5 缓存。
        entries: [{"file_path": ..., "file_size": ..., "file_mtime": ...,
                   "md5": ..., "md5_type": ...}, ...]
        """
        if not entries:
            return
        now = time.time()
        with self._lock:
            conn = self._get_conn()
            try:
                conn.executemany(
                    """INSERT INTO file_md5_cache
                       (file_path, file_size, file_mtime, md5, md5_type, updated_at)
                       VALUES (?, ?, ?, ?, ?, ?)
                       ON CONFLICT(file_path) DO UPDATE SET
                           file_size = excluded.file_size,
                           file_mtime = excluded.file_mtime,
                           md5 = excluded.md5,
                           md5_type = excluded.md5_type,
                           updated_at = excluded.updated_at""",
                    [
                        (e["file_path"], e["file_size"], e["file_mtime"],
                         e["md5"], e.get("md5_type", "full"), now)
                        for e in entries
                    ],
                )
                conn.commit()
                logger.debug(f"已写入 {len(entries)} 条 MD5 缓存")
            except Exception as e:
                logger.error(f"批量写入 MD5 缓存失败: {e}")
            finally:
                conn.close()

    def cleanup_stale_md5_cache(self, max_age_days=30):
        """清理过期的 MD5 缓存记录"""
        with self._lock:
            conn = self._get_conn()
            try:
                cutoff = time.time() - max_age_days * 86400
                cursor = conn.execute(
                    "DELETE FROM file_md5_cache WHERE updated_at < ?",
                    (cutoff,),
                )
                deleted = cursor.rowcount
                conn.commit()
                if deleted:
                    logger.info(f"已清理 {deleted} 条过期 MD5 缓存")
                return deleted
            except Exception as e:
                logger.error(f"清理 MD5 缓存失败: {e}")
                return 0
            finally:
                conn.close()

    def delete_sync_record(self, record_id):
        """删除单条同步记录，返回是否成功"""
        with self._lock:
            conn = self._get_conn()
            try:
                cursor = conn.execute(
                    "DELETE FROM sync_records WHERE id = ?", (record_id,)
                )
                conn.commit()
                deleted = cursor.rowcount
                if deleted:
                    logger.info(f"已删除同步记录 id={record_id}")
                else:
                    logger.warning(f"未找到同步记录 id={record_id}")
                return deleted > 0
            except Exception as e:
                logger.error(f"删除同步记录失败: {e}")
                return False
            finally:
                conn.close()

    def delete_task_records(self, task_id):
        """删除指定任务的所有记录和状态"""
        with self._lock:
            conn = self._get_conn()
            try:
                conn.execute(
                    "DELETE FROM sync_records WHERE task_id = ?", (task_id,)
                )
                conn.execute(
                    "DELETE FROM sync_task_status WHERE task_id = ?", (task_id,)
                )
                conn.commit()
                logger.info(f"已删除任务 {task_id} 的所有数据")
            except Exception as e:
                logger.error(f"删除任务数据失败: {e}")
            finally:
                conn.close()

    def batch_delete_records(self, record_ids):
        """批量删除同步记录，返回删除的记录数"""
        if not record_ids:
            return 0
        total_deleted = 0
        with self._lock:
            conn = self._get_conn()
            try:
                # 分批删除，每批 500 个，避免 SQLite 参数限制
                batch_size = 500
                for i in range(0, len(record_ids), batch_size):
                    batch = record_ids[i:i + batch_size]
                    placeholders = ",".join("?" * len(batch))
                    cursor = conn.execute(
                        f"DELETE FROM sync_records WHERE id IN ({placeholders})",
                        batch,
                    )
                    total_deleted += cursor.rowcount
                conn.commit()
                if total_deleted:
                    logger.info(f"已批量删除 {total_deleted} 条同步记录")
            except Exception as e:
                logger.error(f"批量删除同步记录失败: {e}")
            finally:
                conn.close()
        return total_deleted
