# -*- coding: utf-8 -*-
"""SQLite 数据库操作层 - 管理同步记录和任务状态"""

import os
import time
import sqlite3
import threading
import logging
import json
import uuid

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
                    CREATE TABLE IF NOT EXISTS schema_migrations (
                        id TEXT PRIMARY KEY,
                        applied_at INTEGER NOT NULL
                    );

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

                    CREATE TABLE IF NOT EXISTS app_config (
                        key TEXT PRIMARY KEY,
                        value TEXT NOT NULL,
                        updated_at REAL NOT NULL
                    );
                """)
                self._ensure_task_status_columns(conn)
                self._apply_sql_migrations(conn)
                self._ensure_task_uid_schema(conn)
                conn.commit()
                logger.info(f"同步数据库已初始化: {self.db_path}")
            except Exception as e:
                logger.error(f"初始化数据库失败: {e}")
                raise
            finally:
                conn.close()

    def _apply_sql_migrations(self, conn):
        migrations_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "migrations")
        if not os.path.isdir(migrations_dir):
            return
        try:
            rows = conn.execute("SELECT id FROM schema_migrations").fetchall()
            applied = {row[0] for row in rows}
        except Exception:
            applied = set()

        files = []
        for name in os.listdir(migrations_dir):
            if name.endswith(".sql"):
                files.append(os.path.join(migrations_dir, name))
        files.sort()

        now = int(time.time())
        for fp in files:
            mid = os.path.basename(fp)
            if mid in applied:
                continue
            with open(fp, "r", encoding="utf-8") as f:
                sql = f.read()
            if not sql.strip():
                conn.execute(
                    "INSERT INTO schema_migrations (id, applied_at) VALUES (?, ?)",
                    (mid, now),
                )
                continue
            conn.executescript(sql)
            conn.execute(
                "INSERT INTO schema_migrations (id, applied_at) VALUES (?, ?)",
                (mid, now),
            )

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

    def _ensure_task_uid_schema(self, conn):
        row = conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'tasks' LIMIT 1"
        ).fetchone()
        if not row:
            return
        cols = {r[1] for r in conn.execute("PRAGMA table_info(tasks)").fetchall()}
        if "task_uid" not in cols:
            conn.execute("ALTER TABLE tasks ADD COLUMN task_uid TEXT")
            cols.add("task_uid")
        if {"user_id", "task_uid", "is_deleted"}.issubset(cols):
            conn.execute(
                """CREATE UNIQUE INDEX IF NOT EXISTS ux_tasks_user_task_uid_alive
                   ON tasks(user_id, task_uid)
                   WHERE is_deleted = 0 AND task_uid IS NOT NULL AND task_uid != ''"""
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS ix_tasks_user_task_uid ON tasks(user_id, task_uid)"
            )

    def get_app_config(self, key):
        conn = self._get_conn()
        try:
            row = conn.execute(
                "SELECT value FROM app_config WHERE key = ?",
                (key,),
            ).fetchone()
            return row["value"] if row else None
        finally:
            conn.close()

    def set_app_config(self, key, value):
        if key is None:
            return False
        if value is None:
            value = ""
        with self._lock:
            conn = self._get_conn()
            try:
                now = time.time()
                conn.execute(
                    """INSERT INTO app_config (key, value, updated_at)
                       VALUES (?, ?, ?)
                       ON CONFLICT(key) DO UPDATE SET
                           value = excluded.value,
                           updated_at = excluded.updated_at""",
                    (key, value, now),
                )
                conn.commit()
                return True
            except Exception as e:
                logger.warning(f"写入 app_config 失败: {e}")
                return False
            finally:
                conn.close()

    def delete_app_config(self, key):
        with self._lock:
            conn = self._get_conn()
            try:
                cursor = conn.execute(
                    "DELETE FROM app_config WHERE key = ?",
                    (key,),
                )
                conn.commit()
                return cursor.rowcount > 0
            except Exception as e:
                logger.warning(f"删除 app_config 失败: {e}")
                return False
            finally:
                conn.close()

    def list_app_config_keys(self):
        conn = self._get_conn()
        try:
            rows = conn.execute(
                "SELECT key FROM app_config ORDER BY key ASC"
            ).fetchall()
            return [row["key"] for row in rows]
        finally:
            conn.close()

    def execute_sql(self, sql, params=None):
        if not sql:
            return {"ok": False, "error": "SQL 为空"}
        sql_s = str(sql).strip()
        params = params or []
        with self._lock:
            conn = self._get_conn()
            try:
                cur = conn.execute(sql_s, params)
                cols = [d[0] for d in cur.description] if cur.description else []
                rows = cur.fetchall() if cur.description else []
                conn.commit()
                return {
                    "ok": True,
                    "columns": cols,
                    "rows": [dict(r) for r in rows] if rows else [],
                    "rowcount": cur.rowcount,
                }
            except Exception as e:
                try:
                    conn.rollback()
                except Exception:
                    pass
                return {"ok": False, "error": str(e)}
            finally:
                conn.close()

    def _is_safe_ident(self, name):
        if not name or not isinstance(name, str):
            return False
        for ch in name:
            if not (ch.isalnum() or ch == "_"):
                return False
        return True

    def _ensure_table_allowed(self, table_name):
        if not self._is_safe_ident(table_name):
            return False
        return table_name in set(self.list_tables())

    def list_tables(self):
        conn = self._get_conn()
        try:
            rows = conn.execute(
                """SELECT name
                   FROM sqlite_master
                   WHERE type='table'
                     AND name NOT LIKE 'sqlite_%'
                   ORDER BY name ASC"""
            ).fetchall()
            return [r["name"] for r in rows]
        finally:
            conn.close()

    def get_table_columns(self, table_name):
        if not self._ensure_table_allowed(table_name):
            return []
        conn = self._get_conn()
        try:
            rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
            cols = []
            for r in rows:
                cols.append(
                    {
                        "cid": r["cid"],
                        "name": r["name"],
                        "type": r["type"],
                        "notnull": int(r["notnull"]),
                        "dflt_value": r["dflt_value"],
                        "pk": int(r["pk"]),
                    }
                )
            return cols
        finally:
            conn.close()

    def get_table_meta(self, table_name):
        cols = self.get_table_columns(table_name)
        pk_cols = [c["name"] for c in cols if c.get("pk")]
        pk = pk_cols[0] if len(pk_cols) == 1 else None
        has_is_deleted = any(c["name"] == "is_deleted" for c in cols)
        has_updated_at = any(c["name"] == "updated_at" for c in cols)
        has_created_at = any(c["name"] == "created_at" for c in cols)
        return {
            "table": table_name,
            "columns": cols,
            "pk": pk,
            "pk_columns": pk_cols,
            "has_is_deleted": has_is_deleted,
            "has_updated_at": has_updated_at,
            "has_created_at": has_created_at,
        }

    def get_table_rows(self, table_name, page=1, page_size=50, include_deleted=False, order_by=None, order_dir="DESC", q=None):
        meta = self.get_table_meta(table_name)
        cols = [c["name"] for c in meta["columns"]]
        if not cols:
            return {"ok": True, "columns": [], "rows": [], "page": page, "page_size": page_size, "total": 0}

        pk = meta["pk"] or "rowid"
        if order_by and order_by in cols:
            order_col = order_by
        else:
            order_col = pk if pk in cols else cols[0]
        order_dir = "ASC" if str(order_dir).upper() == "ASC" else "DESC"

        where = []
        params = []
        if meta["has_is_deleted"] and not include_deleted:
            where.append("is_deleted = 0")
        if q:
            q_s = str(q)
            like_cols = []
            for c in meta["columns"]:
                ct = (c.get("type") or "").upper()
                if "CHAR" in ct or "TEXT" in ct or "CLOB" in ct:
                    like_cols.append(c["name"])
            if like_cols:
                parts = []
                for name in like_cols:
                    parts.append(f"{name} LIKE ?")
                    params.append(f"%{q_s}%")
                where.append("(" + " OR ".join(parts) + ")")

        where_sql = (" WHERE " + " AND ".join(where)) if where else ""
        offset = max(int(page) - 1, 0) * int(page_size)
        limit = int(page_size)

        with self._lock:
            conn = self._get_conn()
            try:
                total_row = conn.execute(f"SELECT COUNT(*) AS cnt FROM {table_name}{where_sql}", params).fetchone()
                total = int(total_row["cnt"]) if total_row else 0
                rows = conn.execute(
                    f"SELECT * FROM {table_name}{where_sql} ORDER BY {order_col} {order_dir} LIMIT ? OFFSET ?",
                    [*params, limit, offset],
                ).fetchall()
                return {
                    "ok": True,
                    "columns": cols,
                    "rows": [dict(r) for r in rows],
                    "page": int(page),
                    "page_size": int(page_size),
                    "total": total,
                }
            finally:
                conn.close()

    def upsert_table_row(self, table_name, row_data):
        meta = self.get_table_meta(table_name)
        pk = meta["pk"]
        if not pk:
            return {"ok": False, "error": "该表不支持可视化编辑（需要单列主键）"}
        cols = {c["name"] for c in meta["columns"]}
        data = {}
        for k, v in (row_data or {}).items():
            if k in cols:
                data[k] = v

        now = int(time.time())
        if meta["has_updated_at"] and "updated_at" not in data:
            data["updated_at"] = now
        if meta["has_created_at"] and "created_at" not in data:
            data["created_at"] = now
        if meta["has_is_deleted"] and "is_deleted" not in data:
            data["is_deleted"] = 0

        pk_val = data.get(pk)
        with self._lock:
            conn = self._get_conn()
            try:
                conn.execute("BEGIN IMMEDIATE")
                if pk_val is not None and pk_val != "":
                    exists = conn.execute(f"SELECT 1 FROM {table_name} WHERE {pk} = ? LIMIT 1", (pk_val,)).fetchone()
                    if exists:
                        update_cols = [k for k in data.keys() if k != pk and k in cols]
                        if update_cols:
                            sets = ", ".join([f"{k} = ?" for k in update_cols])
                            params = [data[k] for k in update_cols] + [pk_val]
                            conn.execute(f"UPDATE {table_name} SET {sets} WHERE {pk} = ?", params)
                        conn.commit()
                        return {"ok": True, "pk": pk, "pk_value": pk_val, "mode": "update"}
                insert_cols = [k for k in data.keys() if k in cols and k != pk]
                vals = [data[k] for k in insert_cols]
                if insert_cols:
                    placeholders = ",".join(["?"] * len(insert_cols))
                    cols_sql = ",".join(insert_cols)
                    cur = conn.execute(f"INSERT INTO {table_name} ({cols_sql}) VALUES ({placeholders})", vals)
                else:
                    cur = conn.execute(f"INSERT INTO {table_name} DEFAULT VALUES")
                conn.commit()
                new_id = cur.lastrowid
                return {"ok": True, "pk": pk, "pk_value": new_id, "mode": "insert"}
            except Exception as e:
                try:
                    conn.rollback()
                except Exception:
                    pass
                return {"ok": False, "error": str(e)}
            finally:
                conn.close()

    def delete_table_row(self, table_name, pk_value):
        meta = self.get_table_meta(table_name)
        pk = meta["pk"]
        if not pk:
            return {"ok": False, "error": "该表不支持可视化删除（需要单列主键）"}
        now = int(time.time())
        with self._lock:
            conn = self._get_conn()
            try:
                conn.execute("BEGIN IMMEDIATE")
                if meta["has_is_deleted"]:
                    if meta["has_updated_at"]:
                        conn.execute(
                            f"UPDATE {table_name} SET is_deleted = 1, updated_at = ? WHERE {pk} = ?",
                            (now, pk_value),
                        )
                    else:
                        conn.execute(
                            f"UPDATE {table_name} SET is_deleted = 1 WHERE {pk} = ?",
                            (pk_value,),
                        )
                else:
                    conn.execute(f"DELETE FROM {table_name} WHERE {pk} = ?", (pk_value,))
                conn.commit()
                return {"ok": True}
            except Exception as e:
                try:
                    conn.rollback()
                except Exception:
                    pass
                return {"ok": False, "error": str(e)}
            finally:
                conn.close()

    # ========== 配置关系表（JSON ↔︎ DB）==========

    def _table_exists(self, conn, table_name):
        row = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
            (table_name,),
        ).fetchone()
        return bool(row)

    def ensure_user(self, username, password):
        username = (username or "").strip()
        password = password if password is not None else ""
        if not username:
            username = "admin"
        with self._lock:
            conn = self._get_conn()
            try:
                conn.execute("BEGIN IMMEDIATE")
                row = conn.execute(
                    "SELECT id FROM users WHERE username = ? AND is_deleted = 0 LIMIT 1",
                    (username,),
                ).fetchone()
                now = int(time.time())
                if row:
                    uid = int(row["id"])
                    conn.execute(
                        "UPDATE users SET password = ?, updated_at = ? WHERE id = ?",
                        (password, now, uid),
                    )
                else:
                    cur = conn.execute(
                        "INSERT INTO users (username, password, created_at, updated_at, is_deleted) VALUES (?, ?, ?, ?, 0)",
                        (username, password, now, now),
                    )
                    uid = int(cur.lastrowid)
                conn.commit()
                return uid
            except Exception:
                try:
                    conn.rollback()
                except Exception:
                    pass
                raise
            finally:
                conn.close()

    def get_system_setting(self, user_id, key, default=None):
        conn = self._get_conn()
        try:
            row = conn.execute(
                "SELECT value FROM system_settings WHERE user_id = ? AND key = ? AND is_deleted = 0 LIMIT 1",
                (user_id, key),
            ).fetchone()
            return row["value"] if row else default
        finally:
            conn.close()

    def set_system_setting(self, user_id, key, value):
        if not key:
            return False
        val = "" if value is None else str(value)
        now = int(time.time())
        with self._lock:
            conn = self._get_conn()
            try:
                conn.execute("BEGIN IMMEDIATE")
                row = conn.execute(
                    "SELECT id FROM system_settings WHERE user_id = ? AND key = ? AND is_deleted = 0 LIMIT 1",
                    (user_id, key),
                ).fetchone()
                if row:
                    conn.execute(
                        "UPDATE system_settings SET value = ?, updated_at = ? WHERE id = ?",
                        (val, now, int(row["id"])),
                    )
                else:
                    conn.execute(
                        "INSERT INTO system_settings (user_id, key, value, created_at, updated_at, is_deleted) VALUES (?, ?, ?, ?, ?, 0)",
                        (user_id, key, val, now, now),
                    )
                conn.commit()
                return True
            except Exception:
                try:
                    conn.rollback()
                except Exception:
                    pass
                return False
            finally:
                conn.close()

    def import_config_dict(self, config_dict):
        if not isinstance(config_dict, dict):
            config_dict = {}
        webui = config_dict.get("webui") or {}
        uid = self.ensure_user(webui.get("username") or "admin", webui.get("password") or "admin123")

        now = int(time.time())
        accounts = config_dict.get("accounts")
        if not isinstance(accounts, list):
            accounts = []
        tasklist = config_dict.get("tasklist")
        if not isinstance(tasklist, list):
            tasklist = []
        magic_regex = config_dict.get("magic_regex")
        if not isinstance(magic_regex, dict):
            magic_regex = {}
        plugins = config_dict.get("plugins")
        if not isinstance(plugins, dict):
            plugins = {}
        push_config = config_dict.get("push_config")
        if not isinstance(push_config, dict):
            push_config = {}
        source = config_dict.get("source")
        if not isinstance(source, dict):
            source = {}
        sync_tasks = config_dict.get("sync_tasks")
        if not isinstance(sync_tasks, list):
            sync_tasks = []

        with self._lock:
            conn = self._get_conn()
            try:
                conn.execute("BEGIN IMMEDIATE")

                conn.execute("UPDATE drive_accounts SET is_deleted = 1, updated_at = ? WHERE user_id = ? AND is_deleted = 0", (now, uid))
                for acc in accounts:
                    if not isinstance(acc, dict):
                        continue
                    name = str(acc.get("name", "") or "").strip()
                    drive_type = str(acc.get("drive_type", "quark") or "quark").strip()
                    cookie = str(acc.get("cookie", "") or "")
                    enabled = 1 if acc.get("enabled", True) else 0
                    is_default = 1 if acc.get("default") or acc.get("is_default") else 0
                    token_updated_at = int(acc.get("_token_updated_at") or 0)
                    if not name:
                        name = f"{drive_type}"
                    conn.execute(
                        """INSERT INTO drive_accounts
                           (user_id, name, drive_type, cookie, enabled, is_default, token_updated_at, created_at, updated_at, is_deleted)
                           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0)""",
                        (uid, name, drive_type, cookie, enabled, is_default, token_updated_at, now, now),
                    )

                incoming_task_uids = set()
                for t in tasklist:
                    if not isinstance(t, dict):
                        continue
                    base = dict(t)
                    addition = base.pop("addition", None)
                    runweek = base.pop("runweek", None)
                    taskname = str(base.get("taskname", "") or "")
                    shareurl = str(base.get("shareurl", "") or "")
                    savepath = str(base.get("savepath", "") or "")
                    task_uid = str(base.get("task_uid", "") or "").strip()

                    if not taskname or not shareurl or not savepath:
                        continue

                    existing = None
                    if task_uid:
                        # 优先使用 task_uid 查找
                        existing = conn.execute(
                            "SELECT id, is_deleted, task_uid FROM tasks WHERE user_id = ? AND task_uid = ? LIMIT 1",
                            (uid, task_uid),
                        ).fetchone()
                    
                    if not existing:
                        # 降级用 shareurl+savepath 匹配（用于旧数据首次迁移补齐 task_uid）
                        existing = conn.execute(
                            """SELECT id, is_deleted, task_uid
                               FROM tasks
                               WHERE user_id = ? AND shareurl = ? AND savepath = ?
                               ORDER BY is_deleted ASC, updated_at DESC, id DESC
                               LIMIT 1""",
                            (uid, shareurl, savepath),
                        ).fetchone()
                    
                    if not existing and not task_uid:
                        # 新任务且无 UID -> 生成
                        task_uid = str(uuid.uuid4())
                    elif existing and not task_uid:
                         # 旧任务无 UID -> 使用现有或生成
                         if existing["task_uid"]:
                             task_uid = existing["task_uid"]
                         else:
                             task_uid = str(uuid.uuid4())
                    
                    incoming_task_uids.add(task_uid)

                    pattern = base.get("pattern")
                    replace = base.get("replace")
                    enddate = base.get("enddate")
                    ignore_extension = 1 if base.get("ignore_extension") else 0
                    sort_index = base.get("sort_index")
                    startfid = base.get("startfid")
                    account_name = base.get("account_name")
                    update_subdir = base.get("update_subdir")
                    enabled = 0 if base.get("enabled") is False else 1
                    known_keys = {
                        "task_uid", "taskname", "shareurl", "savepath", "pattern", "replace", "enddate",
                        "ignore_extension", "sort_index", "startfid", "account_name", "update_subdir", "enabled",
                    }
                    extra = {k: v for k, v in base.items() if k not in known_keys}
                    addition_json = json.dumps(addition, ensure_ascii=False) if isinstance(addition, dict) else None
                    extra_json = json.dumps(extra, ensure_ascii=False) if extra else None
                    
                    if existing:
                        task_id = int(existing["id"])
                        conn.execute(
                            """UPDATE tasks SET
                                   task_uid = ?, taskname = ?, shareurl = ?, savepath = ?, pattern = ?, replace = ?, enddate = ?,
                                   ignore_extension = ?, sort_index = ?, startfid = ?, account_name = ?, update_subdir = ?,
                                   addition_json = ?, extra_json = ?, enabled = ?, is_deleted = 0, updated_at = ?
                               WHERE id = ?""",
                            (
                                task_uid, taskname, shareurl, savepath, pattern, replace, enddate,
                                ignore_extension, sort_index, startfid, account_name, update_subdir,
                                addition_json, extra_json, enabled, now, task_id,
                            ),
                        )
                    else:
                        cur = conn.execute(
                            """INSERT INTO tasks
                               (user_id, task_uid, taskname, shareurl, savepath, pattern, replace, enddate,
                                ignore_extension, sort_index, startfid, account_name, update_subdir,
                                addition_json, extra_json, enabled, created_at, updated_at, is_deleted)
                               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)""",
                            (
                                uid, task_uid, taskname, shareurl, savepath, pattern, replace, enddate,
                                ignore_extension, sort_index, startfid, account_name, update_subdir,
                                addition_json, extra_json, enabled, now, now,
                            ),
                        )
                        task_id = int(cur.lastrowid)

                    conn.execute(
                        "UPDATE task_runweek SET is_deleted = 1, updated_at = ? WHERE task_id = ? AND is_deleted = 0",
                        (now, task_id),
                    )
                    if isinstance(runweek, list):
                        for w in runweek:
                            try:
                                wi = int(w)
                            except Exception:
                                continue
                            if 1 <= wi <= 7:
                                conn.execute(
                                    "INSERT INTO task_runweek (task_id, weekday, created_at, updated_at, is_deleted) VALUES (?, ?, ?, ?, 0)",
                                    (task_id, wi, now, now),
                                )

                if incoming_task_uids:
                    placeholders = ",".join("?" * len(incoming_task_uids))
                    conn.execute(
                        f"""UPDATE tasks
                            SET is_deleted = 1, updated_at = ?
                            WHERE user_id = ?
                              AND is_deleted = 0
                              AND (task_uid IS NULL OR task_uid NOT IN ({placeholders}))""",
                        (now, uid, *incoming_task_uids),
                    )
                else:
                    conn.execute(
                        "UPDATE tasks SET is_deleted = 1, updated_at = ? WHERE user_id = ? AND is_deleted = 0",
                        (now, uid),
                    )

                conn.execute("UPDATE regex_rules SET is_deleted = 1, updated_at = ? WHERE user_id = ? AND is_deleted = 0", (now, uid))
                for name, rule in magic_regex.items():
                    if not isinstance(rule, dict):
                        continue
                    pattern = rule.get("pattern")
                    replace = rule.get("replace", "")
                    if not pattern:
                        continue
                    conn.execute(
                        """INSERT INTO regex_rules
                           (user_id, name, pattern, replace, created_at, updated_at, is_deleted)
                           VALUES (?, ?, ?, ?, ?, ?, 0)""",
                        (uid, str(name), str(pattern), str(replace), now, now),
                    )

                conn.execute("UPDATE plugin_configs SET is_deleted = 1, updated_at = ? WHERE user_id = ? AND is_deleted = 0", (now, uid))
                for plugin_key, cfg in plugins.items():
                    enabled = 1
                    config_json = None
                    if isinstance(cfg, dict):
                        enabled = 0 if cfg.get("enabled") is False else 1
                        config_json = json.dumps(cfg, ensure_ascii=False)
                    elif cfg is not None:
                        config_json = json.dumps(cfg, ensure_ascii=False)
                    conn.execute(
                        """INSERT INTO plugin_configs
                           (user_id, plugin_key, enabled, config_json, created_at, updated_at, is_deleted)
                           VALUES (?, ?, ?, ?, ?, ?, 0)""",
                        (uid, str(plugin_key), enabled, config_json, now, now),
                    )

                conn.execute("UPDATE notification_channels SET is_deleted = 1, updated_at = ? WHERE user_id = ? AND is_deleted = 0", (now, uid))
                for k, v in push_config.items():
                    if k is None:
                        continue
                    enabled = 1
                    if isinstance(v, bool):
                        enabled = 1 if v else 0
                        config_json = None
                    else:
                        config_json = json.dumps({str(k): v}, ensure_ascii=False)
                    conn.execute(
                        """INSERT INTO notification_channels
                           (user_id, channel_key, enabled, config_json, created_at, updated_at, is_deleted)
                           VALUES (?, ?, ?, ?, ?, ?, 0)""",
                        (uid, str(k), enabled, config_json, now, now),
                    )

                conn.execute("UPDATE data_sources SET is_deleted = 1, updated_at = ? WHERE user_id = ? AND is_deleted = 0", (now, uid))
                for source_key, cfg in source.items():
                    if source_key is None:
                        continue
                    enabled = 1
                    if isinstance(cfg, dict):
                        if str(cfg.get("enable", "true")).lower() == "false":
                            enabled = 0
                    config_json = json.dumps(cfg, ensure_ascii=False) if cfg is not None else None
                    conn.execute(
                        """INSERT INTO data_sources
                           (user_id, source_key, enabled, config_json, created_at, updated_at, is_deleted)
                           VALUES (?, ?, ?, ?, ?, ?, 0)""",
                        (uid, str(source_key), enabled, config_json, now, now),
                    )

                conn.execute("UPDATE sync_task_configs SET is_deleted = 1, updated_at = ? WHERE user_id = ? AND is_deleted = 0", (now, uid))
                for st in sync_tasks:
                    if not isinstance(st, dict):
                        continue
                    task_id = str(st.get("task_id", "") or "").strip()
                    source_dir = str(st.get("source_dir", "") or "")
                    dest_dir = str(st.get("dest_dir", "") or "")
                    if not task_id or not source_dir or not dest_dir:
                        continue
                    enabled = 1 if st.get("enabled", True) else 0
                    file_type_filter = st.get("file_type_filter")
                    file_type_filter_json = json.dumps(file_type_filter, ensure_ascii=False) if isinstance(file_type_filter, list) else None
                    known_keys = {
                        "task_id", "taskname", "source_dir", "dest_dir", "sync_mode", "match_mode", "cron",
                        "enabled", "regex_filter", "file_type_filter", "exclude_empty_dirs",
                        "notify_on_complete", "notify_on_error",
                    }
                    extra = {k: v for k, v in st.items() if k not in known_keys}
                    extra_json = json.dumps(extra, ensure_ascii=False) if extra else None
                    conn.execute(
                        """INSERT INTO sync_task_configs
                           (user_id, task_id, taskname, source_dir, dest_dir, sync_mode, match_mode, cron, enabled,
                            regex_filter, file_type_filter_json, exclude_empty_dirs, notify_on_complete, notify_on_error,
                            extra_json, created_at, updated_at, is_deleted)
                           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)""",
                        (
                            uid,
                            task_id,
                            st.get("taskname"),
                            source_dir,
                            dest_dir,
                            st.get("sync_mode"),
                            st.get("match_mode"),
                            st.get("cron"),
                            enabled,
                            st.get("regex_filter"),
                            file_type_filter_json,
                            1 if st.get("exclude_empty_dirs") else 0,
                            1 if st.get("notify_on_complete", True) else 0,
                            1 if st.get("notify_on_error", True) else 0,
                            extra_json,
                            now,
                            now,
                        ),
                    )

                conn.commit()
            except Exception:
                try:
                    conn.rollback()
                except Exception:
                    pass
                raise
            finally:
                conn.close()

        self.set_system_setting(uid, "crontab", config_dict.get("crontab") or "")
        self.set_system_setting(uid, "multi_drive_support", "true" if config_dict.get("multi_drive_support") else "false")
        self.set_system_setting(uid, "config_storage_mode", "relational")
        return uid

    def export_config_dict(self, username="admin"):
        conn = self._get_conn()
        try:
            row = conn.execute(
                "SELECT id, username, password FROM users WHERE username = ? AND is_deleted = 0 LIMIT 1",
                (username,),
            ).fetchone()
            if not row:
                return None
            uid = int(row["id"])
            user_block = {"username": row["username"], "password": row["password"]}

            accounts = []
            for r in conn.execute(
                """SELECT name, drive_type, cookie, enabled, is_default, token_updated_at
                   FROM drive_accounts
                   WHERE user_id = ? AND is_deleted = 0
                   ORDER BY id ASC""",
                (uid,),
            ).fetchall():
                accounts.append(
                    {
                        "name": r["name"],
                        "drive_type": r["drive_type"],
                        "cookie": r["cookie"],
                        "enabled": bool(r["enabled"]),
                        "default": bool(r["is_default"]),
                        "_token_updated_at": int(r["token_updated_at"] or 0),
                    }
                )

            magic_regex = {}
            for r in conn.execute(
                """SELECT name, pattern, replace FROM regex_rules
                   WHERE user_id = ? AND is_deleted = 0
                   ORDER BY id ASC""",
                (uid,),
            ).fetchall():
                magic_regex[r["name"]] = {"pattern": r["pattern"], "replace": r["replace"]}

            plugins = {}
            for r in conn.execute(
                """SELECT plugin_key, enabled, config_json FROM plugin_configs
                   WHERE user_id = ? AND is_deleted = 0
                   ORDER BY id ASC""",
                (uid,),
            ).fetchall():
                if r["config_json"]:
                    try:
                        cfg = json.loads(r["config_json"])
                    except Exception:
                        cfg = r["config_json"]
                else:
                    cfg = {}
                plugins[r["plugin_key"]] = cfg

            push_config = {}
            for r in conn.execute(
                """SELECT channel_key, enabled, config_json FROM notification_channels
                   WHERE user_id = ? AND is_deleted = 0
                   ORDER BY id ASC""",
                (uid,),
            ).fetchall():
                if r["config_json"]:
                    try:
                        payload = json.loads(r["config_json"])
                    except Exception:
                        payload = {}
                    if isinstance(payload, dict) and r["channel_key"] in payload:
                        push_config[r["channel_key"]] = payload[r["channel_key"]]
                    else:
                        push_config[r["channel_key"]] = payload
                else:
                    push_config[r["channel_key"]] = bool(r["enabled"])

            source = {}
            for r in conn.execute(
                """SELECT source_key, enabled, config_json FROM data_sources
                   WHERE user_id = ? AND is_deleted = 0
                   ORDER BY id ASC""",
                (uid,),
            ).fetchall():
                if r["config_json"]:
                    try:
                        payload = json.loads(r["config_json"])
                    except Exception:
                        payload = {}
                else:
                    payload = {}
                if isinstance(payload, dict) and "enable" in payload:
                    payload["enable"] = "" if bool(r["enabled"]) else "false"
                source[r["source_key"]] = payload

            sync_tasks = []
            for r in conn.execute(
                """SELECT task_id, taskname, source_dir, dest_dir, sync_mode, match_mode, cron, enabled,
                          regex_filter, file_type_filter_json, exclude_empty_dirs, notify_on_complete, notify_on_error, extra_json
                   FROM sync_task_configs
                   WHERE user_id = ? AND is_deleted = 0
                   ORDER BY id ASC""",
                (uid,),
            ).fetchall():
                st = {
                    "task_id": r["task_id"],
                    "taskname": r["taskname"],
                    "source_dir": r["source_dir"],
                    "dest_dir": r["dest_dir"],
                    "sync_mode": r["sync_mode"],
                    "match_mode": r["match_mode"],
                    "cron": r["cron"],
                    "enabled": bool(r["enabled"]),
                    "exclude_empty_dirs": bool(r["exclude_empty_dirs"]),
                    "notify_on_complete": bool(r["notify_on_complete"]),
                    "notify_on_error": bool(r["notify_on_error"]),
                }
                if r["regex_filter"] is not None:
                    st["regex_filter"] = r["regex_filter"]
                if r["file_type_filter_json"]:
                    try:
                        st["file_type_filter"] = json.loads(r["file_type_filter_json"])
                    except Exception:
                        st["file_type_filter"] = []
                if r["extra_json"]:
                    try:
                        extra = json.loads(r["extra_json"])
                        if isinstance(extra, dict):
                            st.update(extra)
                    except Exception:
                        pass
                sync_tasks.append(st)

            tasks = []
            for r in conn.execute(
                """SELECT id, task_uid, taskname, shareurl, savepath, pattern, replace, enddate,
                          ignore_extension, sort_index, startfid, account_name, update_subdir,
                          addition_json, extra_json, enabled
                   FROM tasks
                   WHERE user_id = ? AND is_deleted = 0
                   ORDER BY id ASC""",
                (uid,),
            ).fetchall():
                t = {
                    "task_uid": r["task_uid"],
                    "taskname": r["taskname"],
                    "shareurl": r["shareurl"],
                    "savepath": r["savepath"],
                }
                if r["pattern"] is not None:
                    t["pattern"] = r["pattern"]
                if r["replace"] is not None:
                    t["replace"] = r["replace"]
                if r["enddate"] is not None:
                    t["enddate"] = r["enddate"]
                if r["ignore_extension"]:
                    t["ignore_extension"] = True
                if r["sort_index"] is not None and r["sort_index"] != "":
                    t["sort_index"] = r["sort_index"]
                if r["startfid"] is not None:
                    t["startfid"] = r["startfid"]
                if r["account_name"] is not None:
                    t["account_name"] = r["account_name"]
                if r["update_subdir"] is not None:
                    t["update_subdir"] = r["update_subdir"]
                if not bool(r["enabled"]):
                    t["enabled"] = False
                if r["addition_json"]:
                    try:
                        add = json.loads(r["addition_json"])
                        if isinstance(add, dict):
                            t["addition"] = add
                    except Exception:
                        pass
                if r["extra_json"]:
                    try:
                        extra = json.loads(r["extra_json"])
                        if isinstance(extra, dict):
                            t.update(extra)
                    except Exception:
                        pass
                rw = [int(x["weekday"]) for x in conn.execute(
                    "SELECT weekday FROM task_runweek WHERE task_id = ? AND is_deleted = 0 ORDER BY weekday ASC",
                    (int(r["id"]),),
                ).fetchall()]
                if rw:
                    t["runweek"] = rw
                tasks.append(t)

            crontab = self.get_system_setting(uid, "crontab", "") or ""
            mds = self.get_system_setting(uid, "multi_drive_support", "false")
            multi_drive_support = str(mds).lower() == "true"

            out = {
                "push_config": push_config,
                "plugins": plugins,
                "magic_regex": magic_regex,
                "tasklist": tasks,
                "webui": user_block,
                "crontab": crontab,
                "multi_drive_support": multi_drive_support,
                "accounts": accounts,
                "source": source,
                "sync_tasks": sync_tasks,
            }
            return out
        finally:
            conn.close()

    def migrate_app_config_to_relational(self, key="quark_config"):
        raw = self.get_app_config(key)
        if not raw:
            return False
        try:
            data = json.loads(raw)
        except Exception:
            return False
        if not isinstance(data, dict):
            return False
        self.import_config_dict(data)
        return True

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

    def update_task_status(self, task_id, status):
        if not task_id or not status:
            return
        with self._lock:
            conn = self._get_conn()
            try:
                conn.execute(
                    """UPDATE sync_task_status
                       SET status = ?
                       WHERE task_id = ?""",
                    (status, task_id),
                )
                conn.commit()
            except Exception as e:
                logger.warning(f"更新任务 status 失败: {e}")
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
