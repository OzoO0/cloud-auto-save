# -*- coding: utf-8 -*-
"""核心同步引擎 - 负责文件扫描、过滤、匹配、复制和通知"""

import os
import re
import json
import time
import shutil
import hashlib
import struct
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

logger = logging.getLogger("sync.engine")

# 预设文件类型过滤器
FILE_TYPE_EXTENSIONS = {
    "video": {".mp4", ".mkv", ".avi", ".mov", ".wmv", ".flv", ".ts", ".m4v", ".rmvb", ".webm", ".m2ts"},
    "audio": {".mp3", ".flac", ".wav", ".aac", ".ogg", ".wma", ".m4a", ".ape", ".dsd"},
    "image": {".jpg", ".jpeg", ".png", ".gif", ".bmp", ".webp", ".svg", ".tiff", ".raw", ".heic"},
    "document": {".pdf", ".doc", ".docx", ".xls", ".xlsx", ".ppt", ".pptx", ".txt", ".md", ".csv"},
    "subtitle": {".srt", ".ass", ".ssa", ".sub", ".idx", ".vtt", ".sup"},
    "nfo": {".nfo"},
}


class FileSyncEngine:
    """本地文件同步引擎"""

    def __init__(self, task_config, db, base_dir, push_config=None,
                 cancel_event=None, synced_files_tracker=None, structured_log=False):
        """
        Args:
            task_config: 同步任务配置字典
            db: SyncDB 实例
            base_dir: datafiles 基础目录的绝对路径
            push_config: 推送配置字典（可选）
            cancel_event: threading.Event，用于外部取消信号（可选）
            synced_files_tracker: 列表，记录本次运行中已成功复制的文件路径（可选）
            structured_log: 是否使用结构化日志回调模式（可选）
        """
        self.task = task_config
        self.db = db
        self.base_dir = os.path.realpath(base_dir)
        self.push_config = push_config or {}
        self.task_id = task_config.get("task_id", "")
        self.taskname = task_config.get("taskname", "未命名任务")
        self.cancel_event = cancel_event
        self.synced_files_tracker = synced_files_tracker
        self.structured_log = structured_log

        # 解析目录路径
        source_rel = task_config.get("source_dir", "")
        dest_rel = task_config.get("dest_dir", "")
        self.source_dir = os.path.realpath(os.path.join(self.base_dir, source_rel))
        self.dest_dir = os.path.realpath(os.path.join(self.base_dir, dest_rel))

        # 同步参数
        self.sync_mode = task_config.get("sync_mode", "incremental")
        self.match_mode = task_config.get("match_mode", "full_name")
        self.regex_filter = task_config.get("regex_filter", "")
        self.file_type_filter = task_config.get("file_type_filter", [])
        self.exclude_empty_dirs = task_config.get("exclude_empty_dirs", True)
        self.notify_on_complete = task_config.get("notify_on_complete", True)
        self.notify_on_error = task_config.get("notify_on_error", True)

        # MD5 优化参数
        self.md5_cache_enabled = task_config.get("md5_cache_enabled", True)
        self.quick_fp_threshold = task_config.get(
            "quick_fingerprint_threshold", 256 * 1024 * 1024  # 256MB
        )
        self.quick_fp_sample_size = 1 * 1024 * 1024  # 1MB 内部常量
        self.md5_workers = task_config.get("md5_workers", 4)

        # 计数器
        self._synced = 0
        self._skipped = 0
        self._failed = 0

    def _log(self, msg, callback=None):
        """统一日志输出"""
        logger.info(msg)
        if callback:
            callback(msg)

    def _emit(self, event_type, data, callback=None):
        """发送结构化事件（仅 structured_log 模式）"""
        if callback and self.structured_log:
            callback(json.dumps({"event": event_type, "data": data}, ensure_ascii=False))

    def _is_cancelled(self):
        """检查任务是否已被取消"""
        return self.cancel_event is not None and self.cancel_event.is_set()

    def _validate_path(self, path):
        """验证路径不逃逸 base_dir"""
        real = os.path.realpath(path)
        if not real.startswith(self.base_dir):
            raise ValueError(f"路径安全校验失败: {path} 不在 {self.base_dir} 内")
        return real

    def execute(self, log_callback=None):
        """
        执行同步任务主流程。
        log_callback: 可选的日志回调函数，用于 SSE 流式输出。
        返回执行结果摘要字典。
        """
        self._synced = 0
        self._skipped = 0
        self._failed = 0
        start_time = time.time()

        self._log(f"[数据同步] 开始执行: {self.taskname}", log_callback)
        self._log(f"  源目录: {self.source_dir}", log_callback)
        self._log(f"  目标目录: {self.dest_dir}", log_callback)
        self._log(f"  同步模式: {self.sync_mode} | 匹配模式: {self.match_mode}", log_callback)

        # 路径安全校验
        try:
            self._validate_path(self.source_dir)
            self._validate_path(self.dest_dir)
        except ValueError as e:
            self._log(f"  错误: {e}", log_callback)
            return self._build_summary("error", str(e), time.time() - start_time)

        # 获取任务锁
        if not self.db.acquire_lock(self.task_id):
            msg = "任务正在运行中，跳过本次执行"
            self._log(f"  {msg}", log_callback)
            return self._build_summary("skipped", msg, time.time() - start_time)

        try:
            # 检查源目录
            if not os.path.isdir(self.source_dir):
                msg = f"源目录不存在: {self.source_dir}"
                self._log(f"  错误: {msg}", log_callback)
                return self._build_summary("error", msg, time.time() - start_time)

            # 确保目标目录存在
            os.makedirs(self.dest_dir, exist_ok=True)

            # 检查取消
            if self._is_cancelled():
                self._log("  任务已被用户取消", log_callback)
                return self._build_summary("cancelled", "用户取消", time.time() - start_time)

            # 扫描文件
            files = self._scan_source_dir(log_callback)
            self._log(f"  扫描到 {len(files)} 个待处理文件", log_callback)

            if not files:
                msg = "没有需要同步的文件"
                self._log(f"  {msg}", log_callback)
                return self._build_summary("success", msg, time.time() - start_time)

            # 发送 init 结构化事件
            self._emit("init", {
                "total": len(files),
                "task_name": self.taskname,
            }, log_callback)

            # 构建目标目录已有文件集合（用于文件匹配）
            dest_files = self._scan_dest_files()
            dest_count = len(dest_files["paths"])
            if dest_count:
                self._log(f"  目标目录已有 {dest_count} 个文件", log_callback)

            # 阶段 1：MD5 预计算（仅 md5 模式且启用缓存时）
            if self.match_mode == "md5" and self.md5_cache_enabled:
                self._batch_compute_md5(files, log_callback)

            # 阶段 2：逐文件同步
            cancelled = False
            for idx, file_info in enumerate(files):
                # 检查取消信号
                if self._is_cancelled():
                    cancelled = True
                    self._log("  任务已被用户取消", log_callback)
                    break

                # 发送 file_start 结构化事件
                self._emit("file_start", {
                    "index": idx,
                    "rel_path": file_info["rel_path"],
                    "name": file_info["name"],
                    "size": file_info.get("size", 0),
                }, log_callback)

                try:
                    status, message = self._process_file(file_info, dest_files, log_callback)
                except Exception as e:
                    self._failed += 1
                    status = "failed"
                    message = str(e)
                    self._log(f"  失败: {file_info['rel_path']} -> {e}", log_callback)
                    self.db.add_sync_record(
                        task_id=self.task_id,
                        file_path=file_info["rel_path"],
                        file_name=file_info["name"],
                        file_size=file_info.get("size"),
                        file_mtime=file_info.get("mtime"),
                        status="failed",
                        message=str(e),
                    )

                # 发送 file_done 结构化事件
                self._emit("file_done", {
                    "index": idx,
                    "rel_path": file_info["rel_path"],
                    "status": status,
                    "message": message,
                }, log_callback)

                # 发送 progress 结构化事件
                processed = idx + 1
                elapsed_now = time.time() - start_time
                self._emit("progress", {
                    "processed": processed,
                    "total": len(files),
                    "synced": self._synced,
                    "skipped": self._skipped,
                    "failed": self._failed,
                    "elapsed": round(elapsed_now, 1),
                }, log_callback)

            elapsed = time.time() - start_time

            if cancelled:
                result = "cancelled"
                self._log(
                    f"  已取消: 同步 {self._synced} | 跳过 {self._skipped} | 失败 {self._failed} | 耗时 {elapsed:.1f}s",
                    log_callback,
                )
            else:
                result = "success" if self._failed == 0 else "partial"
                self._log(
                    f"  完成: 同步 {self._synced} | 跳过 {self._skipped} | 失败 {self._failed} | 耗时 {elapsed:.1f}s",
                    log_callback,
                )

            summary = self._build_summary(result, "", elapsed)

            # 发送 done 结构化事件
            self._emit("done", {
                "result": result,
                "synced": self._synced,
                "skipped": self._skipped,
                "failed": self._failed,
                "elapsed": round(elapsed, 1),
            }, log_callback)

            # 更新任务状态
            self.db.update_task_result(
                self.task_id, result, self._synced, self._skipped, self._failed
            )

            # 发送通知（取消时不通知）
            if not cancelled:
                self._send_notification(summary)

            return summary

        except Exception as e:
            elapsed = time.time() - start_time
            self._log(f"  任务异常: {e}", log_callback)
            self.db.update_task_result(self.task_id, "error", self._synced, self._skipped, self._failed)
            summary = self._build_summary("error", str(e), elapsed)
            if self.notify_on_error:
                self._send_notification(summary)
            return summary
        finally:
            self.db.release_lock(self.task_id)

    # ========== 文件扫描 ==========

    def _scan_source_dir(self, log_callback=None):
        """递归扫描源目录，返回文件信息列表"""
        files = []
        regex = None
        if self.regex_filter:
            try:
                regex = re.compile(self.regex_filter)
            except re.error as e:
                self._log(f"  正则表达式无效: {self.regex_filter} -> {e}", log_callback)

        # 构建允许的扩展名集合
        allowed_exts = set()
        if self.file_type_filter:
            for ft in self.file_type_filter:
                if ft in FILE_TYPE_EXTENSIONS:
                    allowed_exts.update(FILE_TYPE_EXTENSIONS[ft])

        for root, dirs, filenames in os.walk(self.source_dir):
            # 排除空目录
            if self.exclude_empty_dirs and not filenames:
                continue

            for fname in filenames:
                full_path = os.path.join(root, fname)
                rel_path = os.path.relpath(full_path, self.source_dir)

                # 正则过滤
                if regex and not regex.search(fname):
                    continue

                # 文件类型过滤
                if allowed_exts:
                    ext = os.path.splitext(fname)[1].lower()
                    if ext not in allowed_exts:
                        continue

                try:
                    stat = os.stat(full_path)
                    files.append({
                        "name": fname,
                        "rel_path": rel_path,
                        "full_path": full_path,
                        "size": stat.st_size,
                        "mtime": stat.st_mtime,
                    })
                except OSError as e:
                    self._log(f"  无法读取文件状态: {full_path} -> {e}", log_callback)

        return files

    def _scan_dest_files(self):
        """扫描目标目录已有文件，返回多种索引"""
        result = {
            "full_names": set(),        # 完整文件名集合
            "names_no_ext": set(),      # 去扩展名文件名集合
            "md5s": set(),              # MD5 集合（仅在 md5 模式下使用）
            "paths": {},                # rel_path -> file_info 映射
        }

        if not os.path.isdir(self.dest_dir):
            return result

        for root, dirs, filenames in os.walk(self.dest_dir):
            for fname in filenames:
                # 跳过残留的临时下载文件
                if fname.startswith(".") and fname.endswith(".download"):
                    continue
                full_path = os.path.join(root, fname)
                rel_path = os.path.relpath(full_path, self.dest_dir)
                result["full_names"].add(fname)
                result["names_no_ext"].add(os.path.splitext(fname)[0])
                result["paths"][rel_path] = full_path

        return result

    # ========== MD5 计算（含优化） ==========

    def _compute_md5(self, filepath, chunk_size=1024 * 1024):
        """流式计算文件完整 MD5（默认 1MB 分块）"""
        md5 = hashlib.md5()
        file_size = os.path.getsize(filepath)
        processed = 0
        log_threshold = 1024 * 1024 * 512  # 512MB 时打印进度

        with open(filepath, "rb") as f:
            while True:
                data = f.read(chunk_size)
                if not data:
                    break
                md5.update(data)
                processed += len(data)

                if file_size > log_threshold and processed % log_threshold < chunk_size:
                    pct = processed / file_size * 100
                    logger.debug(f"MD5 计算进度: {filepath} -> {pct:.0f}%")

        return md5.hexdigest()

    def _compute_quick_fingerprint(self, filepath):
        """
        对大文件计算快速指纹：读取头部 + 中部 + 尾部各 sample_size 字节。
        返回带 "QF:" 前缀的哈希值。
        """
        sample_size = self.quick_fp_sample_size
        try:
            file_size = os.path.getsize(filepath)
            md5 = hashlib.md5()

            # 将文件大小编码加入哈希，防止不同大小文件的采样点恰好相同
            md5.update(struct.pack(">Q", file_size))

            with open(filepath, "rb") as f:
                # 头部
                f.seek(0)
                md5.update(f.read(sample_size))

                # 中部
                mid_offset = max(0, file_size // 2 - sample_size // 2)
                f.seek(mid_offset)
                md5.update(f.read(sample_size))

                # 尾部
                tail_offset = max(0, file_size - sample_size)
                f.seek(tail_offset)
                md5.update(f.read(sample_size))

            return "QF:" + md5.hexdigest()

        except Exception as e:
            logger.warning(f"Quick Fingerprint 失败，回退到完整 MD5: {filepath} -> {e}")
            return self._compute_md5(filepath)

    def _compute_file_md5(self, file_info):
        """
        计算单个文件的 MD5（供并行调用）。
        根据文件大小选择完整 MD5 或 Quick Fingerprint。
        返回 (md5_value, md5_type)。
        """
        filepath = file_info["full_path"]
        file_size = file_info.get("size", 0)

        if file_size > self.quick_fp_threshold:
            md5_val = self._compute_quick_fingerprint(filepath)
            md5_type = "quick"
        else:
            md5_val = self._compute_md5(filepath)
            md5_type = "full"

        return md5_val, md5_type

    def _batch_compute_md5(self, files, log_callback=None):
        """
        批量 MD5 预计算（两阶段架构的阶段 1）。
        1. 批量查缓存，命中的直接填充 file_info["md5"]
        2. 未命中的并行计算 MD5
        3. 批量写入缓存
        """
        total = len(files)

        # 步骤 1：批量查缓存
        cache_hits = self.db.batch_get_md5_cache(files)
        hit_count = 0
        to_compute = []

        for fi in files:
            cached = cache_hits.get(fi["full_path"])
            if cached:
                fi["md5"] = cached["md5"]
                hit_count += 1
            else:
                to_compute.append(fi)

        miss_count = len(to_compute)
        self._log(
            f"  MD5 缓存命中 {hit_count}/{total}，需计算 {miss_count} 个文件",
            log_callback,
        )

        if not to_compute:
            return

        # 步骤 2：并行计算未命中的文件
        new_cache_entries = []
        workers = min(self.md5_workers, miss_count)

        with ThreadPoolExecutor(max_workers=workers) as executor:
            future_to_fi = {
                executor.submit(self._compute_file_md5, fi): fi
                for fi in to_compute
            }
            for future in as_completed(future_to_fi):
                fi = future_to_fi[future]
                try:
                    md5_val, md5_type = future.result()
                    fi["md5"] = md5_val
                    new_cache_entries.append({
                        "file_path": fi["full_path"],
                        "file_size": fi["size"],
                        "file_mtime": fi["mtime"],
                        "md5": md5_val,
                        "md5_type": md5_type,
                    })
                except Exception as e:
                    logger.error(f"MD5 计算失败: {fi['full_path']} -> {e}")
                    fi["md5"] = None  # 标记失败，_should_sync 会 fallback

        # 步骤 3：批量写入缓存
        if new_cache_entries:
            self.db.batch_set_md5_cache(new_cache_entries)
            self._log(
                f"  已缓存 {len(new_cache_entries)} 个新 MD5 计算结果",
                log_callback,
            )

    # ========== 文件处理 ==========

    def _process_file(self, file_info, dest_files, log_callback=None):
        """处理单个文件的同步逻辑，返回 (status, message)"""
        rel_path = file_info["rel_path"]
        fname = file_info["name"]
        dest_path = os.path.join(self.dest_dir, rel_path)

        # 检查是否需要同步
        should_sync, skip_reason = self._should_sync(file_info, dest_files)

        if not should_sync:
            self._skipped += 1
            logger.debug(f"  跳过: {rel_path} ({skip_reason})")
            return "skipped", skip_reason

        # 执行同步
        self._sync_file(file_info, dest_path, log_callback)
        return "success", ""

    def _should_sync(self, file_info, dest_files):
        """
        判断文件是否需要同步。
        结合数据库记录和目标目录实际文件进行判断。
        返回 (should_sync: bool, reason: str)
        """
        fname = file_info["name"]
        rel_path = file_info["rel_path"]

        if self.sync_mode == "overwrite":
            return True, ""

        # 增量模式：检查是否已存在
        if self.match_mode == "full_name":
            if rel_path in dest_files["paths"]:
                return False, "目标路径已存在"
            if self.db.is_file_synced(self.task_id, rel_path, fname, "full_name"):
                return False, "数据库记录已同步"
            return True, ""

        elif self.match_mode == "name_only":
            name_no_ext = os.path.splitext(fname)[0]
            if name_no_ext in dest_files["names_no_ext"]:
                return False, "目标存在同名文件(忽略扩展名)"
            if self.db.is_file_synced(self.task_id, rel_path, fname, "name_only"):
                return False, "数据库记录已同步(忽略扩展名)"
            return True, ""

        elif self.match_mode == "md5":
            file_md5 = file_info.get("md5")
            if not file_md5:
                try:
                    file_md5 = self._compute_md5(file_info["full_path"])
                    file_info["md5"] = file_md5
                except Exception as e:
                    logger.error(f"MD5 计算失败: {file_info['full_path']} -> {e}")
                    return True, ""

            # 目标路径存在时：快速指纹比较
            if rel_path in dest_files["paths"]:
                dest_path = dest_files["paths"][rel_path]
                try:
                    # 快速检查：文件大小不同则内容必定不同
                    dest_size = os.path.getsize(dest_path)
                    src_size = file_info.get("size", 0)
                    if src_size != dest_size:
                        logger.debug(f"  MD5模式: {rel_path} 大小不同(源={src_size}, 目标={dest_size})，需同步")
                        return True, ""
                    # 大小一致：用快速指纹比较（头+中+尾各1MB）
                    src_fp = self._compute_quick_fingerprint(file_info["full_path"])
                    dest_fp = self._compute_quick_fingerprint(dest_path)
                    if src_fp == dest_fp:
                        return False, f"快速指纹一致({src_fp[3:11]}...)"
                    else:
                        logger.debug(f"  MD5模式: {rel_path} 指纹不同，需同步")
                        return True, ""
                except OSError as e:
                    logger.warning(f"  MD5模式: 无法读取目标文件 {dest_path}: {e}，执行同步")
                    return True, ""

            # 目标路径不存在：查数据库
            if self.db.is_file_synced(self.task_id, rel_path, fname, "md5", file_md5):
                return False, f"数据库记录MD5已同步({file_md5[:8]}...)"
            return True, ""

        return True, ""

    def _sync_file(self, file_info, dest_path, log_callback=None):
        """执行单文件复制（原子性写入：临时文件 → 重命名）"""
        src_path = file_info["full_path"]

        # 确保目标子目录存在
        dest_dir = os.path.dirname(dest_path)
        os.makedirs(dest_dir, exist_ok=True)

        # 原子性写入：先写临时文件，完成后重命名
        fname = os.path.basename(dest_path)
        tmp_path = os.path.join(dest_dir, f".{fname}.download")
        try:
            shutil.copy2(src_path, tmp_path)
            # 重命名（同文件系统内为原子操作）
            os.replace(tmp_path, dest_path)
        except BaseException:
            # 清理残留临时文件
            try:
                os.remove(tmp_path)
            except OSError:
                pass
            raise

        self._synced += 1

        # 记录已同步的文件路径（用于取消时回滚）
        if self.synced_files_tracker is not None:
            self.synced_files_tracker.append(dest_path)

        self._log(f"  同步: {file_info['rel_path']}", log_callback)

        # 记录到数据库
        self.db.add_sync_record(
            task_id=self.task_id,
            file_path=file_info["rel_path"],
            file_name=file_info["name"],
            file_size=file_info.get("size"),
            file_mtime=file_info.get("mtime"),
            file_md5=file_info.get("md5"),
            dest_path=dest_path,
            status="success",
        )

    # ========== 通知与摘要 ==========

    def _send_notification(self, summary):
        """发送同步完成通知"""
        if summary["result"] == "success" and not self.notify_on_complete:
            return
        if summary["result"] in ("error", "partial") and not self.notify_on_error:
            return
        if summary["result"] == "cancelled":
            return

        try:
            from notify import send

            title = f"[数据同步] {self.taskname}"
            status_text = {
                "success": "同步完成",
                "partial": "部分完成",
                "error": "同步失败",
                "skipped": "已跳过",
            }.get(summary["result"], summary["result"])

            body_lines = [
                f"状态: {status_text}",
                f"同步: {summary['synced']} 跳过: {summary['skipped']} 失败: {summary['failed']}",
                f"耗时: {summary['elapsed']:.1f}s",
            ]
            if summary.get("message"):
                body_lines.append(f"信息: {summary['message']}")

            body = "\n".join(body_lines)

            # 临时设置推送配置环境变量
            import os as _os
            old_env = {}
            for key, value in self.push_config.items():
                if isinstance(value, str) and value:
                    old_env[key] = _os.environ.get(key)
                    _os.environ[key] = value

            try:
                send(title, body)
            finally:
                # 恢复环境变量
                for key, old_val in old_env.items():
                    if old_val is None:
                        _os.environ.pop(key, None)
                    else:
                        _os.environ[key] = old_val

        except Exception as e:
            logger.error(f"发送通知失败: {e}")

    def _build_summary(self, result, message, elapsed):
        """构建执行结果摘要"""
        return {
            "task_id": self.task_id,
            "taskname": self.taskname,
            "result": result,
            "message": message,
            "synced": self._synced,
            "skipped": self._skipped,
            "failed": self._failed,
            "elapsed": elapsed,
        }
