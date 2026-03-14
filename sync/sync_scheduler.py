# -*- coding: utf-8 -*-
"""同步任务调度管理器 - APScheduler 集成"""

import logging
import threading
from apscheduler.triggers.cron import CronTrigger

from .file_sync import FileSyncEngine

logger = logging.getLogger("sync.scheduler")


class SyncSchedulerManager:
    """管理同步任务的定时调度"""

    JOB_PREFIX = "sync_"

    def __init__(self, scheduler, db, base_dir, config_getter, cancel_events=None, cancel_events_lock=None):
        """
        Args:
            scheduler: APScheduler BackgroundScheduler 实例
            db: SyncDB 实例
            base_dir: datafiles 基础目录的绝对路径
            config_getter: 获取当前配置的回调函数 (返回 config_data dict)
            cancel_events: 取消事件字典 {task_id: threading.Event}
            cancel_events_lock: 取消事件字典的锁
        """
        self.scheduler = scheduler
        self.db = db
        self.base_dir = base_dir
        self.config_getter = config_getter
        self.cancel_events = cancel_events if cancel_events is not None else {}
        self.cancel_events_lock = cancel_events_lock

    def reload_sync_tasks(self, sync_tasks):
        """
        重新加载所有同步任务的调度。
        移除旧的 sync_ jobs，为每个启用的任务添加新的 CronTrigger job。
        """
        # 移除所有 sync_ 前缀的 jobs
        for job in self.scheduler.get_jobs():
            if job.id.startswith(self.JOB_PREFIX):
                self.scheduler.remove_job(job.id)
                logger.debug(f"移除旧调度 job: {job.id}")

        if not sync_tasks:
            logger.info("无同步任务需要调度")
            return

        loaded = 0
        for task in sync_tasks:
            if not task.get("enabled", True):
                continue

            task_id = task.get("task_id", "")
            cron_expr = task.get("cron", "")
            taskname = task.get("taskname", "")

            if not task_id or not cron_expr:
                logger.warning(f"同步任务配置不完整，跳过: {taskname}")
                continue

            try:
                trigger = CronTrigger.from_crontab(cron_expr)
                job_id = f"{self.JOB_PREFIX}{task_id}"

                self.scheduler.add_job(
                    self._execute_sync_task,
                    trigger=trigger,
                    args=[task],
                    id=job_id,
                    max_instances=1,
                    coalesce=True,
                    misfire_grace_time=300,
                    replace_existing=True,
                )
                loaded += 1
                logger.info(f"已调度同步任务: {taskname} ({cron_expr})")
            except Exception as e:
                logger.error(f"调度同步任务失败 [{taskname}]: {e}")

        logger.info(f"同步调度器已加载 {loaded} 个任务")

    def _execute_sync_task(self, task_config):
        """定时触发的任务执行入口"""
        task_config = dict(task_config or {})
        task_config["_trigger"] = "scheduler"
        taskname = task_config.get("taskname", "")
        task_id = task_config.get("task_id", "")
        logger.info(f"[定时] 开始执行同步任务: {taskname}")

        cancel_event = threading.Event()
        registered = False
        if task_id:
            status = None
            try:
                status = self.db.get_task_status(task_id, include_data=False)
            except Exception:
                status = None
            running = bool(status and status.get("status") == "running")
            if self.cancel_events_lock:
                with self.cancel_events_lock:
                    existing = self.cancel_events.get(task_id)
                    if existing and running:
                        cancel_event = existing
                    else:
                        self.cancel_events[task_id] = cancel_event
                        registered = True
            else:
                existing = self.cancel_events.get(task_id)
                if existing and running:
                    cancel_event = existing
                else:
                    self.cancel_events[task_id] = cancel_event
                    registered = True

        try:
            summary = self.run_task_now(
                task_config=task_config,
                log_callback=None,
                cancel_event=cancel_event,
                synced_files_tracker=None,
                structured_log=True,
            )
            logger.info(
                f"[定时] 同步任务完成: {taskname} "
                f"结果={summary['result']} 同步={summary['synced']} "
                f"跳过={summary['skipped']} 失败={summary['failed']}"
            )
            return summary
        finally:
            if task_id and registered:
                if self.cancel_events_lock:
                    with self.cancel_events_lock:
                        if self.cancel_events.get(task_id) == cancel_event:
                            self.cancel_events.pop(task_id, None)
                else:
                    if self.cancel_events.get(task_id) == cancel_event:
                        self.cancel_events.pop(task_id, None)

    def run_task_now(self, task_config, log_callback=None,
                     cancel_event=None, synced_files_tracker=None, structured_log=False):
        """
        立即执行同步任务（在当前线程中执行）。
        log_callback: 日志回调函数，用于 SSE 流式输出。
        cancel_event: threading.Event，用于外部取消信号。
        synced_files_tracker: 列表，记录本次已同步文件路径（用于回滚）。
        structured_log: 是否使用结构化日志回调。
        """
        push_config = self._get_push_config()
        engine = FileSyncEngine(
            task_config=task_config,
            db=self.db,
            base_dir=self.base_dir,
            push_config=push_config,
            cancel_event=cancel_event,
            synced_files_tracker=synced_files_tracker,
            structured_log=structured_log,
        )
        return engine.execute(log_callback=log_callback)

    def run_task_now_async(self, task_config, log_callback=None):
        """在新线程中执行同步任务"""
        thread = threading.Thread(
            target=self.run_task_now,
            args=[task_config, log_callback],
            daemon=True,
        )
        thread.start()
        return thread

    def get_all_status(self):
        """获取所有同步任务状态"""
        return self.db.get_all_task_status()

    def _get_push_config(self):
        """从配置中获取推送配置"""
        try:
            config = self.config_getter()
            return config.get("push_config", {})
        except Exception:
            return {}
