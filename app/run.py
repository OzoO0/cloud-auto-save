# !/usr/bin/env python3
# -*- coding: utf-8 -*-
from flask import (
    json,
    Flask,
    url_for,
    session,
    jsonify,
    request,
    redirect,
    Response,
    render_template,
    send_from_directory,
    stream_with_context,
)
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from concurrent.futures import ThreadPoolExecutor, as_completed
from sdk.cloudsaver import CloudSaver
from sdk.pansou import PanSou
from datetime import datetime, timezone, timedelta
import subprocess
import requests
from typing import Dict, List, Tuple
import hashlib
import secrets
import logging
import traceback
import base64
import queue
import threading
import sys
import os
import re
import time
import urllib.parse
import uuid

# 导入日志工具
try:
    from utils.logger import get_logger, log_function_call, log_execution_time
    logger = get_logger("app.run")
except ImportError:
    # 兼容模式
    logging.basicConfig(
        level=logging.DEBUG if os.getenv('DEBUG', 'false').lower() == 'true' else logging.INFO,
        format='%(asctime)s | %(levelname)-8s | %(name)s | %(funcName)s:%(lineno)d | %(message)s'
    )
    logger = logging.getLogger("app.run")
    def log_function_call(func):
        return func
    class log_execution_time:
        def __init__(self, *args, **kwargs):
            pass
        def __enter__(self):
            return self
        def __exit__(self, *args):
            return False

parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, parent_dir)
from quark_auto_save import Quark, Config, MagicRename

# 尝试导入多网盘支持模块
try:
    from adapters import (

        
        AdapterFactory, AccountManager, 
        QuarkAdapter, Cloud115Adapter,
        BaiduAdapter, XunleiAdapter, AliyunAdapter, UCAdapter
    )
    MULTI_DRIVE_SUPPORT = True
except ImportError:
    MULTI_DRIVE_SUPPORT = False

# 导入应用级缓存模块
from utils.cache import (
    make_cache_key, get_cached_fids, set_cached_fids,
    get_cached_lsdir, set_cached_lsdir, invalidate_all
)

print(
    r"""
   ____    ___   _____
  / __ \  /   | / ___/
 / / / / / /| | \__ \
/ /_/ / / ___ |___/ /
\___\_\/_/  |_/____/

-- Cloud-Auto-Save --
 """
)
sys.stdout.flush()


def get_app_ver():
    """获取应用版本"""
    logger.debug("[get_app_ver] 开始获取应用版本")
    try:
        with open("build.json", "r") as f:
            build_info = json.loads(f.read())
            BUILD_SHA = build_info["BUILD_SHA"]
            BUILD_TAG = build_info["BUILD_TAG"]
            logger.debug(f"[get_app_ver] 从 build.json 读取：BUILD_TAG={BUILD_TAG}, BUILD_SHA={BUILD_SHA}")
    except Exception as e:
        logger.warning(f"[get_app_ver] 读取 build.json 失败：{e}，尝试从环境变量读取")
        BUILD_SHA = os.getenv("BUILD_SHA", "")
        BUILD_TAG = os.getenv("BUILD_TAG", "")
        logger.debug(f"[get_app_ver] 从环境变量读取：BUILD_TAG={BUILD_TAG}, BUILD_SHA={BUILD_SHA}")
    
    if BUILD_TAG[:1] == "v":
        logger.debug(f"[get_app_ver] 返回正式版本号：{BUILD_TAG}")
        return BUILD_TAG
    elif BUILD_SHA:
        version = f"{BUILD_TAG}({BUILD_SHA[:7]})"
        logger.debug(f"[get_app_ver] 返回开发版本号：{version}")
        return version
    else:
        logger.debug(f"[get_app_ver] 返回 dev 版本")
        return "dev"


# 文件路径
PYTHON_PATH = "python3" if os.path.exists("/usr/bin/python3") else "python"
SCRIPT_PATH = os.environ.get("SCRIPT_PATH", "./quark_auto_save.py")
CONFIG_PATH = os.environ.get("CONFIG_PATH", "./config/quark_config.json")
PLUGIN_FLAGS = os.environ.get("PLUGIN_FLAGS", "")
DEBUG = os.environ.get("DEBUG", "false").lower() == "true"
HOST = os.environ.get("HOST", "0.0.0.0")
PORT = os.environ.get("PORT", 5005)
TASK_TIMEOUT = int(os.environ.get("TASK_TIMEOUT", 1800))

config_data = {}
task_plugins_config_default = {}
CONFIG_DB_KEY = "quark_config"
_config_lock = threading.Lock()

_cloud189_login_sessions = {}
_cloud189_login_sessions_lock = threading.Lock()
_cloud189_login_session_ttl = 300

def _cloud189_check_session_detail(session: requests.Session) -> Tuple[bool, str]:
    try:
        def _snip(s: str, n: int = 160) -> str:
            try:
                t = (s or "").strip().replace("\r", " ").replace("\n", " ")
                return t[:n]
            except Exception:
                return ""

        resp = session.get("https://cloud.189.cn/v2/getUserLevelInfo.action", timeout=15)
        text = resp.text or ""
        if resp.status_code != 200:
            if resp.status_code in (400, 401, 403):
                r2 = None
                r3 = None
                try:
                    r2 = session.get("https://cloud.189.cn/v2/getLoginedInfos.action", timeout=15)
                    if r2.status_code == 200:
                        j2 = r2.json()
                        if isinstance(j2, dict) and j2.get("userId"):
                            return True, ""
                except Exception:
                    pass
                try:
                    r3 = session.get(
                        "https://cloud.189.cn/api/portal/listFiles.action",
                        params={"fileId": "-11", "noCache": str(time.time())},
                        timeout=15,
                    )
                    if r3.status_code == 200:
                        j3 = r3.json()
                        if isinstance(j3, dict) and not j3.get("errorCode"):
                            return True, ""
                except Exception:
                    pass

                parts = [f"http={resp.status_code}", f"userLevel={_snip(text)}"]
                try:
                    if r2 is not None:
                        parts.append(f"logined={r2.status_code}:{_snip(r2.text or '')}")
                except Exception:
                    pass
                try:
                    if r3 is not None:
                        parts.append(f"portal={r3.status_code}:{_snip(r3.text or '')}")
                except Exception:
                    pass
                return False, " ".join(parts)
            return False, f"http={resp.status_code} body={_snip(text)}"
        if "InvalidSessionKey" in text:
            try:
                j = resp.json()
                if isinstance(j, dict) and j.get("errorMsg"):
                    return False, str(j.get("errorMsg"))
                if isinstance(j, dict) and j.get("errorCode"):
                    return False, str(j.get("errorCode"))
            except Exception:
                pass
            return False, "InvalidSessionKey"
        return True, ""
    except Exception as e:
        return False, str(e)


def _cloud189_finalize_login_and_get_cookies(session: requests.Session, to_url: str = "") -> dict[str, str]:
    def _extract_next_url(html: str) -> str:
        if not html:
            return ""
        patterns = [
            r'http-equiv=[\'"]refresh[\'"][^>]*content=[\'"][^>]*url=([^\'">\\s;]+)',
            r'(?:location\.href|window\.location|top\.location)\s*=\s*[\'"]([^\'"]+)[\'"]',
            r'location\.replace\(\s*[\'"]([^\'"]+)[\'"]\s*\)',
            r'(https?://[^\s\'"]+)',
        ]
        for p in patterns:
            m = re.search(p, html, re.I)
            if m:
                return (m.group(1) or "").strip()
        return ""

    def _follow(u: str, max_hops: int = 3):
        cur = (u or "").strip()
        visited = set()
        for _ in range(max_hops):
            if not cur or cur in visited:
                return
            visited.add(cur)
            resp = session.get(cur, timeout=15, allow_redirects=True)
            try:
                ct = (resp.headers.get("Content-Type") or "").lower()
            except Exception:
                ct = ""
            if "text/html" not in ct:
                return
            nxt = _extract_next_url(resp.text or "")
            if not nxt:
                return
            nxt = nxt.replace("&amp;", "&")
            try:
                if nxt.startswith("//"):
                    nxt = "https:" + nxt
                elif nxt.startswith("/"):
                    nxt = urllib.parse.urljoin(cur, nxt)
            except Exception:
                pass
            cur = nxt

    try:
        if to_url:
            _follow(to_url)
    except Exception:
        pass
    try:
        _follow("https://cloud.189.cn/main.action")
    except Exception:
        pass
    try:
        return session.cookies.get_dict()
    except Exception:
        return {}

# 数据同步模块
sync_db = None
sync_manager = None
DATAFILES_DIR = os.environ.get("DATAFILES_DIR", "./datafiles")

# 同步任务取消信号管理
_cancel_events = {}       # {task_id: threading.Event}
_cancel_actions = {}      # {task_id: "keep"|"rollback"}
_cancel_events_lock = threading.Lock()

_run_procs_lock = threading.Lock()
_run_procs = {}

app = Flask(__name__)
app.config["APP_VERSION"] = get_app_ver()


def _get_or_create_flask_secret_key():
    key = os.environ.get("FLASK_SECRET_KEY") or os.environ.get("SECRET_KEY")
    if key:
        return key

    config_dir = os.path.dirname(os.path.realpath(CONFIG_PATH))
    try:
        os.makedirs(config_dir, exist_ok=True)
    except OSError:
        pass

    key_path = os.path.join(config_dir, ".flask_secret_key")
    try:
        if os.path.exists(key_path):
            with open(key_path, "r", encoding="utf-8") as f:
                saved = f.read().strip()
            if saved:
                return saved
    except OSError:
        pass

    key = secrets.token_hex(32)
    try:
        with open(key_path, "w", encoding="utf-8") as f:
            f.write(key)
        try:
            os.chmod(key_path, 0o600)
        except OSError:
            pass
    except OSError:
        pass
    return key


app.secret_key = _get_or_create_flask_secret_key()
app.config["SESSION_COOKIE_NAME"] = "CLOUD_AUTO_SAVE_SESSION"
app.config["PERMANENT_SESSION_LIFETIME"] = timedelta(days=31)
app.json.ensure_ascii = False
app.json.sort_keys = False
app.jinja_env.variable_start_string = "[["
app.jinja_env.variable_end_string = "]]"

_BEIJING_TZ = timezone(timedelta(hours=8))


def _beijing_time_converter(ts):
    return datetime.fromtimestamp(ts, _BEIJING_TZ).timetuple()


logging.Formatter.converter = staticmethod(_beijing_time_converter)


class _APSchedulerBeijingTZFilter(logging.Filter):
    def filter(self, record):
        try:
            args = record.args
            if not args:
                return True

            if isinstance(args, tuple):
                new_args = []
                for a in args:
                    if isinstance(a, datetime):
                        dt = a
                        if dt.tzinfo is None:
                            dt = dt.replace(tzinfo=timezone.utc)
                        dt = dt.astimezone(_BEIJING_TZ)
                        new_args.append(dt.strftime("%Y-%m-%d %H:%M:%S%z"))
                        continue

                    if hasattr(a, "id") and hasattr(a, "name") and hasattr(a, "next_run_time"):
                        nr = getattr(a, "next_run_time", None)
                        if isinstance(nr, datetime):
                            dt = nr
                            if dt.tzinfo is None:
                                dt = dt.replace(tzinfo=timezone.utc)
                            dt = dt.astimezone(_BEIJING_TZ)
                            nr_s = dt.strftime("%Y-%m-%d %H:%M:%S%z")
                        else:
                            nr_s = str(nr)
                        new_args.append(f'{a.name} (id={a.id}, next run at: {nr_s})')
                        continue

                    new_args.append(a)

                record.args = tuple(new_args)
        except Exception:
            return True
        return True
logging.basicConfig(
    level=logging.DEBUG if DEBUG else logging.INFO,
    format="[%(asctime)s][%(levelname)s] %(message)s",
    datefmt="%m-%d %H:%M:%S",
)

logging.getLogger("apscheduler.executors").addFilter(_APSchedulerBeijingTZFilter())
logging.getLogger("apscheduler.executors.default").addFilter(_APSchedulerBeijingTZFilter())
scheduler = BackgroundScheduler(timezone=_BEIJING_TZ)
# 过滤werkzeug日志输出
if not DEBUG:
    logging.getLogger("werkzeug").setLevel(logging.ERROR)


    logging.getLogger("apscheduler").setLevel(logging.ERROR)
    sys.modules["flask.cli"].show_server_banner = lambda *x: None


def gen_md5(string):
    md5 = hashlib.md5()
    md5.update(string.encode("utf-8"))
    return md5.hexdigest()


def _normalize_cookie_list(cookie_val):
    if not cookie_val:
        return []
    if isinstance(cookie_val, str):
        s = cookie_val.strip()
        return [s] if s else []
    if isinstance(cookie_val, list):
        out = []
        for it in cookie_val:
            if isinstance(it, str):
                s = it.strip()
                if s:
                    out.append(s)
        return out
    return []


def _sanitize_config_data(data):
    if not isinstance(data, dict):
        data = {}

    cookie_list = _normalize_cookie_list(data.get("cookie"))

    accounts = data.get("accounts")
    if not isinstance(accounts, list):
        accounts = []

    if cookie_list and not accounts:
        accounts = []
        for idx, ck in enumerate(cookie_list):
            accounts.append(
                {
                    "name": f"夸克{idx + 1}",
                    "drive_type": "quark",
                    "cookie": ck,
                    "enabled": True,
                    "default": idx == 0,
                }
            )

    for acc in accounts:
        if not isinstance(acc, dict):
            continue
        acc.setdefault("name", "")
        acc.setdefault("drive_type", "quark")
        acc.setdefault("cookie", "")
        acc.setdefault("enabled", True)
        acc.setdefault("default", False)

    data["accounts"] = accounts

    if "cookie" in data:
        data.pop("cookie", None)

    return data


def _load_config_from_db():
    if not sync_db:
        return None
    try:
        username = os.environ.get("WEBUI_USERNAME") or "admin"
        data = sync_db.export_config_dict(username=username)
        if data:
            return _sanitize_config_data(data)
    except Exception:
        pass
    raw = sync_db.get_app_config(CONFIG_DB_KEY)
    if not raw:
        return None
    try:
        data = json.loads(raw)
    except Exception:
        return None
    return _sanitize_config_data(data)


def _save_config_to_db(data):
    if not sync_db:
        return False
    if not isinstance(data, dict):
        data = {}
    if "webui" not in data:
        if isinstance(config_data, dict) and isinstance(config_data.get("webui"), dict):
            data["webui"] = config_data["webui"]
        else:
            data["webui"] = {"username": "admin", "password": "admin123"}
    try:
        if hasattr(sync_db, "import_config_dict"):
            sync_db.import_config_dict(data)
            try:
                payload = json.dumps(data, ensure_ascii=False)
                sync_db.set_app_config(CONFIG_DB_KEY, payload)
            except Exception:
                pass
            return True
    except Exception:
        pass
    try:
        payload = json.dumps(data, ensure_ascii=False)
    except Exception:
        payload = "{}"
    return sync_db.set_app_config(CONFIG_DB_KEY, payload)


def _reload_config_data():
    global config_data
    loaded = _load_config_from_db()
    if loaded is not None:
        config_data = loaded
    return config_data


def _get_default_account_cookie(drive_type="quark"):
    accounts = config_data.get("accounts") if isinstance(config_data, dict) else []
    if not isinstance(accounts, list):
        return ""
    enabled = [a for a in accounts if isinstance(a, dict) and a.get("enabled", True) and a.get("drive_type") == drive_type]
    if not enabled:
        return ""
    for a in enabled:
        if a.get("default") or a.get("is_default"):
            return (a.get("cookie") or "").strip()
    return (enabled[0].get("cookie") or "").strip()


def _export_config_to_tempfile():
    with _config_lock:
        cfg = _reload_config_data()
        export_data = json.loads(json.dumps(cfg, ensure_ascii=False)) if isinstance(cfg, dict) else {}
    config_dir = os.path.dirname(os.path.realpath(CONFIG_PATH))
    try:
        os.makedirs(config_dir, exist_ok=True)
    except Exception:
        pass
    tmp_name = f".quark_config.export.{int(time.time() * 1000)}.json"
    export_path = os.path.join(config_dir, tmp_name)
    Config.write_json(export_path, export_data)
    return export_path


def _task_match_key(task):
    if not isinstance(task, dict):
        return None
    task_uid = str(task.get("task_uid", "") or "").strip()
    if task_uid:
        return ("uid", task_uid)
    shareurl = str(task.get("shareurl", "") or "").strip()
    savepath = str(task.get("savepath", "") or "").strip()
    if shareurl or savepath:
        return ("sp", shareurl, savepath)
    return None


def _extract_task_bans_from_config_file(export_path):
    if not export_path:
        return {}
    try:
        if not os.path.exists(export_path):
            return {}
    except Exception:
        return {}
    try:
        with open(export_path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        return {}
    tasklist = data.get("tasklist", [])
    if not isinstance(tasklist, list):
        return {}
    bans = {}
    for task in tasklist:
        if not isinstance(task, dict):
            continue
        ban = task.get("shareurl_ban")
        if ban is None:
            continue
        ban = str(ban).strip()
        if not ban:
            continue
        k = _task_match_key(task)
        if k:
            bans[k] = ban
    return bans


def _merge_task_bans_into_main_config(bans_map):
    if not bans_map:
        return 0
    changed = 0
    with _config_lock:
        cfg = _reload_config_data()
        if not isinstance(cfg, dict):
            return 0
        tasklist = cfg.get("tasklist", [])
        if not isinstance(tasklist, list):
            return 0
        for task in tasklist:
            if not isinstance(task, dict):
                continue
            k = _task_match_key(task)
            if not k:
                continue
            ban = bans_map.get(k)
            if not ban:
                continue
            if str(task.get("shareurl_ban", "") or "") == ban:
                continue
            task["shareurl_ban"] = ban
            changed += 1
        if changed > 0:
            cfg = _sanitize_config_data(cfg)
            globals()["config_data"] = cfg
            try:
                if sync_db:
                    _save_config_to_db(cfg)
                else:
                    Config.write_json(CONFIG_PATH, cfg)
            except Exception:
                pass
    return changed


def _handle_linked_sync_tasks(tasklist, run_id=None):
    """
    处理转存任务完成后的关联同步任务触发

    Args:
        tasklist: 转存任务列表
        run_id: 执行批次标识
    """
    if not tasklist:
        return

    # 收集所有启用了关联的同步任务配置
    sync_configs = []
    for task in tasklist:
        sync_cfg = task.get("sync_task_config")
        if not sync_cfg:
            continue
        # 检查是否启用（类似插件的 enable 判断逻辑）
        if not sync_cfg.get("enable"):
            continue
        sync_task_id = sync_cfg.get("sync_task_id")
        if not sync_task_id:
            continue
        sync_configs.append({
            "sync_task_id": sync_task_id,
            "delay_seconds": sync_cfg.get("delay_seconds", 0)
        })

    if sync_configs:
        _trigger_linked_sync_tasks(sync_configs, run_id)


def _trigger_linked_sync_tasks(sync_configs, run_id=None):
    """
    触发关联的同步任务

    Args:
        sync_configs: [{'sync_task_id': 'xxx', 'delay_seconds': 0}, ...]
        run_id: 批量执行时的批次标识（用于日志区分）
    """
    if not sync_configs or not sync_manager:
        return

    # 1. 去重：根据 sync_task_id 去重
    unique_task_map = {}  # {sync_task_id: delay_seconds} - 保留最小的 delay_seconds
    for cfg in sync_configs:
        task_id = cfg.get("sync_task_id")
        if not task_id:
            continue
        delay = cfg.get("delay_seconds", 0)
        delay = max(0, int(delay) if delay else 0)
        if task_id not in unique_task_map or delay < unique_task_map[task_id]:
            unique_task_map[task_id] = delay

    if not unique_task_map:
        return

    # 2. 获取同步任务配置
    sync_tasks = config_data.get("sync_tasks", [])
    task_map = {t.get("task_id"): t for t in sync_tasks if t.get("task_id")}

    # 3. 按 delay_seconds 分组
    from collections import defaultdict
    delay_groups = defaultdict(list)
    for task_id, delay in unique_task_map.items():
        if task_id in task_map:
            delay_groups[delay].append(task_id)

    # 4. 调度触发
    for delay, task_ids in delay_groups.items():
        if delay > 0:
            # 延迟触发
            threading.Timer(delay, _execute_linked_sync_tasks,
                          args=(task_ids, run_id)).start()
            logger.info(f"[关联同步] 已安排 {len(task_ids)} 个任务在 {delay} 秒后执行")
        else:
            # 立即触发
            _execute_linked_sync_tasks(task_ids, run_id)


def _execute_linked_sync_tasks(task_ids, run_id=None):
    """执行关联的同步任务"""
    if not task_ids or not sync_manager:
        return

    sync_tasks = config_data.get("sync_tasks", [])
    task_map = {t.get("task_id"): t for t in sync_tasks if t.get("task_id")}

    for task_id in task_ids:
        task_config = task_map.get(task_id)
        if not task_config:
            logger.warning(f"[关联同步] 未找到同步任务: {task_id}")
            continue

        # 检查任务是否启用
        if not task_config.get("enabled", True):
            logger.info(f"[关联同步] 同步任务已禁用，跳过: {task_config.get('taskname', task_id)}")
            continue

        try:
            task_config = dict(task_config)
            task_config["_trigger"] = "linked"
            if run_id:
                task_config["_run_id"] = run_id

            sync_manager.run_task_now_async(task_config)
            logger.info(f"[关联同步] 已触发同步任务: {task_config.get('taskname', task_id)}")
        except Exception as e:
            logger.error(f"[关联同步] 触发同步任务失败 [{task_id}]: {e}")


def get_login_token():
    username = config_data["webui"]["username"]
    password = config_data["webui"]["password"]
    return gen_md5(f"token{username}{password}+-*/")[8:24]


def is_login():
    login_token = get_login_token()
    if session.get("token") == login_token or request.args.get("token") == login_token:
        return True
    else:
        return False


def get_account_by_name(account_name=None):
    """
    根据账户名称获取对应的适配器或 Quark 实例
    
    Args:
        account_name: 账户名称，None 或 'auto' 表示使用默认账户
    
    Returns:
        tuple: (adapter/quark实例, drive_type)
    """
    accounts = config_data.get("accounts", []) if isinstance(config_data, dict) else []
    if not isinstance(accounts, list):
        accounts = []
    enabled_accounts = [acc for acc in accounts if isinstance(acc, dict) and acc.get("enabled", True)]
    if not enabled_accounts:
        return None, None

    target_account = None
    if account_name and account_name != "auto":
        for acc in enabled_accounts:
            if acc.get("name") == account_name:
                target_account = acc
                break

    if not target_account:
        for acc in enabled_accounts:
            if acc.get("is_default") or acc.get("default"):
                target_account = acc
                break
        if not target_account:
            target_account = enabled_accounts[0]

    drive_type = target_account.get("drive_type", "quark")
    cookie = (target_account.get("cookie") or "").strip()

    if MULTI_DRIVE_SUPPORT:
        adapter = AdapterFactory.create_adapter(drive_type, cookie, 0)
        if adapter:
            return adapter, drive_type
        if drive_type != "quark":
            return None, None
        fallback = AdapterFactory.create_adapter("quark", cookie, 0)
        return (fallback, "quark") if fallback else (None, None)

    if drive_type == "quark" and cookie:
        return Quark(cookie), "quark"
    return None, None


def get_adapter_for_url(shareurl):
    """
    根据分享链接 URL 自动选择合适的适配器
    
    Args:
        shareurl: 分享链接 URL
    
    Returns:
        tuple: (adapter/quark实例, drive_type)
    """
    if not MULTI_DRIVE_SUPPORT:
        ck = _get_default_account_cookie("quark")
        return (Quark(ck), "quark") if ck else (None, None)
    
    # 根据 URL 判断网盘类型
    drive_type = AdapterFactory.get_drive_type_by_url(shareurl)
    logging.debug(f">>> URL检测: {shareurl[:50]}... -> drive_type={drive_type}")
    
    if not drive_type:
        logging.warning(f">>> 无法识别的分享链接类型: {shareurl}")
        return None, None
    
    # 从账户中查找对应类型的可用账户
    if config_data.get("accounts"):
        accounts = config_data.get("accounts", [])
        logging.debug(f">>> 查找 {drive_type} 类型账户，共有 {len(accounts)} 个账户")
        for acc in accounts:
            acc_enabled = acc.get("enabled", True)
            acc_type = acc.get("drive_type")
            logging.debug(f">>>   账户 '{acc.get('name')}': type={acc_type}, enabled={acc_enabled}")
            if acc_enabled and acc_type == drive_type:
                cookie = acc.get("cookie", "")
                logging.info(f">>> 使用账户 '{acc.get('name')}' ({drive_type})")
                # 使用工厂创建适配器
                adapter = AdapterFactory.create_adapter(drive_type, cookie, 0)
                if adapter:
                    return adapter, drive_type
    
    # 回退到旧格式
    logging.warning(f">>> 未找到 {drive_type} 类型的可用账户")
    return None, None


# 设置icon
@app.route("/favicon.ico")
def favicon():
    return send_from_directory(
        os.path.join(app.root_path, "static"),
        "favicon.ico",
        mimetype="image/vnd.microsoft.icon",
    )


# 登录页面
@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        username = config_data["webui"]["username"]
        password = config_data["webui"]["password"]
        # 验证用户名和密码
        if (username == request.form.get("username")) and (
            password == request.form.get("password")
        ):
            logging.info(f">>> 用户 {username} 登录成功")
            session.permanent = True
            session["token"] = get_login_token()
            return redirect(url_for("index"))
        else:
            logging.info(f">>> 用户 {username} 登录失败")
            return render_template("login.html", message="登录失败")

    if is_login():
        return redirect(url_for("index"))
    return render_template("login.html", error=None)


# 退出登录
@app.route("/logout")
def logout():
    session.pop("token", None)
    return redirect(url_for("login"))


# 管理页面
@app.route("/")
def index():
    if not is_login():
        return redirect(url_for("login"))
    return render_template(
        "index.html", version=app.config["APP_VERSION"], plugin_flags=PLUGIN_FLAGS
    )


@app.route("/db-console")
def db_console():
    if not is_login():
        return redirect(url_for("login"))
    return render_template("db_console.html", version=app.config["APP_VERSION"])


@app.route("/api/db/info")
def db_info():
    if not is_login():
        return jsonify({"success": False, "message": "未登录"})
    if not sync_db:
        return jsonify({"success": False, "message": "数据库未初始化"})
    try:
        r = sync_db.execute_sql(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name ASC"
        )
        tables = [row.get("name") for row in (r.get("rows") or [])] if r.get("ok") else []
        keys = sync_db.list_app_config_keys()
        return jsonify(
            {
                "success": True,
                "data": {
                    "db_path": getattr(sync_db, "db_path", ""),
                    "tables": tables,
                    "app_config_keys": keys,
                },
            }
        )
    except Exception as e:
        return jsonify({"success": False, "message": str(e)})


@app.route("/api/db/exec", methods=["POST"])
def db_exec():
    if not is_login():
        return jsonify({"success": False, "message": "未登录"})
    if not sync_db:
        return jsonify({"success": False, "message": "数据库未初始化"})
    sql_text = (request.json or {}).get("sql", "")
    result = sync_db.execute_sql(sql_text)
    if result.get("ok"):
        return jsonify({"success": True, "data": result})
    return jsonify({"success": False, "message": result.get("error", "执行失败")})


@app.route("/api/db/tables")
def db_tables():
    if not is_login():
        return jsonify({"success": False, "message": "未登录"})
    if not sync_db:
        return jsonify({"success": False, "message": "数据库未初始化"})
    try:
        return jsonify({"success": True, "data": {"tables": sync_db.list_tables()}})
    except Exception as e:
        return jsonify({"success": False, "message": str(e)})


@app.route("/api/db/table/<table_name>/meta")
def db_table_meta(table_name):
    if not is_login():
        return jsonify({"success": False, "message": "未登录"})
    if not sync_db:
        return jsonify({"success": False, "message": "数据库未初始化"})
    try:
        meta = sync_db.get_table_meta(table_name)
        if not meta.get("columns"):
            return jsonify({"success": False, "message": "表不存在或不可访问"})
        return jsonify({"success": True, "data": meta})
    except Exception as e:
        return jsonify({"success": False, "message": str(e)})


@app.route("/api/db/table/<table_name>/rows")
def db_table_rows(table_name):
    if not is_login():
        return jsonify({"success": False, "message": "未登录"})
    if not sync_db:
        return jsonify({"success": False, "message": "数据库未初始化"})
    page = request.args.get("page", "1")
    page_size = request.args.get("page_size", "50")
    include_deleted = request.args.get("include_deleted", "0") == "1"
    order_by = request.args.get("order_by")
    order_dir = request.args.get("order_dir", "DESC")
    q = request.args.get("q")
    try:
        data = sync_db.get_table_rows(
            table_name,
            page=int(page),
            page_size=int(page_size),
            include_deleted=include_deleted,
            order_by=order_by,
            order_dir=order_dir,
            q=q,
        )
        if not data.get("ok"):
            return jsonify({"success": False, "message": data.get("error", "查询失败")})
        return jsonify({"success": True, "data": data})
    except Exception as e:
        return jsonify({"success": False, "message": str(e)})


@app.route("/api/db/table/<table_name>/row", methods=["POST"])
def db_table_upsert_row(table_name):
    if not is_login():
        return jsonify({"success": False, "message": "未登录"})
    if not sync_db:
        return jsonify({"success": False, "message": "数据库未初始化"})
    row_data = request.json or {}
    try:
        r = sync_db.upsert_table_row(table_name, row_data)
        if r.get("ok"):
            return jsonify({"success": True, "data": r})
        return jsonify({"success": False, "message": r.get("error", "保存失败")})
    except Exception as e:
        return jsonify({"success": False, "message": str(e)})


@app.route("/api/db/table/<table_name>/row/delete", methods=["POST"])
def db_table_delete_row(table_name):
    if not is_login():
        return jsonify({"success": False, "message": "未登录"})
    if not sync_db:
        return jsonify({"success": False, "message": "数据库未初始化"})
    pk_value = (request.json or {}).get("pk_value")
    if pk_value is None or pk_value == "":
        return jsonify({"success": False, "message": "缺少主键值"})
    try:
        r = sync_db.delete_table_row(table_name, pk_value)
        if r.get("ok"):
            return jsonify({"success": True, "data": r})
        return jsonify({"success": False, "message": r.get("error", "删除失败")})
    except Exception as e:
        return jsonify({"success": False, "message": str(e)})


# 获取配置数据
@app.route("/data")
def get_data():
    if not is_login():
        return jsonify({"success": False, "message": "未登录"})
    with _config_lock:
        cfg = _reload_config_data()
        data = json.loads(json.dumps(cfg, ensure_ascii=False)) if isinstance(cfg, dict) else {}
    data.pop("webui", None)
    data["api_token"] = get_login_token()
    data["task_plugins_config_default"] = task_plugins_config_default
    # 添加多网盘支持标识
    data["multi_drive_support"] = MULTI_DRIVE_SUPPORT
    # 确保 accounts 字段存在（新格式支持）
    if "accounts" not in data:
        data["accounts"] = []
    return jsonify({"success": True, "data": data})


# 更新数据
@app.route("/update", methods=["POST"])
def update():
    global config_data
    if not is_login():
        return jsonify({"success": False, "message": "未登录"})
    with _config_lock:
        config_data = _reload_config_data()

    dont_save_keys = ["task_plugins_config_default", "api_token", "sync_tasks", "cookie"]
    
    # 处理阿里云盘/迅雷网盘 refresh_token 保护
    # 对于这两种网盘类型，一旦配置了 cookie，就只能通过专门的刷新接口更新
    incoming_accounts = request.json.get("accounts", [])
    if incoming_accounts and MULTI_DRIVE_SUPPORT:
        current_accounts = config_data.get("accounts", [])
        # 构建当前需要 token 保护的账户映射（阿里云盘和迅雷网盘）
        current_token_protected = {}
        for acc in current_accounts:
            if acc.get("drive_type") in ("aliyun", "xunlei"):
                name = acc.get("name", "")
                drive_type = acc.get("drive_type")
                key = f"{drive_type}:{name}"
                current_cookie = acc.get("cookie", "")
                if current_cookie:  # 只有已填写 cookie 的账户才需要保护
                    current_token_protected[key] = {
                        "cookie": current_cookie,
                        "_token_updated_at": acc.get("_token_updated_at", 0),
                    }
        
        # 对于已有 cookie 的阿里云盘/迅雷网盘账户，强制保留当前配置的 cookie
        for acc in incoming_accounts:
            if acc.get("drive_type") in ("aliyun", "xunlei"):
                name = acc.get("name", "")
                drive_type = acc.get("drive_type")
                key = f"{drive_type}:{name}"
                if key in current_token_protected:
                    current_info = current_token_protected[key]
                    # 无论传入什么值，都使用当前配置的 cookie（防止前端修改）
                    logging.debug(f"[{drive_type}] 保护账户 {name} 的 token，忽略传入的修改")
                    acc["cookie"] = current_info["cookie"]
                    acc["_token_updated_at"] = current_info["_token_updated_at"]
    
    for key, value in request.json.items():
        if key not in dont_save_keys:
            config_data.update({key: value})
    config_data = _sanitize_config_data(config_data)
    with _config_lock:
        _save_config_to_db(config_data)
    # 配置变更时清空适配器实例缓存，确保新配置生效
    if MULTI_DRIVE_SUPPORT:
        AdapterFactory.clear_cache()
    invalidate_all()  # 同步清空应用级预览缓存
    # 重新加载任务
    if reload_tasks():
        logging.info(f">>> 配置更新成功")
        return jsonify({"success": True, "message": "配置更新成功"})
    else:
        logging.info(f">>> 配置更新失败")
        return jsonify({"success": False, "message": "配置更新失败"})


# 处理运行脚本请求
@app.route("/run_script_now", methods=["POST"])
def run_script_now():
    if not is_login():
        return jsonify({"success": False, "message": "未登录"})
    tasklist = request.json.get("tasklist", [])
    logging.info(
        f">>> 手动运行任务 [{tasklist[0].get('taskname') if len(tasklist)>0 else 'ALL'}] 开始执行..."
    )

    def generate_output():
        run_id = f"manual_{int(time.time() * 1000)}"
        export_path = None
        try:
            export_path = _export_config_to_tempfile()
        except Exception:
            export_path = None

        # 设置环境变量
        process_env = os.environ.copy()
        process_env["PYTHONIOENCODING"] = "utf-8"
        if request.json.get("quark_test"):
            process_env["QUARK_TEST"] = "true"
            ck = _get_default_account_cookie("quark")
            cookies = [ck] if ck else []
            process_env["COOKIE"] = json.dumps(cookies, ensure_ascii=False)
            push_cfg = request.json.get("push_config")
            if push_cfg is None:
                push_cfg = config_data.get("push_config", {})
            process_env["PUSH_CONFIG"] = json.dumps(push_cfg, ensure_ascii=False)
        if tasklist:
            process_env["TASKLIST"] = json.dumps(tasklist, ensure_ascii=False)
        command = [PYTHON_PATH, "-u", SCRIPT_PATH, export_path or CONFIG_PATH]
        process = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            universal_newlines=True,
            encoding="utf-8",
            errors="replace",
            bufsize=1,
            env=process_env,
        )
        with _run_procs_lock:
            _run_procs[run_id] = {"type": "script", "process": process}
        try:
            for line in iter(process.stdout.readline, ""):
                s = line.rstrip("\n")
                if s:
                    logging.info(s)
                try:
                    yield f"data: {line}\n\n"
                except GeneratorExit:
                    break
            try:
                yield "data: [DONE]\n\n"
            except GeneratorExit:
                pass
        finally:
            with _run_procs_lock:
                _run_procs.pop(run_id, None)
            process.stdout.close()
            try:
                process.wait(timeout=2)
            except Exception:
                try:
                    process.terminate()
                except Exception:
                    pass
                try:
                    process.wait(timeout=2)
                except Exception:
                    try:
                        process.kill()
                    except Exception:
                        pass

            # 脚本执行完成后，触发关联的同步任务
            _handle_linked_sync_tasks(tasklist, run_id)

            if export_path and os.path.exists(export_path):
                try:
                    bans = _extract_task_bans_from_config_file(export_path)
                    changed = _merge_task_bans_into_main_config(bans)
                    if changed > 0:
                        logging.info(f">>> 已同步 {changed} 个任务的 shareurl_ban")
                except Exception:
                    pass
                try:
                    os.remove(export_path)
                except Exception:
                    pass

    return Response(
        stream_with_context(generate_output()),
        content_type="text/event-stream;charset=utf-8",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )


@app.route("/task_suggestions")
def get_task_suggestions():
    if not is_login():
        return jsonify({"success": False, "message": "未登录"})
    query = request.args.get("q", "").lower()
    deep = request.args.get("d", "").lower()
    net_data = config_data.get("source", {}).get("net", {})
    cs_data = config_data.get("source", {}).get("cloudsaver", {})
    ps_data = config_data.get("source", {}).get("pansou", {})

    def net_search():
        if str(net_data.get("enable", "true")).lower() != "false":
            base_url = base64.b64decode("aHR0cHM6Ly9zLjkxNzc4OC54eXo=").decode()
            url = f"{base_url}/task_suggestions?q={query}&d={deep}"
            response = requests.get(url)
            return response.json()
        return []

    def cs_search():
        if (
            cs_data.get("server")
            and cs_data.get("username")
            and cs_data.get("password")
        ):
            cs = CloudSaver(cs_data.get("server"))
            cs.set_auth(
                cs_data.get("username", ""),
                cs_data.get("password", ""),
                cs_data.get("token", ""),
            )
            search = cs.auto_login_search(query)
            if search.get("success"):
                if search.get("new_token"):
                    cs_data["token"] = search.get("new_token")
                    with _config_lock:
                        _save_config_to_db(_sanitize_config_data(config_data))
                search_results = cs.clean_search_results(search.get("data"))
                return search_results
        return []

    def ps_search():
        if ps_data.get("server"):
            ps = PanSou(ps_data.get("server"))
            return ps.search(query, deep == "1")
        return []

    try:
        search_results = []
        with ThreadPoolExecutor(max_workers=3) as executor:
            features = []
            # features.append(executor.submit(net_search))
            features.append(executor.submit(cs_search))
            features.append(executor.submit(ps_search))
            for future in as_completed(features):
                result = future.result()
                search_results.extend(result)

        # 按时间排序并去重
        results = []
        link_array = []
        search_results.sort(key=lambda x: x.get("datetime", ""), reverse=True)
        for item in search_results:
            url = item.get("shareurl", "")
            if url != "" and url not in link_array:
                link_array.append(url)
                results.append(item)

        return jsonify({"success": True, "data": results})
    except Exception as e:
        return jsonify({"success": True, "message": f"error: {str(e)}"})


@app.route("/get_share_detail", methods=["POST"])
def get_share_detail():
    """获取分享详情接口，支持正则预览处理（串行复用连接 + 三层缓存）"""
    logger.debug(f"[get_share_detail] 收到请求")
    
    if not is_login():
        logger.warning(f"[get_share_detail] 用户未登录")
        return jsonify({"success": False, "message": "未登录"})
    
    try:
        def _mask_share_secret(v: str) -> str:
            if not v:
                return ""
            return "***" if len(v) <= 6 else f"{v[:2]}***{v[-2:]}"

        # 提前提取所有请求参数（避免子线程访问 Flask request 上下文）
        shareurl = request.json.get("shareurl", "")
        stoken = request.json.get("stoken", "")
        account_name = request.json.get("account_name", "")
        task = request.json.get("task", {})
        magic_regex = request.json.get("magic_regex", {})
        
        logger.debug(f"[get_share_detail] 请求参数：shareurl={shareurl[:50] if shareurl else 'None'}..., account_name={account_name}")
        if shareurl:
            try:
                u = urllib.parse.urlparse(shareurl)
                logger.debug(
                    f"[get_share_detail] shareurl parsed: host={u.netloc}, path={u.path}, "
                    f"query_keys={list(urllib.parse.parse_qs(u.query).keys())}, fragment={u.fragment[:50] if u.fragment else ''}"
                )
            except Exception as e:
                logger.debug(f"[get_share_detail] shareurl parse failed: {e}")
        logger.debug(f"[get_share_detail] 任务配置：pattern={task.get('pattern', '')}, replace={task.get('replace', '')}, sort_index={task.get('sort_index', 1)}")
        
        # 根据 URL 或指定账户获取适配器
        if account_name and account_name != "auto":
            account, drive_type = get_account_by_name(account_name)
            logger.debug(f"[get_share_detail] 使用指定账户：{account_name}, 类型：{drive_type}")
        else:
            account, drive_type = get_adapter_for_url(shareurl)
            logger.debug(f"[get_share_detail] 自动检测账户类型：{drive_type}")
        
        if not account:
            detected_type = AdapterFactory.get_drive_type_by_url(shareurl) if MULTI_DRIVE_SUPPORT else "quark"
            type_label = {"quark": "夸克网盘", "115": "115 网盘"}.get(detected_type, detected_type)
            logger.error(f"[get_share_detail] 未配置{type_label}账户")
            return jsonify({"success": False, "data": {"error": f"未配置有效的{type_label}账户，请先在「系统配置」→「多网盘账户」中添加{type_label}账户"}})
        
        pwd_id, passcode, pdir_fid, paths = account.extract_url(shareurl)
        logger.debug(
            f"[get_share_detail] extract_url: drive_type={drive_type}, pwd_id={pwd_id}, pdir_fid={pdir_fid}, "
            f"passcode_present={bool(passcode)}, paths_len={len(paths) if isinstance(paths, list) else 'na'}"
        )
        
        # ── 判断是否需要预览 ──
        need_preview = bool(task)
        savepath = task.get("savepath", "") if task else ""
        
        # 解析预览账户（主线程完成，避免线程竞态）
        preview_account = None
        if need_preview and savepath:
            task_account_name = task.get("account_name", "")
            if task_account_name and task_account_name != "auto":
                preview_account, _ = get_account_by_name(task_account_name)
            if not preview_account:
                preview_account = account
            if not preview_account:
                ck = _get_default_account_cookie("quark")
                if ck and MULTI_DRIVE_SUPPORT:
                    preview_account = AdapterFactory.create_adapter("quark", ck, 0)
                elif ck:
                    preview_account = Quark(ck)
        
        # ── 定义两个并行任务 ──
        def fetch_share_data():
            """线程 A：获取分享详情（stoken → get_detail）"""
            nonlocal stoken
            if not stoken:
                logger.debug(f"[fetch_share] stoken missing, calling get_stoken(pwd_id={pwd_id}, passcode_present={bool(passcode)})")
                get_stoken = account.get_stoken(pwd_id, passcode)
                if get_stoken.get("status") == 200:
                    stoken = get_stoken["data"]["stoken"]
                    logger.debug(f"[fetch_share] get_stoken ok, stoken_present={bool(stoken)}")
                else:
                    logger.warning(f"[fetch_share] get_stoken failed: status={get_stoken.get('status')}, message={get_stoken.get('message')}")
                    return {"error": get_stoken.get("message", "获取 stoken 失败")}
            else:
                logger.debug(f"[fetch_share] stoken provided by client, stoken_present={bool(stoken)}")
            
            share_detail = account.get_detail(
                pwd_id, stoken, pdir_fid, _fetch_share=1, fetch_share_full_path=1
            )
            logger.debug(
                f"[fetch_share] get_detail done: code={share_detail.get('code')}, message={share_detail.get('message')}, "
                f"pdir_fid={pdir_fid}, list_len={len(share_detail.get('data', {}).get('list', []))}"
            )
            return {"share_detail": share_detail, "stoken": stoken}
        
        def fetch_dir_data():
            """线程 B：获取目标目录文件列表（缓存命中直接返回，否则走原始 API）"""
            if not preview_account or not savepath:
                return []
            
            savepath_normalized = re.sub(r'/+', '/', savepath)
            try:
                drive_type_key = getattr(preview_account, 'DRIVE_TYPE', 'quark')
                cookie_key = getattr(preview_account, 'cookie', '')
                use_cache = drive_type_key not in ("123pan", "cloud189")

                # 优先读缓存（不阻塞）
                fid = preview_account.savepath_fid.get(savepath_normalized)
                if not fid:
                    fids_cache_key = make_cache_key(drive_type_key, cookie_key, 'fids', savepath_normalized)
                    fid = get_cached_fids(fids_cache_key) if use_cache else None

                if fid:
                    lsdir_cache_key = make_cache_key(drive_type_key, cookie_key, 'lsdir', str(fid))
                    cached_ls = get_cached_lsdir(lsdir_cache_key) if use_cache else None
                    if cached_ls is not None:
                        logger.debug(f"[fetch_dir] 缓存命中，{len(cached_ls)} 个文件")
                        return cached_ls

                # 无缓存：走原始 API 获取 fid + 目录列表
                if not fid:
                    get_fids_result = preview_account.get_fids([savepath_normalized])
                    if get_fids_result:
                        fid = get_fids_result[0]["fid"]
                        fids_cache_key = make_cache_key(drive_type_key, cookie_key, 'fids', savepath_normalized)
                        if use_cache:
                            set_cached_fids(fids_cache_key, fid)

                if fid:
                    ls_result = preview_account.ls_dir(fid, max_items=2000)
                    if ls_result and "data" in ls_result:
                        dir_list = ls_result["data"].get("list", [])
                        lsdir_cache_key = make_cache_key(drive_type_key, cookie_key, 'lsdir', str(fid))
                        if use_cache:
                            set_cached_lsdir(lsdir_cache_key, dir_list)
                        logger.debug(f"[fetch_dir] API 获取目录，{len(dir_list)} 个文件")
                        return dir_list
            except Exception as e:
                logger.warning(f"[fetch_dir] 获取目标目录失败：{e}")
            return []
        
        # ── 并行执行：分享详情 & 目录查询同时进行 ──
        if need_preview and savepath and preview_account:
            logger.debug(f"[get_share_detail] 并行获取：分享详情 + 目录列表")
            with ThreadPoolExecutor(max_workers=2) as executor:
                future_share = executor.submit(fetch_share_data)
                future_dir = executor.submit(fetch_dir_data)
                share_result = future_share.result(timeout=60)
                dir_file_list = future_dir.result(timeout=60)
        else:
            share_result = fetch_share_data()
            dir_file_list = []
        
        # ── 处理分享数据 ──
        if "error" in share_result:
            return jsonify({"success": False, "data": {"error": share_result["error"]}})
        
        share_detail = share_result["share_detail"]
        stoken = share_result["stoken"]
        
        if share_detail.get("code") != 0:
            logger.error(f"[get_share_detail] 分享详情获取失败：{share_detail.get('message')}")
            return jsonify(
                {"success": False, "data": {"error": share_detail.get("message")}}
            )

        data = share_detail["data"]
        data["paths"] = [
            {"fid": i["fid"], "name": i["file_name"]}
            for i in share_detail["data"].get("full_path", [])
        ] or paths
        data["stoken"] = stoken
        data["drive_type"] = drive_type
        logger.debug(f"[get_share_detail] 分享详情获取成功，路径：{'/'.join([p['name'] for p in data['paths']])}")

        # ── 正则处理预览（dir_file_list 已通过并行获取）──
        if need_preview:
            _apply_preview_regex(data, task, magic_regex, dir_file_list, preview_account, account)

        return jsonify({"success": True, "data": data})
    except Exception as e:
        logging.error(f">>> get_share_detail 错误: {str(e)}")
        return jsonify({"success": False, "data": {"error": f"获取分享详情失败: {str(e)}"}})


def _apply_preview_regex(data, task, magic_regex, dir_file_list, preview_account, share_account):
    """
    对分享文件列表应用正则预览处理（纯 CPU 操作，使用预先获取的 dir_file_list）。

    Args:
        data: 分享文件列表数据
        task: 任务配置字典
        magic_regex: 正则规则集合
        dir_file_list: 目标目录已有文件列表（已通过并行线程预取）
        preview_account: 预览使用的网盘账户
        share_account: 分享来源账户（后备）
    """
    logger.debug(f"[preview_regex] 开始执行正则预览处理")
    mr = MagicRename(magic_regex)
    mr.set_taskname(task.get("taskname", ""))
    logger.debug(f"[preview_regex] 任务名：{task.get('taskname', '')}, 正则规则数：{len(magic_regex)}")

    # 如果未通过并行获取 preview_account，尝试解析
    if not preview_account:
        task_account_name = task.get("account_name", "")
        if task_account_name and task_account_name != "auto":
            preview_account, _ = get_account_by_name(task_account_name)
        if not preview_account and share_account:
            preview_account = share_account
        if not preview_account:
            ck = _get_default_account_cookie("quark")
            if ck and MULTI_DRIVE_SUPPORT:
                preview_account = AdapterFactory.create_adapter("quark", ck, 0)
            elif ck:
                preview_account = Quark(ck)
            else:
                logger.warning(f"[preview_regex] 未配置任何账户，跳过预览")
                return

    # 如果 dir_file_list 未通过并行获取（无 savepath 场景），这里补充获取
    savepath = task.get("savepath", "")
    if savepath and not dir_file_list:
        savepath_normalized = re.sub(r'/+', '/', savepath)
        logger.debug(f"[preview_regex] 补充获取目标目录文件列表：{savepath_normalized}")
        try:
            drive_type_key = getattr(preview_account, 'DRIVE_TYPE', 'quark')
            cookie_key = getattr(preview_account, 'cookie', '')

            fid = preview_account.savepath_fid.get(savepath_normalized)
            if not fid:
                fids_cache_key = make_cache_key(drive_type_key, cookie_key, 'fids', savepath_normalized)
                fid = get_cached_fids(fids_cache_key)
            if not fid:
                get_fids_result = preview_account.get_fids([savepath_normalized])
                if get_fids_result:
                    fid = get_fids_result[0]["fid"]
                    fids_cache_key = make_cache_key(drive_type_key, cookie_key, 'fids', savepath_normalized)
                    set_cached_fids(fids_cache_key, fid)

            if fid:
                lsdir_cache_key = make_cache_key(drive_type_key, cookie_key, 'lsdir', str(fid))
                cached_ls = None if drive_type_key == "123pan" else get_cached_lsdir(lsdir_cache_key)
                if cached_ls is not None:
                    dir_file_list = cached_ls
                else:
                    ls_result = preview_account.ls_dir(fid, max_items=2000)
                    if ls_result and "data" in ls_result:
                        dir_file_list = ls_result["data"].get("list", [])
                        if drive_type_key != "123pan":
                            set_cached_lsdir(lsdir_cache_key, dir_file_list)
        except Exception as e:
            logger.warning(f"[preview_regex] 获取目标目录失败：{e}")

    dir_filename_list = [f["file_name"] for f in dir_file_list]
    logger.debug(f"[preview_regex] 目标目录中有 {len(dir_file_list)} 个文件")

    pattern, replace = mr.magic_regex_conv(
        task.get("pattern", ""), task.get("replace", "")
    )
    logger.debug(f"[preview_regex] 正则表达式：pattern={pattern}, replace={replace}")
    
    # 预编译搜索正则，避免循环内重复编译
    compiled_search = re.compile(pattern) if pattern else None
    compiled_subdir = re.compile(task["update_subdir"]) if task.get("update_subdir") else None
    startfid = str(task.get("startfid", "")).strip()
    ignore_ext = task.get("ignore_extension")
    start_ts = None
    fid_keep = None
    if startfid:
        def _to_ts(v):
            try:
                return float(v)
            except Exception:
                return None

        _start_item = next(
            (
                f
                for f in data["list"]
                if str(f.get("fid", "")).strip() == startfid
            ),
            None,
        )
        if _start_item:
            start_ts = _to_ts(_start_item.get("updated_at"))
            if start_ts is None:
                _sorted = sorted(
                    data["list"],
                    key=lambda x: _to_ts(x.get("updated_at")) or 0,
                    reverse=True,
                )
                _kept = []
                for f in _sorted:
                    if str(f.get("fid", "")).strip() == startfid:
                        break
                    _kept.append(str(f.get("fid", "")).strip())
                fid_keep = set(_kept)

    for share_file in data["list"]:
        if startfid:
            if start_ts is not None:
                if (_to_ts(share_file.get("updated_at")) or 0) <= start_ts:
                    share_file["file_name_saved"] = "起始及之前"
                    continue
            elif fid_keep is not None:
                if str(share_file.get("fid", "")).strip() not in fid_keep:
                    share_file["file_name_saved"] = "起始及之前"
                    continue
        search_re = (
            compiled_subdir
            if share_file["dir"] and compiled_subdir
            else compiled_search
        )
        if (not search_re) or search_re.search(share_file["file_name"]):
            # 文件名重命名，目录不重命名
            file_name_re = (
                share_file["file_name"]
                if share_file["dir"]
                else mr.sub(pattern, replace, share_file["file_name"])
            )
            if file_name_saved := mr.is_exists(
                file_name_re,
                dir_filename_list,
                (ignore_ext and not share_file["dir"]),
            ):
                share_file["file_name_saved"] = file_name_saved
            else:
                share_file["file_name_re"] = file_name_re
    
    _best = {}
    for _idx, _f in enumerate(data["list"]):
        if _f.get("file_name_saved"):
            continue
        if _f.get("dir"):
            continue
        _target = _f.get("file_name_re")
        if not _target:
            continue
        _key = os.path.splitext(_target)[0] if (ignore_ext and not _f.get("dir")) else _target
        try:
            _ts = float(_f.get("updated_at"))
        except Exception:
            _ts = float("-inf")
        _prev = _best.get(_key)
        if (_prev is None) or (_ts > _prev[0]) or (_ts == _prev[0] and _idx > _prev[1]):
            _best[_key] = (_ts, _idx)
    if _best:
        _keep_idx = set(v[1] for v in _best.values())
        for _idx, _f in enumerate(data["list"]):
            if _idx in _keep_idx:
                continue
            if _f.get("file_name_saved"):
                continue
            if _f.get("dir"):
                continue
            if _f.get("file_name_re"):
                _f["file_name_saved"] = "重命名冲突（保留最新）"
                _f.pop("file_name_re", None)

    # 文件列表排序
    if re.search(r"\{I+\}", replace):
        start_index = task.get("sort_index", 1)
        if not start_index or start_index == "":
            start_index = 1
        else:
            try:
                start_index = int(start_index)
            except (ValueError, TypeError):
                start_index = 1
        logger.debug(f"[preview_regex] 检测到排序变量 {{I+}}，使用排序基数：start_index={start_index}")
        mr.set_dir_file_list(dir_file_list, replace, start_index)
        mr.sort_file_list(data["list"], start_index=start_index)
        logger.debug(f"[preview_regex] 排序完成，应用 sort_index={start_index}")
    else:
        logger.debug(f"[preview_regex] 未检测到排序变量，跳过排序处理")

@app.route("/get_savepath_detail")
def get_savepath_detail():
    if not is_login():
        return jsonify({"success": False, "message": "未登录"})
    
    try:
        # 支持通过参数指定账户
        account_name = request.args.get("account_name", "")
        account, drive_type = get_account_by_name(account_name)
        
        if not account:
            return jsonify({"success": False, "data": {"error": "未配置有效的网盘账户，请先在系统配置中添加Cookie或多网盘账户"}})

        paths = []
        drive_type_key = getattr(account, 'DRIVE_TYPE', 'quark')
        cookie_key = getattr(account, 'cookie', '')
        use_cache = drive_type_key not in ("123pan", "cloud189")

        if path := request.args.get("path"):
            path = re.sub(r"/+", "/", path)
            if path == "/":
                fid = 0
            else:
                dir_names = path.split("/")
                if dir_names[0] == "":
                    dir_names.pop(0)

                # 尝试从 adapter 的 savepath_fid 获取最终目录 fid
                full_path = "/" + "/".join(dir_names)
                fid = account.savepath_fid.get(full_path)

                if fid:
                    # savepath_fid 命中，但仍需构建 paths 面包屑
                    # 无法从缓存重建完整面包屑，走正常流程获取
                    path_fids = []
                    current_path = ""
                    for dir_name in dir_names:
                        current_path += "/" + dir_name
                        path_fids.append(current_path)
                    get_fids = account.get_fids(path_fids)
                    if get_fids:
                        fid = get_fids[-1]["fid"]
                        paths = [
                            {"fid": get_fid["fid"], "name": dir_name}
                            for get_fid, dir_name in zip(get_fids, dir_names)
                        ]
                else:
                    # 尝试应用级缓存
                    fids_cache_key = make_cache_key(drive_type_key, cookie_key, 'fids', full_path)
                    cached_fid = get_cached_fids(fids_cache_key) if use_cache else None

                    if cached_fid:
                        fid = cached_fid
                        # 同样需要面包屑，走 get_fids
                        path_fids = []
                        current_path = ""
                        for dir_name in dir_names:
                            current_path += "/" + dir_name
                            path_fids.append(current_path)
                        get_fids = account.get_fids(path_fids)
                        if get_fids:
                            fid = get_fids[-1]["fid"]
                            paths = [
                                {"fid": get_fid["fid"], "name": dir_name}
                                for get_fid, dir_name in zip(get_fids, dir_names)
                            ]
                    else:
                        # API 请求
                        path_fids = []
                        current_path = ""
                        for dir_name in dir_names:
                            current_path += "/" + dir_name
                            path_fids.append(current_path)
                        get_fids = account.get_fids(path_fids)

                        if get_fids:
                            fid = get_fids[-1]["fid"]
                            if use_cache:
                                set_cached_fids(fids_cache_key, fid)
                            paths = [
                                {"fid": get_fid["fid"], "name": dir_name}
                                for get_fid, dir_name in zip(get_fids, dir_names)
                            ]
                        else:
                            return jsonify({"success": False, "data": {"error": "获取fid失败，请检查路径是否存在"}})
        else:
            fid = request.args.get("fid", "0")
            logging.info(f">>> get_savepath_detail fid={repr(fid)}, drive_type={drive_type}")
            # 如果通过 fid 访问子目录，尝试获取路径信息（面包屑导航）
            if fid and str(fid) != "0" and str(fid) != "root":
                # 尝试获取路径信息（如果适配器支持）
                if hasattr(account, 'get_file_path'):
                    try:
                        paths = account.get_file_path(fid)
                    except Exception as e:
                        logging.debug(f">>> 获取文件路径失败: {e}")
                        paths = []

        # ls_dir 结果走应用级缓存
        lsdir_cache_key = make_cache_key(drive_type_key, cookie_key, 'lsdir', str(fid))
        cached_ls = get_cached_lsdir(lsdir_cache_key) if use_cache else None
        if cached_ls is not None:
            file_list_data = cached_ls
        else:
            ls_result = account.ls_dir(fid)
            if not ls_result or ls_result.get("code") not in (0, "0") or "data" not in ls_result:
                err = ""
                try:
                    err = (ls_result or {}).get("message") or ""
                except Exception:
                    err = ""
                msg = err or "获取目录列表失败，请检查Cookie是否有效"
                return jsonify({"success": False, "data": {"error": msg}})
            file_list_data = ls_result["data"].get("list", [])
            if use_cache:
                set_cached_lsdir(lsdir_cache_key, file_list_data)

        file_list = {
            "list": file_list_data,
            "paths": paths,
            "drive_type": drive_type,  # 返回网盘类型
        }
        return jsonify({"success": True, "data": file_list})
    except Exception as e:
        logging.error(f">>> get_savepath_detail 错误: {str(e)}")
        return jsonify({"success": False, "data": {"error": f"获取目录失败: {str(e)}"}})


@app.route("/delete_file", methods=["POST"])
def delete_file():
    if not is_login():
        return jsonify({"code": 1, "message": "未登录"})
    if not MULTI_DRIVE_SUPPORT:
        return jsonify({"code": 1, "message": "多网盘支持未启用"})
    
    # 支持通过参数指定账户
    account_name = request.json.get("account_name", "")
    drive_type = request.json.get("drive_type", "")
    with _config_lock:
        _reload_config_data()

    account = None
    if account_name and account_name != "auto":
        account, _ = get_account_by_name(account_name)
    if not account and drive_type:
        ck = _get_default_account_cookie(drive_type)
        if ck:
            try:
                account = AdapterFactory.create_adapter(drive_type, ck, 0)
            except Exception:
                account = None
    
    if not account:
        return jsonify({"code": 1, "message": "未配置有效的网盘账户"})
    
    if fid := request.json.get("fid"):
        response = account.delete([fid])
    else:
        response = {"code": 1, "message": "缺失必要字段: fid"}
    return jsonify(response)


# ==================== 阿里云盘二维码登录 ====================

@app.route("/aliyun/qrcode/generate", methods=["GET"])
def aliyun_qrcode_generate():
    """生成阿里云盘登录二维码"""
    if not is_login():
        return jsonify({"success": False, "message": "未登录"})
    
    if not MULTI_DRIVE_SUPPORT:
        return jsonify({"success": False, "message": "多网盘支持未启用"})
    
    try:
        result = AliyunAdapter.generate_qrcode()
        return jsonify(result)
    except Exception as e:
        logging.error(f"[Aliyun] 生成二维码失败: {e}")
        return jsonify({"success": False, "message": str(e)})


@app.route("/aliyun/qrcode/query", methods=["GET"])
def aliyun_qrcode_query():
    """查询阿里云盘二维码扫描状态"""
    if not is_login():
        return jsonify({"success": False, "message": "未登录"})
    
    if not MULTI_DRIVE_SUPPORT:
        return jsonify({"success": False, "message": "多网盘支持未启用"})
    
    t = request.args.get("t", "")
    ck = request.args.get("ck", "")
    
    if not t or not ck:
        return jsonify({"success": False, "message": "缺少参数"})
    
    try:
        result = AliyunAdapter.query_qrcode_status(t, ck)
        return jsonify(result)
    except Exception as e:
        logging.error(f"[Aliyun] 查询二维码状态失败: {e}")
        return jsonify({"success": False, "message": str(e)})


@app.route("/aliyun/token/refresh", methods=["POST"])
def aliyun_token_refresh():
    """刷新阿里云盘 token 并获取最新的 refresh_token"""
    global config_data
    if not is_login():
        return jsonify({"success": False, "message": "未登录"})
    
    if not MULTI_DRIVE_SUPPORT:
        return jsonify({"success": False, "message": "多网盘支持未启用"})
    
    account_name = request.json.get("account_name", "")
    
    # 查找对应的账户
    with _config_lock:
        config_data = _reload_config_data()
    accounts = config_data.get("accounts", [])
    target_account = None
    for acc in accounts:
        if acc.get("drive_type") == "aliyun":
            if not account_name or acc.get("name") == account_name:
                target_account = acc
                break
    
    if not target_account:
        return jsonify({"success": False, "message": "未找到阿里云盘账户"})
    
    try:
        # 创建适配器并刷新 token
        adapter = AliyunAdapter(target_account.get("cookie", ""), 0, target_account.get("name", ""))
        result = adapter.init()
        
        if result:
            # 获取新的 refresh_token
            new_token = adapter._refresh_token
            if new_token and new_token != target_account.get("cookie", ""):
                # 更新配置
                target_account["cookie"] = new_token
                with _config_lock:
                    _save_config_to_db(_sanitize_config_data(config_data))
                # 清除适配器缓存
                AdapterFactory.clear_cache()
                invalidate_all()  # 同步清空应用级预览缓存
                
            return jsonify({
                "success": True,
                "data": {
                    "refresh_token": new_token,
                    "user_info": result,
                },
                "message": "Token 刷新成功",
            })
        else:
            return jsonify({"success": False, "message": "Token 刷新失败，请检查 refresh_token 是否有效"})
    except Exception as e:
        logging.error(f"[Aliyun] Token 刷新失败: {e}")
        return jsonify({"success": False, "message": str(e)})


@app.route("/xunlei/token/refresh", methods=["POST"])
def xunlei_token_refresh():
    """刷新迅雷网盘 token 并获取最新的 refresh_token"""
    global config_data
    if not is_login():
        return jsonify({"success": False, "message": "未登录"})

    if not MULTI_DRIVE_SUPPORT:
        return jsonify({"success": False, "message": "多网盘支持未启用"})

    account_name = request.json.get("account_name", "")

    # 查找对应的账户
    with _config_lock:
        config_data = _reload_config_data()
    accounts = config_data.get("accounts", [])
    target_account = None
    for acc in accounts:
        if acc.get("drive_type") == "xunlei":
            if not account_name or acc.get("name") == account_name:
                target_account = acc
                break

    if not target_account:
        return jsonify({"success": False, "message": "未找到迅雷网盘账户"})

    try:
        from adapters.xunlei_adapter import XunleiAdapter
        adapter = XunleiAdapter(target_account.get("cookie", ""), 0, target_account.get("name", ""))
        result = adapter.init()

        if result:
            new_token = adapter._refresh_token
            if new_token and new_token != target_account.get("cookie", ""):
                target_account["cookie"] = new_token
                target_account["_token_updated_at"] = time.time()
                with _config_lock:
                    _save_config_to_db(_sanitize_config_data(config_data))
                AdapterFactory.clear_cache()
                invalidate_all()  # 同步清空应用级预览缓存

            return jsonify({
                "success": True,
                "data": {
                    "refresh_token": new_token,
                    "user_info": result,
                },
                "message": "Token 刷新成功",
            })
        else:
            return jsonify({"success": False, "message": "Token 刷新失败，请检查 refresh_token 是否有效"})
    except Exception as e:
        logging.error(f"[Xunlei] Token 刷新失败: {e}")
        return jsonify({"success": False, "message": str(e)})


@app.route("/cloud189/login/start", methods=["POST"])
def cloud189_login_start():
    global config_data
    if not is_login():
        return jsonify({"success": False, "message": "未登录"})
    if not MULTI_DRIVE_SUPPORT:
        return jsonify({"success": False, "message": "多网盘支持未启用"})

    account_name = (request.json or {}).get("account_name", "")

    with _cloud189_login_sessions_lock:
        now = time.time()
        expired = [k for k, v in _cloud189_login_sessions.items() if v.get("expires_at", 0) <= now]
        for k in expired:
            _cloud189_login_sessions.pop(k, None)

    with _config_lock:
        config_data = _reload_config_data()
    accounts = config_data.get("accounts", []) if isinstance(config_data, dict) else []
    target_account = None
    for acc in accounts:
        if not isinstance(acc, dict):
            continue
        if acc.get("drive_type") != "cloud189":
            continue
        if account_name and acc.get("name") != account_name:
            continue
        target_account = acc
        break
    if not target_account:
        return jsonify({"success": False, "message": "未找到天翼云盘账户"})

    try:
        from adapters.cloud189_adapter import Cloud189Adapter, Cloud189CaptchaRequired, Cloud189SecondValidRequired
        adapter = Cloud189Adapter(target_account.get("cookie", "") or "", 0)
        if not adapter._user_name or not adapter._password:
            return jsonify({"success": False, "message": "请先在 cookie 参数串中填写 username 与 password"})

        try:
            adapter._login_by_username_password(adapter._user_name, adapter._password, "")
        except Cloud189CaptchaRequired as ce:
            login_session_id = uuid.uuid4().hex
            img_b64 = base64.b64encode(ce.image_bytes or b"").decode("utf-8")
            with _cloud189_login_sessions_lock:
                _cloud189_login_sessions[login_session_id] = {
                    "expires_at": time.time() + _cloud189_login_session_ttl,
                    "account_name": target_account.get("name", ""),
                    "cookie": target_account.get("cookie", "") or "",
                    "username": adapter._user_name,
                    "password": adapter._password,
                    "login_params": dict(ce.context or {}),
                    "session": adapter._session,
                }
            return jsonify(
                {
                    "success": False,
                    "require_captcha": True,
                    "data": {"login_session_id": login_session_id, "captcha_image_base64": img_b64},
                    "message": "需要验证码",
                }
            )
        except Cloud189SecondValidRequired as se:
            login_session_id = uuid.uuid4().hex
            ctx = se.context or {}
            with _cloud189_login_sessions_lock:
                _cloud189_login_sessions[login_session_id] = {
                    "expires_at": time.time() + _cloud189_login_session_ttl,
                    "account_name": target_account.get("name", ""),
                    "cookie": target_account.get("cookie", "") or "",
                    "username": adapter._user_name,
                    "password": adapter._password,
                    "login_params": dict(ctx),
                    "apToken": ctx.get("apToken") or ctx.get("ap_token") or "",
                    "mobile": ctx.get("mobile") or "",
                    "second_mode": ctx.get("second_mode") or "",
                    "session": adapter._session,
                }
            return jsonify(
                {
                    "success": False,
                    "require_second_valid": True,
                    "data": {
                        "login_session_id": login_session_id,
                        "show_name": ctx.get("showName") or "",
                        "is_system": ctx.get("isSystem"),
                        "mode": ctx.get("second_mode") or "",
                    },
                    "message": str(se),
                }
            )
            cookies = _cloud189_finalize_login_and_get_cookies(adapter._session, to_url)
        ok, reason = _cloud189_check_session_detail(adapter._session)
        if not ok:
                return jsonify({"success": False, "message": f"二次校验后登录态无效({reason})，请重试。"})
        sson = cookies.get("SSON") or ""
        if not sson:
            return jsonify({"success": False, "message": "登录成功但未获取到 SSON"})
        adapter._cookie_kv["ssoncookie"] = sson
        adapter._cookie_kv["SSON"] = sson
        for k in ("OPENINFO", "DEVICEID", "GUID", "LT", "JSESSIONID", "pageOp", "GRAYNUMBER"):
            v = cookies.get(k) or ""
            if v:
                adapter._cookie_kv[k] = v
        try:
            adapter._cookie_kv["cookiejar"] = adapter._export_cookiejar_b64()
        except Exception:
            pass
        new_cookie = adapter._cookie_kv_to_str(adapter._cookie_kv)

        with _config_lock:
            cfg = _reload_config_data()
            accounts2 = cfg.get("accounts", []) if isinstance(cfg, dict) else []
            for acc in accounts2:
                if not isinstance(acc, dict):
                    continue
                if acc.get("drive_type") != "cloud189":
                    continue
                if acc.get("name") != target_account.get("name"):
                    continue
                acc["cookie"] = new_cookie
                acc["_token_updated_at"] = time.time()
                break
            cfg["accounts"] = accounts2
            cfg = _sanitize_config_data(cfg)
            _save_config_to_db(cfg)
            config_data = cfg

        AdapterFactory.clear_cache()
        invalidate_all()
        return jsonify({"success": True, "message": "登录成功", "data": {"ssoncookie": sson}})
    except Exception as e:
        logging.error(f"[cloud189] login start failed: {e}")
        return jsonify({"success": False, "message": str(e)})


@app.route("/cloud189/second_valid/submit_password", methods=["POST"])
def cloud189_second_valid_submit_password():
    global config_data
    if not is_login():
        return jsonify({"success": False, "message": "未登录"})
    if not MULTI_DRIVE_SUPPORT:
        return jsonify({"success": False, "message": "多网盘支持未启用"})

    body = request.json or {}
    login_session_id = (body.get("login_session_id") or "").strip()
    password = (body.get("password") or "").strip()
    if not login_session_id or not password:
        return jsonify({"success": False, "message": "缺少参数"})

    with _cloud189_login_sessions_lock:
        sess = _cloud189_login_sessions.get(login_session_id)
        if not sess:
            return jsonify({"success": False, "message": "二次校验会话已失效，请重新登录"})
        if sess.get("expires_at", 0) <= time.time():
            _cloud189_login_sessions.pop(login_session_id, None)
            return jsonify({"success": False, "message": "二次校验会话已过期，请重新登录"})

    try:
        from adapters.cloud189_adapter import Cloud189Adapter
        adapter = Cloud189Adapter(sess.get("cookie", "") or "", 0)
        if sess.get("session"):
            adapter._session = sess.get("session")
        login_params = sess.get("login_params", {}) or {}
        ap_token = (sess.get("apToken") or "").strip()
        username = sess.get("username", "") or ""

        j = adapter._second_valid_submit_password(login_params, ap_token, username, password)
        if isinstance(j, dict) and int(j.get("result", 1)) == 0:
            to_url = j.get("toUrl") or ""
            cookies = _cloud189_finalize_login_and_get_cookies(adapter._session, to_url)
            if not isinstance(cookies, dict) or not cookies:
                return jsonify({"success": False, "message": "二次校验成功但未获取到 Cookie"})
            sson = cookies.get("SSON") or ""
            if sson:
                adapter._cookie_kv["ssoncookie"] = sson
                adapter._cookie_kv["SSON"] = sson
            for k, v in cookies.items():
                if k and v:
                    adapter._cookie_kv[str(k)] = str(v)
            try:
                adapter._cookie_kv["cookiejar"] = adapter._export_cookiejar_b64()
            except Exception:
                pass
            new_cookie = adapter._cookie_kv_to_str(adapter._cookie_kv)

            with _config_lock:
                cfg = _reload_config_data()
                accounts = cfg.get("accounts", []) if isinstance(cfg, dict) else []
                for acc in accounts:
                    if not isinstance(acc, dict):
                        continue
                    if acc.get("drive_type") != "cloud189":
                        continue
                    if acc.get("name") != (sess.get("account_name") or ""):
                        continue
                    acc["cookie"] = new_cookie
                    acc["_token_updated_at"] = time.time()
                    break
                cfg["accounts"] = accounts
                cfg = _sanitize_config_data(cfg)
                _save_config_to_db(cfg)
                config_data = cfg

            with _cloud189_login_sessions_lock:
                _cloud189_login_sessions.pop(login_session_id, None)

            AdapterFactory.clear_cache()
            invalidate_all()
            return jsonify({"success": True, "message": "二次校验成功", "data": {"ssoncookie": sson}})

        msg = (j or {}).get("msg") if isinstance(j, dict) else ""
        return jsonify({"success": False, "message": msg or "二次校验失败"})
    except Exception as e:
        logging.error(f"[cloud189] second valid failed: {e}")
        return jsonify({"success": False, "message": str(e)})


@app.route("/cloud189/second_valid/send_sms", methods=["POST"])
def cloud189_second_valid_send_sms():
    if not is_login():
        return jsonify({"success": False, "message": "未登录"})
    if not MULTI_DRIVE_SUPPORT:
        return jsonify({"success": False, "message": "多网盘支持未启用"})

    body = request.json or {}
    login_session_id = (body.get("login_session_id") or "").strip()
    if not login_session_id:
        return jsonify({"success": False, "message": "缺少参数"})

    with _cloud189_login_sessions_lock:
        sess = _cloud189_login_sessions.get(login_session_id)
        if not sess:
            return jsonify({"success": False, "message": "二次校验会话已失效，请重新登录"})
        if sess.get("expires_at", 0) <= time.time():
            _cloud189_login_sessions.pop(login_session_id, None)
            return jsonify({"success": False, "message": "二次校验会话已过期，请重新登录"})

    try:
        from adapters.cloud189_adapter import Cloud189Adapter
        adapter = Cloud189Adapter(sess.get("cookie", "") or "", 0)
        if sess.get("session"):
            adapter._session = sess.get("session")
        mobile = (sess.get("mobile") or "").strip()
        if not mobile:
            return jsonify({"success": False, "message": "未获取到手机号，无法发送验证码"})
        j = adapter._second_valid_send_sms(sess.get("login_params", {}) or {}, mobile)
        if isinstance(j, dict) and int(j.get("result", 1)) == 0:
            return jsonify({"success": True, "message": "验证码已发送"})
        msg = (j or {}).get("msg") if isinstance(j, dict) else ""
        return jsonify({"success": False, "message": msg or "发送验证码失败"})
    except Exception as e:
        logging.error(f"[cloud189] second valid send sms failed: {e}")
        return jsonify({"success": False, "message": str(e)})


@app.route("/cloud189/second_valid/submit_sms", methods=["POST"])
def cloud189_second_valid_submit_sms():
    global config_data
    if not is_login():
        return jsonify({"success": False, "message": "未登录"})
    if not MULTI_DRIVE_SUPPORT:
        return jsonify({"success": False, "message": "多网盘支持未启用"})

    body = request.json or {}
    login_session_id = (body.get("login_session_id") or "").strip()
    sms_code = (body.get("sms_code") or "").strip()
    if not login_session_id or not sms_code:
        return jsonify({"success": False, "message": "缺少参数"})

    with _cloud189_login_sessions_lock:
        sess = _cloud189_login_sessions.get(login_session_id)
        if not sess:
            return jsonify({"success": False, "message": "二次校验会话已失效，请重新登录"})
        if sess.get("expires_at", 0) <= time.time():
            _cloud189_login_sessions.pop(login_session_id, None)
            return jsonify({"success": False, "message": "二次校验会话已过期，请重新登录"})

    try:
        from adapters.cloud189_adapter import Cloud189Adapter
        adapter = Cloud189Adapter(sess.get("cookie", "") or "", 0)
        if sess.get("session"):
            adapter._session = sess.get("session")
        login_params = sess.get("login_params", {}) or {}
        username = sess.get("username", "") or ""
        mobile = (sess.get("mobile") or "").strip()
        if not mobile:
            return jsonify({"success": False, "message": "未获取到手机号，无法提交验证码"})
        j = adapter._second_valid_submit_sms(login_params, mobile, username, sms_code)
        if isinstance(j, dict) and int(j.get("result", 1)) == 0:
            to_url = j.get("toUrl") or ""
            cookies = _cloud189_finalize_login_and_get_cookies(adapter._session, to_url)
            if not isinstance(cookies, dict) or not cookies:
                return jsonify({"success": False, "message": "二次校验成功但未获取到 Cookie"})
            sson = cookies.get("SSON") or ""
            if sson:
                adapter._cookie_kv["ssoncookie"] = sson
                adapter._cookie_kv["SSON"] = sson
            for k, v in cookies.items():
                if k and v:
                    adapter._cookie_kv[str(k)] = str(v)
            try:
                adapter._cookie_kv["cookiejar"] = adapter._export_cookiejar_b64()
            except Exception:
                pass
            new_cookie = adapter._cookie_kv_to_str(adapter._cookie_kv)

            with _config_lock:
                cfg = _reload_config_data()
                accounts = cfg.get("accounts", []) if isinstance(cfg, dict) else []
                for acc in accounts:
                    if not isinstance(acc, dict):
                        continue
                    if acc.get("drive_type") != "cloud189":
                        continue
                    if acc.get("name") != (sess.get("account_name") or ""):
                        continue
                    acc["cookie"] = new_cookie
                    acc["_token_updated_at"] = time.time()
                    break
                cfg["accounts"] = accounts
                cfg = _sanitize_config_data(cfg)
                _save_config_to_db(cfg)
                config_data = cfg

            with _cloud189_login_sessions_lock:
                _cloud189_login_sessions.pop(login_session_id, None)

            AdapterFactory.clear_cache()
            invalidate_all()
            return jsonify({"success": True, "message": "二次校验成功", "data": {"ssoncookie": sson}})

        msg = (j or {}).get("msg") if isinstance(j, dict) else ""
        return jsonify({"success": False, "message": msg or "二次校验失败"})
    except Exception as e:
        logging.error(f"[cloud189] second valid submit sms failed: {e}")
        return jsonify({"success": False, "message": str(e)})


@app.route("/cloud189/login/submit", methods=["POST"])
def cloud189_login_submit():
    global config_data
    if not is_login():
        return jsonify({"success": False, "message": "未登录"})
    if not MULTI_DRIVE_SUPPORT:
        return jsonify({"success": False, "message": "多网盘支持未启用"})

    body = request.json or {}
    login_session_id = (body.get("login_session_id") or "").strip()
    captcha_code = (body.get("captcha_code") or "").strip()
    if not login_session_id or not captcha_code:
        return jsonify({"success": False, "message": "缺少参数"})

    with _cloud189_login_sessions_lock:
        sess = _cloud189_login_sessions.get(login_session_id)
        if not sess:
            return jsonify({"success": False, "message": "验证码会话已失效，请重新获取"})
        if sess.get("expires_at", 0) <= time.time():
            _cloud189_login_sessions.pop(login_session_id, None)
            return jsonify({"success": False, "message": "验证码会话已过期，请重新获取"})

    try:
        from adapters.cloud189_adapter import Cloud189Adapter
        adapter = Cloud189Adapter(sess.get("cookie", "") or "", 0)
        if sess.get("session"):
            adapter._session = sess.get("session")
        j = adapter._login_submit(
            sess.get("username", "") or "",
            sess.get("password", "") or "",
            captcha_code,
            sess.get("login_params", {}) or {},
        )
        msg = j.get("msg") or ""
        if msg != "登录成功":
            return jsonify({"success": False, "message": msg or "登录失败"})
        to_url = j.get("toUrl") or ""
        cookies = _cloud189_finalize_login_and_get_cookies(adapter._session, to_url)
        ok, reason = _cloud189_check_session_detail(adapter._session)
        if not ok:
            return jsonify({"success": False, "message": f"登录态无效({reason})，请重试。"})
        sson = cookies.get("SSON") or ""
        if not sson:
            return jsonify({"success": False, "message": "登录成功但未获取到 SSON"})
        adapter._cookie_kv["ssoncookie"] = sson
        adapter._cookie_kv["SSON"] = sson
        for k in ("OPENINFO", "DEVICEID", "GUID", "LT", "JSESSIONID", "pageOp", "GRAYNUMBER"):
            v = cookies.get(k) or ""
            if v:
                adapter._cookie_kv[k] = v
        try:
            adapter._cookie_kv["cookiejar"] = adapter._export_cookiejar_b64()
        except Exception:
            pass
        new_cookie = adapter._cookie_kv_to_str(adapter._cookie_kv)

        with _config_lock:
            cfg = _reload_config_data()
            accounts = cfg.get("accounts", []) if isinstance(cfg, dict) else []
            for acc in accounts:
                if not isinstance(acc, dict):
                    continue
                if acc.get("drive_type") != "cloud189":
                    continue
                if acc.get("name") != (sess.get("account_name") or ""):
                    continue
                acc["cookie"] = new_cookie
                acc["_token_updated_at"] = time.time()
                break
            cfg["accounts"] = accounts
            cfg = _sanitize_config_data(cfg)
            _save_config_to_db(cfg)
            config_data = cfg

        with _cloud189_login_sessions_lock:
            _cloud189_login_sessions.pop(login_session_id, None)

        AdapterFactory.clear_cache()
        invalidate_all()
        return jsonify({"success": True, "message": "登录成功", "data": {"ssoncookie": sson}})
    except Exception as e:
        logging.error(f"[cloud189] login submit failed: {e}")
        return jsonify({"success": False, "message": str(e)})


@app.route("/account/update_token", methods=["POST"])
def update_account_token():
    """手动更新阿里云盘/迅雷网盘的 Token"""
    global config_data
    if not is_login():
        return jsonify({"success": False, "message": "未登录"})

    if not MULTI_DRIVE_SUPPORT:
        return jsonify({"success": False, "message": "多网盘支持未启用"})

    account_name = request.json.get("account_name", "")
    drive_type = request.json.get("drive_type", "")
    new_token = request.json.get("new_token", "").strip()

    if not new_token:
        return jsonify({"success": False, "message": "Token 不能为空"})

    if drive_type not in ("aliyun", "xunlei"):
        return jsonify({"success": False, "message": "仅支持阿里云盘和迅雷网盘的 Token 更新"})

    with _config_lock:
        config_data = _reload_config_data()
    accounts = config_data.get("accounts", [])
    target_account = None
    for acc in accounts:
        if acc.get("drive_type") == drive_type:
            if not account_name or acc.get("name") == account_name:
                target_account = acc
                break

    if not target_account:
        return jsonify({"success": False, "message": f"未找到对应的{drive_type}账户"})

    try:
        target_account["cookie"] = new_token
        target_account["_token_updated_at"] = time.time()
        with _config_lock:
            _save_config_to_db(_sanitize_config_data(config_data))
        # 清除适配器缓存
        AdapterFactory.clear_cache()
        invalidate_all()  # 同步清空应用级预览缓存
        logging.info(f"[{drive_type}] 账户 {account_name} 的 Token 已手动更新")
        return jsonify({"success": True, "message": "Token 更新成功"})
    except Exception as e:
        logging.error(f"[{drive_type}] 手动更新 Token 失败: {e}")
        return jsonify({"success": False, "message": str(e)})


# 添加任务接口
@app.route("/api/add_task", methods=["POST"])
def add_task():
    global config_data
    # 验证token
    if not is_login():
        return jsonify({"success": False, "code": 1, "message": "未登录"}), 401
    # 必选字段
    request_data = request.json
    required_fields = ["taskname", "shareurl", "savepath"]
    for field in required_fields:
        if field not in request_data or not request_data[field]:
            return (
                jsonify(
                    {"success": False, "code": 2, "message": f"缺少必要字段: {field}"}
                ),
                400,
            )
    if not request_data.get("addition"):
        request_data["addition"] = task_plugins_config_default
    # 添加任务
    with _config_lock:
        config_data = _reload_config_data()
        config_data.setdefault("tasklist", []).append(request_data)
        _save_config_to_db(_sanitize_config_data(config_data))
    logging.info(f">>> 通过API添加任务: {request_data['taskname']}")
    return jsonify(
        {"success": True, "code": 0, "message": "任务添加成功", "data": request_data}
    )



# ==================== 数据同步 API ====================

@app.route("/api/sync/tasks", methods=["GET"])
def get_sync_tasks():
    """获取同步任务配置和状态"""
    if not is_login():
        return jsonify({"success": False, "message": "未登录"})
    try:
        sync_tasks = config_data.get("sync_tasks", [])
        task_status = sync_db.get_all_task_status(include_data=False) if sync_db else {}
        return jsonify({
            "success": True,
            "data": {
                "sync_tasks": sync_tasks,
                "task_status": task_status,
            }
        })
    except Exception as e:
        logging.error(f">>> 获取同步任务失败: {e}")
        return jsonify({"success": False, "message": str(e)})


@app.route("/api/sync/status", methods=["GET"])
def get_sync_status():
    """仅获取同步任务运行状态（轻量轮询接口，不返回任务配置）"""
    if not is_login():
        return jsonify({"success": False, "message": "未登录"})
    try:
        task_status = sync_db.get_all_task_status(include_data=False) if sync_db else {}
        return jsonify({"success": True, "data": {"task_status": task_status}})
    except Exception as e:
        logging.error(f">>> 获取同步状态失败: {e}")
        return jsonify({"success": False, "message": str(e)})


@app.route("/api/sync/tasks", methods=["POST"])
def save_sync_tasks():
    """保存同步任务配置"""
    if not is_login():
        return jsonify({"success": False, "message": "未登录"})
    try:
        sync_tasks = request.json.get("sync_tasks", [])
        global config_data
        with _config_lock:
            config_data = _reload_config_data()
            config_data["sync_tasks"] = sync_tasks
            _save_config_to_db(_sanitize_config_data(config_data))
        # 重载同步调度
        if sync_manager:
            sync_manager.reload_sync_tasks(sync_tasks)
        logging.info(f">>> 同步任务配置已保存 ({len(sync_tasks)} 个任务)")
        return jsonify({"success": True, "message": "同步任务配置已保存"})
    except Exception as e:
        logging.error(f">>> 保存同步任务失败: {e}")
        return jsonify({"success": False, "message": str(e)})


@app.route("/api/sync/run", methods=["POST"])
def sync_run():
    """立即执行同步任务（SSE 流式日志，支持结构化事件）"""
    if not is_login():
        return jsonify({"success": False, "message": "未登录"})
    if not sync_manager:
        return jsonify({"success": False, "message": "数据同步模块未初始化"})

    task_id = request.json.get("task_id", "")
    # 从配置中查找任务
    sync_tasks = config_data.get("sync_tasks", [])
    task_config = None
    for t in sync_tasks:
        if t.get("task_id") == task_id:
            task_config = t
            break

    if not task_config:
        return jsonify({"success": False, "message": f"未找到任务: {task_id}"})

    task_config = dict(task_config)
    task_config["_trigger"] = "manual"

    log_queue = queue.Queue()
    synced_files_tracker = []
    registered = False

    with _cancel_events_lock:
        status = None
        try:
            status = sync_db.get_task_status(task_id, include_data=False) if sync_db else None
        except Exception:
            status = None
        running = bool(status and status.get("status") == "running")
        existing = _cancel_events.get(task_id)
        if existing and running:
            cancel_event = existing
        else:
            cancel_event = threading.Event()
            _cancel_events[task_id] = cancel_event
            registered = True

    def log_callback(msg):
        log_queue.put(msg)

    def run_in_thread():
        summary = None
        try:
            summary = sync_manager.run_task_now(
                task_config, log_callback=log_callback,
                cancel_event=cancel_event,
                synced_files_tracker=synced_files_tracker,
                structured_log=True,
            )
        except Exception as e:
            log_queue.put(f"任务执行异常: {e}")
        finally:
            # 处理回滚
            cancelled = cancel_event.is_set()
            action = _cancel_actions.pop(task_id, "keep")
            if cancelled and action == "rollback" and synced_files_tracker:
                log_queue.put("[数据同步] 正在回滚已同步文件...")
                rollback_count = 0
                for fpath in synced_files_tracker:
                    try:
                        if os.path.exists(fpath):
                            os.remove(fpath)
                            rollback_count += 1
                    except OSError as e:
                        logging.error(f"回滚删除文件失败: {fpath} -> {e}")
                log_queue.put(f"[数据同步] 已回滚 {rollback_count} 个文件")
                # 清理对应的数据库记录
                if sync_db and rollback_count > 0:
                    try:
                        sync_db.cleanup_records(task_id)
                    except Exception:
                        pass

            if registered:
                with _cancel_events_lock:
                    if _cancel_events.get(task_id) == cancel_event:
                        _cancel_events.pop(task_id, None)

            if cancelled:
                log_queue.put("[CANCELLED]")
            else:
                log_queue.put("[DONE]")

    thread = threading.Thread(target=run_in_thread, daemon=True)
    thread.start()

    def generate_output():
        try:
            while True:
                try:
                    msg = log_queue.get(timeout=TASK_TIMEOUT)
                    if msg == "[DONE]":
                        yield "data: [DONE]\n\n"
                        break
                    elif msg == "[CANCELLED]":
                        yield "data: [CANCELLED]\n\n"
                        break
                    if msg.startswith("{"):
                        try:
                            evt = json.loads(msg)
                            event_type = evt.get("event", "log")
                            data_str = json.dumps(evt.get("data", {}), ensure_ascii=False)
                            yield f"event: {event_type}\ndata: {data_str}\n\n"
                            continue
                        except (json.JSONDecodeError, AttributeError):
                            pass
                    yield f"data: {msg}\n\n"
                except Exception:
                    yield "data: [TIMEOUT]\n\n"
                    break
        except GeneratorExit:
            pass
        finally:
            if registered:
                with _cancel_events_lock:
                    if _cancel_events.get(task_id) == cancel_event:
                        _cancel_events.pop(task_id, None)

    return Response(
        stream_with_context(generate_output()),
        content_type="text/event-stream;charset=utf-8",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )


@app.route("/api/sync/logs", methods=["GET"])
def get_sync_logs():
    if not is_login():
        return jsonify({"success": False, "message": "未登录"})
    return jsonify({
        "success": False,
        "code": "DEPRECATED",
        "message": "已弃用，请使用 /api/sync/log/stream (SSE) 获取结构化日志",
    })


@app.route("/api/sync/log/stream", methods=["GET"])
def stream_sync_log():
    if not is_login():
        return jsonify({"success": False, "message": "未登录"})
    if not sync_db:
        return jsonify({"success": False, "message": "数据同步模块未初始化"})

    task_id = request.args.get("task_id", "")
    if not task_id:
        return jsonify({"success": False, "message": "缺少 task_id 参数"})

    def generate_output():
        offset = 0
        terminal_seen = False
        first = True
        while True:
            status = None
            try:
                status = sync_db.get_task_status(task_id, include_data=True)
            except Exception as e:
                logging.error(f">>> 获取同步日志流失败: {e}")
                status = None

            data = (status or {}).get("data") or ""
            running = bool(status and status.get("status") == "running")

            if first:
                if data:
                    if data.endswith("\n\n"):
                        yield data
                    else:
                        yield data + "\n\n"
                offset = len(data)
                first = False
            else:
                if len(data) < offset:
                    if data:
                        if data.endswith("\n\n"):
                            yield data
                        else:
                            yield data + "\n\n"
                    offset = len(data)
                elif len(data) > offset:
                    yield data[offset:]
                    offset = len(data)

            if "event: task_execute" in data or "event: done" in data:
                terminal_seen = True

            if (not running) and terminal_seen:
                break

            time.sleep(0.25)

        yield "data: [DONE]\n\n"

    return Response(
        stream_with_context(generate_output()),
        content_type="text/event-stream;charset=utf-8",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )


@app.route("/api/sync/replay", methods=["GET"])
def replay_sync_events():
    return stream_sync_log()


@app.route("/api/sync/records", methods=["GET"])
def get_sync_records():
    """查询同步记录（支持分页和树状全量两种模式）"""
    if not is_login():
        return jsonify({"success": False, "message": "未登录"})
    if not sync_db:
        return jsonify({"success": False, "message": "数据同步模块未初始化"})

    task_id = request.args.get("task_id", "")
    view = request.args.get("view", "table")

    try:
        if view == "tree":
            # 树状视图：返回全量记录（按路径排序，带上限）
            records = sync_db.get_all_sync_records(task_id)
            total = sync_db.get_records_count(task_id)
            return jsonify({
                "success": True,
                "data": {
                    "records": records,
                    "total": total,
                }
            })
        else:
            # 列表视图：分页返回
            page = int(request.args.get("page", 1))
            page_size = int(request.args.get("page_size", 20))
            records = sync_db.get_sync_records(task_id, page, page_size)
            total = sync_db.get_records_count(task_id)
            return jsonify({
                "success": True,
                "data": {
                    "records": records,
                    "total": total,
                    "page": page,
                    "page_size": page_size,
                }
            })
    except Exception as e:
        logging.error(f">>> 查询同步记录失败: {e}")
        return jsonify({"success": False, "message": str(e)})


@app.route("/api/sync/records/cleanup", methods=["POST"])
def cleanup_sync_records():
    """清理同步记录"""
    if not is_login():
        return jsonify({"success": False, "message": "未登录"})
    if not sync_db:
        return jsonify({"success": False, "message": "数据同步模块未初始化"})

    task_id = request.json.get("task_id", "")
    before_days = request.json.get("before_days")  # None 表示全部清理

    try:
        deleted = sync_db.cleanup_records(task_id, before_days)
        return jsonify({
            "success": True,
            "message": f"已清理 {deleted} 条记录",
            "data": {"deleted": deleted}
        })
    except Exception as e:
        logging.error(f">>> 清理同步记录失败: {e}")
        return jsonify({"success": False, "message": str(e)})


@app.route("/api/sync/record/delete", methods=["POST"])
def delete_sync_record():
    """删除单条同步记录"""
    if not is_login():
        return jsonify({"success": False, "message": "未登录"})
    if not sync_db:
        return jsonify({"success": False, "message": "数据同步模块未初始化"})

    record_id = request.json.get("record_id")
    if record_id is None:
        return jsonify({"success": False, "message": "缺少 record_id 参数"})

    try:
        ok = sync_db.delete_sync_record(record_id)
        if ok:
            return jsonify({"success": True, "message": "记录已删除"})
        else:
            return jsonify({"success": False, "message": "记录不存在或删除失败"})
    except Exception as e:
        logging.error(f">>> 删除同步记录失败: {e}")
        return jsonify({"success": False, "message": str(e)})


@app.route("/api/sync/browse", methods=["GET"])
def sync_browse():
    """浏览 datafiles 目录树"""
    if not is_login():
        return jsonify({"success": False, "message": "未登录"})

    rel_path = request.args.get("path", "")
    datafiles_real = os.path.realpath(DATAFILES_DIR)

    # 安全校验
    target = os.path.realpath(os.path.join(datafiles_real, rel_path))
    if not target.startswith(datafiles_real):
        return jsonify({"success": False, "message": "路径安全校验失败"})

    if not os.path.isdir(target):
        return jsonify({"success": False, "message": f"目录不存在: {rel_path}"})

    try:
        items = []
        for name in sorted(os.listdir(target)):
            full = os.path.join(target, name)
            try:
                stat = os.stat(full)
                items.append({
                    "name": name,
                    "is_dir": os.path.isdir(full),
                    "size": stat.st_size if not os.path.isdir(full) else 0,
                    "mtime": stat.st_mtime,
                })
            except OSError:
                continue

        # 构建面包屑
        breadcrumbs = []
        if rel_path:
            parts = rel_path.replace("\\", "/").strip("/").split("/")
            for i, part in enumerate(parts):
                breadcrumbs.append({
                    "name": part,
                    "path": "/".join(parts[:i + 1]),
                })

        return jsonify({
            "success": True,
            "data": {
                "list": items,
                "current_path": rel_path,
                "breadcrumbs": breadcrumbs,
            }
        })
    except Exception as e:
        logging.error(f">>> 浏览目录失败: {e}")
        return jsonify({"success": False, "message": str(e)})


@app.route("/api/sync/lock/release", methods=["POST"])
def sync_release_lock():
    """强制释放任务锁"""
    if not is_login():
        return jsonify({"success": False, "message": "未登录"})
    if not sync_db:
        return jsonify({"success": False, "message": "数据同步模块未初始化"})

    task_id = request.json.get("task_id", "")
    try:
        sync_db.force_release_lock(task_id)
        return jsonify({"success": True, "message": "任务锁已释放"})
    except Exception as e:
        logging.error(f">>> 释放任务锁失败: {e}")
        return jsonify({"success": False, "message": str(e)})


@app.route("/api/sync/cancel", methods=["POST"])
def sync_cancel():
    """取消运行中的同步任务"""
    if not is_login():
        return jsonify({"success": False, "message": "未登录"})

    task_id = request.json.get("task_id", "")
    action = request.json.get("action", "keep")  # "keep" 或 "rollback"
    cancelled = []

    with _cancel_events_lock:
        if task_id == "__all__":
            for tid, evt in _cancel_events.items():
                evt.set()
                _cancel_actions[tid] = action
                cancelled.append(tid)
        elif task_id in _cancel_events:
            _cancel_events[task_id].set()
            _cancel_actions[task_id] = action
            cancelled.append(task_id)

    if cancelled:
        try:
            if sync_db:
                for tid in cancelled:
                    sync_db.update_task_status(tid, "stopping")
        except Exception:
            pass
        logging.info(f">>> 已发送取消信号: {cancelled}, action={action}")
        return jsonify({
            "success": True,
            "code": "STOPPING",
            "message": f"正在停止 {len(cancelled)} 个任务",
            "data": {"cancelled": cancelled, "status": "stopping"}
        })
    else:
        return jsonify({"success": False, "code": "NOT_RUNNING", "message": "没有找到运行中的任务"})


@app.route("/api/sync/records/batch-delete", methods=["POST"])
def batch_delete_sync_records():
    """批量删除同步记录"""
    if not is_login():
        return jsonify({"success": False, "message": "未登录"})
    if not sync_db:
        return jsonify({"success": False, "message": "数据同步模块未初始化"})

    record_ids = request.json.get("record_ids", [])
    if not record_ids or not isinstance(record_ids, list):
        return jsonify({"success": False, "message": "缺少有效的 record_ids 参数"})
    if len(record_ids) > 2000:
        return jsonify({"success": False, "message": "单次最多删除 2000 条记录"})

    # 确保所有 ID 为整数
    try:
        record_ids = [int(rid) for rid in record_ids]
    except (ValueError, TypeError):
        return jsonify({"success": False, "message": "record_ids 中包含无效值"})

    try:
        deleted = sync_db.batch_delete_records(record_ids)
        return jsonify({
            "success": True,
            "message": f"已删除 {deleted} 条记录",
            "data": {"deleted": deleted}
        })
    except Exception as e:
        logging.error(f">>> 批量删除同步记录失败: {e}")
        return jsonify({"success": False, "message": str(e)})


def run_python(script_path, config_path):
    logging.info(f">>> 定时运行任务")

    process_env = os.environ.copy()
    process_env["PYTHONIOENCODING"] = "utf-8"
    export_path = None
    try:
        export_path = _export_config_to_tempfile()
    except Exception:
        export_path = None
    command = [PYTHON_PATH, "-u", script_path, export_path or config_path]
    process = None
    try:
        process = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            universal_newlines=True,
            encoding="utf-8",
            errors="replace",
            bufsize=1,
            env=process_env,
        )
        with _run_procs_lock:
            _run_procs["__scheduler__"] = {"type": "scheduler", "process": process}
        start = time.time()
        for line in iter(process.stdout.readline, ""):
            s = line.rstrip("\n")
            if s:
                logging.info(s)
            if time.time() - start > TASK_TIMEOUT:
                break
        try:
            process.wait(timeout=2)
        except Exception:
            pass
        if process.poll() is None:
            try:
                process.terminate()
            except Exception:
                pass
            try:
                process.wait(timeout=5)
            except Exception:
                try:
                    process.kill()
                except Exception:
                    pass
        code = process.returncode if process else -1
        if code == 0:
            logging.info(f">>> 任务执行成功")
        else:
            logging.error(f">>> 任务执行失败，返回码: {code}")
    except Exception as e:
        logging.error(f">>> 任务执行异常: {str(e)}")
        logging.error(traceback.format_exc())
    finally:
        with _run_procs_lock:
            v = _run_procs.get("__scheduler__")
            if v and v.get("process") == process:
                _run_procs.pop("__scheduler__", None)
        if process and process.stdout:
            try:
                process.stdout.close()
            except Exception:
                pass
        if export_path and os.path.exists(export_path):
            try:
                bans = _extract_task_bans_from_config_file(export_path)
                changed = _merge_task_bans_into_main_config(bans)
                if changed > 0:
                    logging.info(f">>> 已同步 {changed} 个任务的 shareurl_ban")
            except Exception:
                pass
            try:
                os.remove(export_path)
            except Exception:
                pass
        logging.debug(f">>> run_python 函数执行完成")


# 重新加载任务
def reload_tasks():
    # 读取定时规则
    if crontab := config_data.get("crontab"):
        if scheduler.state == 1:
            scheduler.pause()  # 暂停调度器
        trigger = CronTrigger.from_crontab(crontab)
        # 仅移除非同步任务的 jobs（保留 sync_ 前缀的调度）
        for job in scheduler.get_jobs():
            if not job.id.startswith("sync_"):
                scheduler.remove_job(job.id)
        scheduler.add_job(
            run_python,
            trigger=trigger,
            args=[SCRIPT_PATH, CONFIG_PATH],
            id=SCRIPT_PATH,
            max_instances=1,  # 最多允许1个实例运行
            coalesce=True,  # 合并错过的任务，避免堆积
            misfire_grace_time=300,  # 错过任务的宽限期(秒)，超过则跳过
            replace_existing=True,  # 替换已存在的同ID任务
        )
        if scheduler.state == 0:
            scheduler.start()
        elif scheduler.state == 2:
            scheduler.resume()
        scheduler_state_map = {0: "停止", 1: "运行", 2: "暂停"}
        logging.info(">>> 重载调度器")
        logging.info(f"调度状态: {scheduler_state_map[scheduler.state]}")
        logging.info(f"定时规则: {crontab}")
        logging.info(f"现有任务: {scheduler.get_jobs()}")
        return True
    else:
        logging.info(">>> no crontab")
        return False


@app.route("/api/scheduler/stop", methods=["POST"])
def stop_scheduler_run():
    if not is_login():
        return jsonify({"success": False, "message": "未登录"})
    with _run_procs_lock:
        info = _run_procs.get("__scheduler__")
        proc = info.get("process") if info else None
    if not proc:
        return jsonify({"success": False, "message": "没有找到运行中的任务"})
    try:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except Exception:
            proc.kill()
        return jsonify({"success": True, "message": "已发送停止信号"})
    except Exception as e:
        return jsonify({"success": False, "message": str(e)})


def init():
    global config_data, task_plugins_config_default, sync_db, sync_manager
    logging.info(">>> 初始化配置")
    config_dir = os.path.dirname(os.path.realpath(CONFIG_PATH))
    try:
        os.makedirs(config_dir, exist_ok=True)
    except Exception:
        pass

    try:
        from sync import SyncDB, SyncSchedulerManager
        db_path = os.path.join(config_dir, "sync_records.db")
        sync_db = SyncDB(db_path)
        sync_db.cleanup_stale_locks()
    except Exception as e:
        sync_db = None
        sync_manager = None
        logging.warning(f">>> 初始化数据同步模块失败: {e}")

    with _config_lock:
        config_data = _load_config_from_db()
        if config_data is None:
            if os.path.exists(CONFIG_PATH):
                config_data = Config.read_json(CONFIG_PATH)
            elif os.path.exists("quark_config.json"):
                config_data = Config.read_json("quark_config.json")
            else:
                config_data = {}
            config_data = _sanitize_config_data(config_data)
        Config.breaking_change_update(config_data)
        if not config_data.get("magic_regex"):
            config_data["magic_regex"] = MagicRename().magic_regex

    # 默认管理账号
    config_data["webui"] = {
        "username": os.environ.get("WEBUI_USERNAME")
        or config_data.get("webui", {}).get("username", "admin"),
        "password": os.environ.get("WEBUI_PASSWORD")
        or config_data.get("webui", {}).get("password", "admin123"),
    }

    # 默认定时规则
    if not config_data.get("crontab"):
        config_data["crontab"] = "0 8,18,20 * * *"

    # 初始化插件配置
    _, plugins_config_default, task_plugins_config_default = Config.load_plugins()
    for name, config in plugins_config_default.items():
        for key, value in config.items():
            config[key] = (
                config_data.setdefault("plugins", {})
                .setdefault(name, {})
                .get(key, value)
            )
    config_data["plugins"] = plugins_config_default

    tasklist = config_data.get("tasklist", [])
    if isinstance(tasklist, list) and isinstance(task_plugins_config_default, dict):
        for task in tasklist:
            if not isinstance(task, dict):
                continue
            addition = task.get("addition")
            if not isinstance(addition, dict):
                addition = {}
            for plugin_name, default_task_cfg in task_plugins_config_default.items():
                if plugin_name not in addition or not isinstance(addition.get(plugin_name), dict):
                    addition[plugin_name] = default_task_cfg
                else:
                    for k, v in default_task_cfg.items():
                        addition[plugin_name].setdefault(k, v)
            task["addition"] = addition

    config_data = _sanitize_config_data(config_data)
    with _config_lock:
        _save_config_to_db(config_data)
        config_data = _reload_config_data()
    
    # 初始化阿里云盘 token 保存器
    if MULTI_DRIVE_SUPPORT:
        def _make_token_saver(drive_type):
            def _saver(new_token, account_name=None):
                global config_data
                updated = False
                now = time.time()
                with _config_lock:
                    cfg = _reload_config_data()
                    accounts = cfg.get("accounts", []) if isinstance(cfg, dict) else []
                    for acc in accounts:
                        if not isinstance(acc, dict):
                            continue
                        if acc.get("drive_type") != drive_type:
                            continue
                        if account_name and acc.get("name") != account_name:
                            if drive_type != "123pan":
                                continue
                            try:
                                cookie_str = acc.get("cookie", "") or ""
                                kv = {}
                                for part in cookie_str.split(";"):
                                    part = part.strip()
                                    if not part or "=" not in part:
                                        continue
                                    k, v = part.split("=", 1)
                                    kv[k.strip().lower()] = v.strip()
                                cookie_username = kv.get("username") or kv.get("passport")
                                cookie_name = kv.get("name")
                                if account_name not in (cookie_username, cookie_name):
                                    continue
                            except Exception:
                                continue
                        acc["cookie"] = new_token
                        acc["_token_updated_at"] = now
                        updated = True
                        if account_name:
                            break
                    if updated:
                        cfg["accounts"] = accounts
                        cfg = _sanitize_config_data(cfg)
                        _save_config_to_db(cfg)
                        config_data = cfg
                if updated and MULTI_DRIVE_SUPPORT:
                    AdapterFactory.clear_cache()
                    invalidate_all()
                return updated
            return _saver
        try:
            from adapters.aliyun_adapter import set_config_saver
            set_config_saver(_make_token_saver("aliyun"))
            logging.info(">>> 阿里云盘 token 保存器已初始化")
        except Exception as e:
            logging.warning(f">>> 初始化阿里云盘 token 保存器失败: {e}")

    # 初始化迅雷网盘 token 保存器
    if MULTI_DRIVE_SUPPORT:
        try:
            from adapters.xunlei_adapter import set_config_saver as xunlei_set_config_saver
            xunlei_set_config_saver(_make_token_saver("xunlei"))
            logging.info(">>> 迅雷网盘 token 保存器已初始化")
        except Exception as e:
            logging.warning(f">>> 初始化迅雷网盘 token 保存器失败: {e}")

    if MULTI_DRIVE_SUPPORT:
        try:
            from adapters.pan123_adapter import set_config_saver as pan123_set_config_saver
            pan123_set_config_saver(_make_token_saver("123pan"))
            logging.info(">>> 123网盘 token 保存器已初始化")
        except Exception as e:
            logging.warning(f">>> 初始化123网盘 token 保存器失败: {e}")

    if MULTI_DRIVE_SUPPORT:
        try:
            from adapters.cloud189_adapter import set_config_saver as cloud189_set_config_saver
            cloud189_set_config_saver(_make_token_saver("cloud189"))
            logging.info(">>> 天翼云盘 token 保存器已初始化")
        except Exception as e:
            logging.warning(f">>> 初始化天翼云盘 token 保存器失败: {e}")

    if sync_db:
        try:
            from sync import SyncSchedulerManager
            datafiles_abs = os.path.realpath(DATAFILES_DIR)
            if not os.path.exists(datafiles_abs):
                os.makedirs(datafiles_abs)
            sync_manager = SyncSchedulerManager(
                scheduler=scheduler,
                db=sync_db,
                base_dir=datafiles_abs,
                config_getter=lambda: config_data,
                cancel_events=_cancel_events,
                cancel_events_lock=_cancel_events_lock,
            )
            logging.info(f">>> 数据同步模块已初始化 (datafiles={datafiles_abs})")
        except Exception as e:
            logging.warning(f">>> 初始化数据同步模块失败: {e}")
            import traceback
            traceback.print_exc()


if __name__ == "__main__":
    init()
    reload_tasks()
    # 加载数据同步调度
    if sync_manager:
        try:
            sync_tasks = config_data.get("sync_tasks", [])
            sync_manager.reload_sync_tasks(sync_tasks)
        except Exception as e:
            logging.warning(f">>> 加载数据同步调度失败: {e}")
    logging.info(">>> 启动Web服务")
    logging.info(f"运行在: http://{HOST}:{PORT}")
    app.run(
        debug=DEBUG,
        host=HOST,
        port=PORT,
    )
    
