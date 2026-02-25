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
from datetime import timedelta
import subprocess
import requests
import hashlib
import logging
import traceback
import base64
import sys
import os
import re
import time

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

print(
    r"""
   ____    ___   _____
  / __ \  /   | / ___/
 / / / / / /| | \__ \
/ /_/ / / ___ |___/ /
\___\_\/_/  |_/____/

-- Quark-Auto-Save --
 """
)
sys.stdout.flush()


def get_app_ver():
    """获取应用版本"""
    try:
        with open("build.json", "r") as f:
            build_info = json.loads(f.read())
            BUILD_SHA = build_info["BUILD_SHA"]
            BUILD_TAG = build_info["BUILD_TAG"]
    except Exception as e:
        BUILD_SHA = os.getenv("BUILD_SHA", "")
        BUILD_TAG = os.getenv("BUILD_TAG", "")
    if BUILD_TAG[:1] == "v":
        return BUILD_TAG
    elif BUILD_SHA:
        return f"{BUILD_TAG}({BUILD_SHA[:7]})"
    else:
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

app = Flask(__name__)
app.config["APP_VERSION"] = get_app_ver()
app.secret_key = "ca943f6db6dd34823d36ab08d8d6f65d"
app.config["SESSION_COOKIE_NAME"] = "QUARK_AUTO_SAVE_SESSION"
app.config["PERMANENT_SESSION_LIFETIME"] = timedelta(days=31)
app.json.ensure_ascii = False
app.json.sort_keys = False
app.jinja_env.variable_start_string = "[["
app.jinja_env.variable_end_string = "]]"

scheduler = BackgroundScheduler()
logging.basicConfig(
    level=logging.DEBUG if DEBUG else logging.INFO,
    format="[%(asctime)s][%(levelname)s] %(message)s",
    datefmt="%m-%d %H:%M:%S",
)
# 过滤werkzeug日志输出
if not DEBUG:
    logging.getLogger("werkzeug").setLevel(logging.ERROR)
    logging.getLogger("apscheduler").setLevel(logging.ERROR)
    sys.modules["flask.cli"].show_server_banner = lambda *x: None


def gen_md5(string):
    md5 = hashlib.md5()
    md5.update(string.encode("utf-8"))
    return md5.hexdigest()


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
    支持新格式(accounts数组)和旧格式(cookie数组)的兼容
    
    Args:
        account_name: 账户名称，None 或 'auto' 表示使用默认账户
    
    Returns:
        tuple: (adapter/quark实例, drive_type)
    """
    # 检查是否使用新格式配置
    if MULTI_DRIVE_SUPPORT and config_data.get("accounts"):
        accounts = config_data.get("accounts", [])
        enabled_accounts = [acc for acc in accounts if acc.get("enabled", True)]
        
        if not enabled_accounts:
            # 无可用账户，回退到旧格式
            if config_data.get("cookie"):
                return Quark(config_data["cookie"][0]), "quark"
            return None, None
        
        # 查找指定账户或默认账户
        target_account = None
        if account_name and account_name != "auto":
            for acc in enabled_accounts:
                if acc.get("name") == account_name:
                    target_account = acc
                    break
        
        if not target_account:
            # 使用默认账户或第一个可用账户
            for acc in enabled_accounts:
                if acc.get("is_default") or acc.get("default"):
                    target_account = acc
                    break
            if not target_account:
                target_account = enabled_accounts[0]
        
        # 创建适配器
        drive_type = target_account.get("drive_type", "quark")
        cookie = target_account.get("cookie", "")
        
        # 使用工厂创建适配器
        adapter = AdapterFactory.create_adapter(drive_type, cookie, 0)
        if adapter:
            return adapter, drive_type
        
        # 工厂创建失败，回退到默认
        return QuarkAdapter(cookie), "quark"
    
    # 旧格式兼容
    if config_data.get("cookie"):
        return Quark(config_data["cookie"][0]), "quark"
    
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
        if config_data.get("cookie"):
            return Quark(config_data["cookie"][0]), "quark"
        return None, None
    
    # 根据 URL 判断网盘类型
    drive_type = AdapterFactory.get_drive_type_by_url(shareurl)
    logging.debug(f">>> URL检测: {shareurl[:50]}... -> drive_type={drive_type}")
    
    if not drive_type:
        logging.warning(f">>> 无法识别的分享链接类型: {shareurl}")
        # 尝试回退到旧格式的夸克
        if config_data.get("cookie"):
            return Quark(config_data["cookie"][0]), "quark"
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
    if drive_type == "quark" and config_data.get("cookie"):
        logging.info(f">>> 回退到旧格式Cookie配置")
        return Quark(config_data["cookie"][0]), "quark"
    
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


# 获取配置数据
@app.route("/data")
def get_data():
    if not is_login():
        return jsonify({"success": False, "message": "未登录"})
    data = Config.read_json(CONFIG_PATH)
    del data["webui"]
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
    global config_data,CONFIG_PATH
    if not is_login():
        return jsonify({"success": False, "message": "未登录"})
    config_data = Config.read_json(CONFIG_PATH)

    dont_save_keys = ["task_plugins_config_default", "api_token"]
    
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
    Config.write_json(CONFIG_PATH, config_data)
    # 配置变更时清空适配器实例缓存，确保新配置生效
    if MULTI_DRIVE_SUPPORT:
        AdapterFactory.clear_cache()
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
    command = [PYTHON_PATH, "-u", SCRIPT_PATH, CONFIG_PATH]
    logging.info(
        f">>> 手动运行任务 [{tasklist[0].get('taskname') if len(tasklist)>0 else 'ALL'}] 开始执行..."
    )

    def generate_output():
        # 设置环境变量
        process_env = os.environ.copy()
        process_env["PYTHONIOENCODING"] = "utf-8"
        if request.json.get("quark_test"):
            process_env["QUARK_TEST"] = "true"
            process_env["COOKIE"] = json.dumps(
                request.json.get("cookie", []), ensure_ascii=False
            )
            process_env["PUSH_CONFIG"] = json.dumps(
                request.json.get("push_config", {}), ensure_ascii=False
            )
        if tasklist:
            process_env["TASKLIST"] = json.dumps(tasklist, ensure_ascii=False)
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
        try:
            for line in iter(process.stdout.readline, ""):
                logging.info(line.strip())
                yield f"data: {line}\n\n"
            yield "data: [DONE]\n\n"
        finally:
            process.stdout.close()
            process.wait()

    return Response(
        stream_with_context(generate_output()),
        content_type="text/event-stream;charset=utf-8",
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
                    Config.write_json(CONFIG_PATH, config_data)
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
            features.append(executor.submit(net_search))
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
    if not is_login():
        return jsonify({"success": False, "message": "未登录"})
    
    try:
        shareurl = request.json.get("shareurl", "")
        stoken = request.json.get("stoken", "")
        account_name = request.json.get("account_name", "")
        
        # 根据 URL 或指定账户获取适配器
        if account_name and account_name != "auto":
            account, drive_type = get_account_by_name(account_name)
        else:
            account, drive_type = get_adapter_for_url(shareurl)
        
        if not account:
            # 检测URL类型以提供更详细的错误信息
            detected_type = AdapterFactory.get_drive_type_by_url(shareurl) if MULTI_DRIVE_SUPPORT else "quark"
            type_label = {"quark": "夸克网盘", "115": "115网盘"}.get(detected_type, detected_type)
            return jsonify({"success": False, "data": {"error": f"未配置有效的{type_label}账户，请先在「系统配置」→「多网盘账户」中添加{type_label}账户"}})

        pwd_id, passcode, pdir_fid, paths = account.extract_url(shareurl)
        if not stoken:
            get_stoken = account.get_stoken(pwd_id, passcode)
            if get_stoken.get("status") == 200:
                stoken = get_stoken["data"]["stoken"]
            else:
                return jsonify(
                    {"success": False, "data": {"error": get_stoken.get("message")}}
                )
        share_detail = account.get_detail(
            pwd_id, stoken, pdir_fid, _fetch_share=1, fetch_share_full_path=1
        )

        if share_detail.get("code") != 0:
            return jsonify(
                {"success": False, "data": {"error": share_detail.get("message")}}
            )

        data = share_detail["data"]
        data["paths"] = [
            {"fid": i["fid"], "name": i["file_name"]}
            for i in share_detail["data"].get("full_path", [])
        ] or paths
        data["stoken"] = stoken
        data["drive_type"] = drive_type  # 返回网盘类型供前端使用

        # 正则处理预览
        def preview_regex(data, share_account=None):
            """
            对分享文件列表应用正则预览处理。
            
            Args:
                data: 分享文件列表数据
                share_account: 用于获取分享的账户（作为后备账户使用）
            """
            task = request.json.get("task", {})
            magic_regex = request.json.get("magic_regex", {})
            mr = MagicRename(magic_regex)
            mr.set_taskname(task.get("taskname", ""))
            # 获取用于预览的账户（用于查看目标目录中的已有文件）
            # 优先级：1.任务指定的账户 2.用于获取分享的账户 3.旧格式cookie
            preview_account = None
            task_account_name = task.get("account_name", "")
            
            if task_account_name and task_account_name != "auto":
                preview_account, _ = get_account_by_name(task_account_name)
            
            if not preview_account and share_account:
                # 使用获取分享时的同一个账户
                preview_account = share_account
            
            if not preview_account:
                # 回退到旧格式
                if config_data.get("cookie"):
                    preview_account = Quark(config_data["cookie"][0])
                else:
                    return
            
            # 获取目标目录的已有文件列表
            dir_file_list = []
            dir_filename_list = []
            savepath = task.get("savepath", "")
            
            if savepath:
                try:
                    get_fids = preview_account.get_fids([savepath])
                    if get_fids:
                        ls_result = preview_account.ls_dir(get_fids[0]["fid"])
                        if ls_result and "data" in ls_result:
                            dir_file_list = ls_result["data"].get("list", [])
                            dir_filename_list = [f["file_name"] for f in dir_file_list]
                except Exception as e:
                    logging.warning(f"[preview_regex] 获取目标目录失败: {e}")

            pattern, replace = mr.magic_regex_conv(
                task.get("pattern", ""), task.get("replace", "")
            )
            for share_file in data["list"]:
                search_pattern = (
                    task["update_subdir"]
                    if share_file["dir"] and task.get("update_subdir")
                    else pattern
                )
                if re.search(search_pattern, share_file["file_name"]):
                    # 文件名重命名，目录不重命名
                    file_name_re = (
                        share_file["file_name"]
                        if share_file["dir"]
                        else mr.sub(pattern, replace, share_file["file_name"])
                    )
                    if file_name_saved := mr.is_exists(
                        file_name_re,
                        dir_filename_list,
                        (task.get("ignore_extension") and not share_file["dir"]),
                    ):
                        share_file["file_name_saved"] = file_name_saved
                    else:
                        share_file["file_name_re"] = file_name_re
            # 文件列表排序
            if re.search(r"\{I+\}", replace):
                mr.set_dir_file_list(dir_file_list, replace)
                mr.sort_file_list(data["list"])

        if request.json.get("task"):
            preview_regex(data, share_account=account)

        return jsonify({"success": True, "data": data})
    except Exception as e:
        logging.error(f">>> get_share_detail 错误: {str(e)}")
        return jsonify({"success": False, "data": {"error": f"获取分享详情失败: {str(e)}"}})


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
        if path := request.args.get("path"):
            path = re.sub(r"/+", "/", path)
            if path == "/":
                fid = 0
            else:
                dir_names = path.split("/")
                if dir_names[0] == "":
                    dir_names.pop(0)
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
        ls_result = account.ls_dir(fid)
        if not ls_result or "data" not in ls_result:
            return jsonify({"success": False, "data": {"error": "获取目录列表失败，请检查Cookie是否有效"}})
        
        file_list = {
            "list": ls_result["data"].get("list", []),
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
        return jsonify({"success": False, "message": "未登录"})
    
    # 支持通过参数指定账户
    account_name = request.json.get("account_name", "")
    account, _ = get_account_by_name(account_name)
    
    if not account:
        return jsonify({"success": False, "message": "未配置有效的网盘账户"})
    
    if fid := request.json.get("fid"):
        response = account.delete([fid])
    else:
        response = {"success": False, "message": "缺失必要字段: fid"}
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
                Config.write_json(CONFIG_PATH, config_data)
                # 清除适配器缓存
                AdapterFactory.clear_cache()
                
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
                Config.write_json(CONFIG_PATH, config_data)
                AdapterFactory.clear_cache()

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
        old_token = target_account.get("cookie", "")
        target_account["cookie"] = new_token
        target_account["_token_updated_at"] = time.time()
        Config.write_json(CONFIG_PATH, config_data)
        # 清除适配器缓存
        AdapterFactory.clear_cache()
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
    config_data["tasklist"].append(request_data)
    Config.write_json(CONFIG_PATH, config_data)
    logging.info(f">>> 通过API添加任务: {request_data['taskname']}")
    return jsonify(
        {"success": True, "code": 0, "message": "任务添加成功", "data": request_data}
    )


# 定时任务执行的函数
def run_python(args):
    logging.info(f">>> 定时运行任务")
    try:
        result = subprocess.run(
            f"{PYTHON_PATH} {args}",
            shell=True,
            timeout=TASK_TIMEOUT,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
        )
        # 输出执行日志
        if result.stdout:
            for line in result.stdout.strip().split("\n"):
                if line.strip():
                    logging.info(line)

        if result.returncode == 0:
            logging.info(f">>> 任务执行成功")
        else:
            logging.error(f">>> 任务执行失败，返回码: {result.returncode}")
            if result.stderr:
                logging.error(f"错误信息: {result.stderr[:500]}")
    except subprocess.TimeoutExpired as e:
        logging.error(f">>> 任务执行超时(>{TASK_TIMEOUT}s)，强制终止")
    except Exception as e:
        logging.error(f">>> 任务执行异常: {str(e)}")
        logging.error(traceback.format_exc())
    finally:
        # 确保函数能够正常返回
        logging.debug(f">>> run_python 函数执行完成")


# 重新加载任务
def reload_tasks():
    # 读取定时规则
    if crontab := config_data.get("crontab"):
        if scheduler.state == 1:
            scheduler.pause()  # 暂停调度器
        trigger = CronTrigger.from_crontab(crontab)
        scheduler.remove_all_jobs()
        scheduler.add_job(
            run_python,
            trigger=trigger,
            args=[f"{SCRIPT_PATH} {CONFIG_PATH}"],
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


def init():
    global config_data, task_plugins_config_default
    logging.info(">>> 初始化配置")
    # 检查配置文件是否存在
    if not os.path.exists(CONFIG_PATH):
        if not os.path.exists(os.path.dirname(CONFIG_PATH)):
            os.makedirs(os.path.dirname(CONFIG_PATH))
        with open("quark_config.json", "rb") as src, open(CONFIG_PATH, "wb") as dest:
            dest.write(src.read())

    # 读取配置
    config_data = Config.read_json(CONFIG_PATH)
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
    plugins_config_default.update(config_data.get("plugins", {}))
    config_data["plugins"] = plugins_config_default

    # 更新配置
    Config.write_json(CONFIG_PATH, config_data)
    
    # 初始化阿里云盘 token 保存器
    if MULTI_DRIVE_SUPPORT:
        try:
            from adapters.aliyun_adapter import set_config_saver
            set_config_saver(CONFIG_PATH)
            logging.info(">>> 阿里云盘 token 保存器已初始化")
        except Exception as e:
            logging.warning(f">>> 初始化阿里云盘 token 保存器失败: {e}")

    # 初始化迅雷网盘 token 保存器
    if MULTI_DRIVE_SUPPORT:
        try:
            from adapters.xunlei_adapter import set_config_saver as xunlei_set_config_saver
            xunlei_set_config_saver(CONFIG_PATH)
            logging.info(">>> 迅雷网盘 token 保存器已初始化")
        except Exception as e:
            logging.warning(f">>> 初始化迅雷网盘 token 保存器失败: {e}")


if __name__ == "__main__":
    init()
    reload_tasks()
    logging.info(">>> 启动Web服务")
    logging.info(f"运行在: http://{HOST}:{PORT}")
    app.run(
        debug=DEBUG,
        host=HOST,
        port=PORT,
    )
