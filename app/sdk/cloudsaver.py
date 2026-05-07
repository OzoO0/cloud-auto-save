import json
import logging
import os
import re
from typing import Any

import requests
from sdk.common import iso_to_cst


try:
    from utils.logger import get_logger

    logger = get_logger("sdk.cloudsaver")
except Exception:
    _level_str = os.getenv("LOG_LEVEL", "INFO").upper()
    _level = getattr(logging, _level_str, logging.INFO)
    if os.getenv("DEBUG", "false").strip().lower() == "true":
        _level = logging.DEBUG
    logging.basicConfig(
        level=_level,
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(funcName)s:%(lineno)d | %(message)s",
    )
    logger = logging.getLogger("sdk.cloudsaver")


def _mask_secret(value: str, keep: int = 2) -> str:
    v = (value or "").strip()
    if not v:
        return ""
    if len(v) <= keep:
        return "*" * len(v)
    return v[:keep] + "*" * (len(v) - keep)


def _redact(obj: Any) -> Any:
    if isinstance(obj, dict):
        out = {}
        for k, v in obj.items():
            key = str(k).lower()
            if key in {"password", "passwd"}:
                out[k] = "***"
            elif key in {"token", "access_token", "refresh_token", "authorization"}:
                out[k] = _mask_secret(str(v), keep=4)
            elif key in {"cookie", "cookies"}:
                out[k] = "***"
            else:
                out[k] = _redact(v)
        return out
    if isinstance(obj, list):
        return [_redact(x) for x in obj]
    return obj


def _truncate_text(text: str, max_chars: int) -> str:
    if max_chars <= 0:
        return text
    if len(text) <= max_chars:
        return text
    head = text[: max_chars // 2]
    tail = text[-max_chars // 2 :]
    return f"{head}\n... (truncated, total_chars={len(text)}) ...\n{tail}"


def _strip_html(text: str) -> str:
    if not text:
        return ""
    t = text.replace('<mark class="highlight">', "").replace("</mark>", "")
    t = re.sub(r"<[^>]+>", "", t)
    return t


def _title_contains_keyword(title: str, keyword: str) -> bool:
    title = (_strip_html(title) or "").strip()
    keyword = (keyword or "").strip()
    if not keyword:
        return True
    if match := re.search(r"(名称|标题)[：:]?(.*)", title, re.DOTALL):
        title = (match.group(2) or "").strip()
    title_norm = re.sub(r"\s+", "", title).lower()
    tokens = [t for t in re.split(r"\s+", keyword) if t]
    return all(re.sub(r"\s+", "", t).lower() in title_norm for t in tokens)


def _debug_dump(label: str, obj: Any) -> None:
    if not logger.isEnabledFor(logging.DEBUG):
        return
    max_chars = int(os.getenv("CLOUDSAVER_DEBUG_MAX_CHARS", "200000"))
    try:
        s = json.dumps(_redact(obj), ensure_ascii=False, indent=2, default=str)
    except Exception:
        s = repr(_redact(obj))
    logger.debug(f"{label}={_truncate_text(s, max_chars)}")


class CloudSaver:
    """
    CloudSaver 类，用于获取云盘资源
    """

    def __init__(self, server):
        self.server = server
        self.username = None
        self.password = None
        self.token = None
        self.session = requests.Session()
        self.session.headers.update({"Content-Type": "application/json"})
        logger.debug(f"[init] server={self.server}")

    def set_auth(self, username, password, token=""):
        self.username = username
        self.password = password
        self.token = token
        self.session.headers.update({"Authorization": f"Bearer {self.token}"})
        logger.debug(
            f"[set_auth] username={_mask_secret(self.username or '')} token={_mask_secret(self.token or '', keep=4)}"
        )

    def login(self):
        if not self.username or not self.password:
            return {"success": False, "message": "CloudSaver未设置用户名或密码"}
        try:
            url = f"{self.server}/api/user/login"
            data = {"username": self.username, "password": self.password}
            logger.debug(f"[login] POST {url} username={_mask_secret(self.username)}")
            response = self.session.post(url, json=data)
            logger.debug(
                f"[login] status_code={response.status_code} content_type={response.headers.get('Content-Type','')}"
            )
            result = response.json()
            _debug_dump("[login] response_json", result)
            if result.get("success"):
                self.token = result.get("data", {}).get("token")
                self.session.headers.update({"Authorization": f"Bearer {self.token}"})
                logger.debug(
                    f"[login] success token_len={len(self.token or '')} token={_mask_secret(self.token or '', keep=4)}"
                )
                return {"success": True, "token": self.token}
            else:
                logger.debug(f"[login] failed message={result.get('message', '')}")
                return {
                    "success": False,
                    "message": f"CloudSaver登录{result.get('message', '未知错误')}",
                }
        except Exception as e:
            logger.exception(f"[login] exception: {e}")
            return {"success": False, "message": str(e)}

    def search(self, keyword, last_message_id=""):
        """
        搜索资源

        Args:
            keyword (str): 搜索关键词
            last_message_id (str): 上一条消息ID，用于分页

        Returns:
            list: 搜索结果列表
        """
        try:
            url = f"{self.server}/api/search"
            params = {"keyword": keyword, "lastMessageId": last_message_id}
            logger.debug(f"[search] GET {url} params={params}")
            response = self.session.get(url, params=params)
            logger.debug(
                f"[search] status_code={response.status_code} content_type={response.headers.get('Content-Type','')}"
            )
            try:
                result = response.json()
            except Exception:
                max_chars = int(os.getenv("CLOUDSAVER_DEBUG_MAX_CHARS", "200000"))
                logger.debug(
                    f"[search] response_text={_truncate_text(response.text or '', max_chars)}"
                )
                raise
            _debug_dump("[search] response_json", result)
            if result.get("success"):
                data = result.get("data", [])
                if isinstance(data, list):
                    logger.debug(f"[search] success channels={len(data)}")
                else:
                    logger.debug(f"[search] success data_type={type(data).__name__}")
                if (
                    isinstance(data, list)
                    and os.getenv("CLOUDSAVER_TITLE_FILTER", "1").strip() != "0"
                ):
                    before_items = sum(
                        len(ch.get("list", [])) for ch in data if isinstance(ch, dict)
                    )
                    filtered_data = []
                    for ch in data:
                        if not isinstance(ch, dict):
                            continue
                        lst = ch.get("list", [])
                        if not isinstance(lst, list):
                            filtered_data.append(ch)
                            continue
                        new_list = [
                            it
                            for it in lst
                            if isinstance(it, dict)
                            and _title_contains_keyword(it.get("title", ""), keyword)
                        ]
                        if new_list:
                            new_ch = dict(ch)
                            new_ch["list"] = new_list
                            filtered_data.append(new_ch)
                    after_items = sum(
                        len(ch.get("list", []))
                        for ch in filtered_data
                        if isinstance(ch, dict)
                    )
                    logger.debug(
                        f"[search] title_filter enabled keyword={keyword} items={before_items}->{after_items} channels={len(data)}->{len(filtered_data)}"
                    )
                    data = filtered_data
                return {"success": True, "data": data}
            else:
                logger.debug(f"[search] failed message={result.get('message', '')}")
                return {"success": False, "message": result.get("message", "未知错误")}
        except Exception as e:
            logger.exception(f"[search] exception: {e}")
            return {"success": False, "message": str(e)}

    def auto_login_search(self, keyword, last_message_id=""):
        """
        自动登录并搜索资源

        Args:
            keyword (str): 搜索关键词
            last_message_id (str): 上一条消息ID，用于分页
        """
        logger.debug(f"[auto_login_search] keyword={keyword} last_message_id={last_message_id}")
        result = self.search(keyword, last_message_id)
        if result.get("success"):
            return result
        else:
            if (
                result.get("message") == "无效的 token"
                or result.get("message") == "未提供 token"
            ):
                logger.debug("[auto_login_search] token invalid/missing, try login")
                login_result = self.login()
                if login_result.get("success"):
                    result = self.search(keyword, last_message_id)
                    result["new_token"] = login_result.get("token")
                    return result
                else:
                    logger.debug(f"[auto_login_search] login failed: {login_result.get('message', '')}")
                    return {
                        "success": False,
                        "message": login_result.get("message", "未知错误"),
                    }
            logger.debug(f"[auto_login_search] search failed: {result.get('message', '未知错误')}")
            return {"success": False, "message": result.get("message", "未知错误")}

    def clean_search_results(self, search_results):
        """
        清洗搜索结果

        Args:
            search_results (list): 搜索结果列表

        Returns:
            list: 夸克网盘链接列表
        """
        logger.debug(f"[clean_search_results] input_type={type(search_results).__name__}")
        if os.getenv("CLOUDSAVER_DEBUG_VERBOSE", "0").strip() == "1":
            _debug_dump("[clean_search_results] input", search_results)
        pattern_title = r"(名称|标题)[：:]?(.*)"
        pattern_content = r"(描述|简介)[：:]?(.*)(链接|标签)"
        clean_results = []
        link_array = []
        for channel in search_results:
            if os.getenv("CLOUDSAVER_DEBUG_VERBOSE", "0").strip() == "1":
                _debug_dump("[clean_search_results] channel", channel)
            for item in channel.get("list", []):
                if os.getenv("CLOUDSAVER_DEBUG_VERBOSE", "0").strip() == "1":
                    _debug_dump("[clean_search_results] item", item)
                cloud_links = item.get("cloudLinks", [])
                for link in cloud_links:
                    if os.getenv("CLOUDSAVER_DEBUG_VERBOSE", "0").strip() == "1":
                        _debug_dump("[clean_search_results] cloudLink", link)
                    if link.get("cloudType") in ["quark", "pan123", "pan115", "uc","tianyi","aliyun","xunlei","baiduPan"]:
                        # 清洗标题
                        title = item.get("title", "")
                        if match := re.search(pattern_title, title, re.DOTALL):
                            title = match.group(2)
                        title = title.replace("&amp;", "&").strip()
                        # 清洗内容
                        content = item.get("content", "")
                        if match := re.search(pattern_content, content, re.DOTALL):
                            content = match.group(2)
                        content = content.replace('<mark class="highlight">', "")
                        content = content.replace("</mark>", "")
                        content = content.strip()
                        # 统一发布时间格式
                        pubdate = item.get("pubDate", "")
                        if pubdate:
                            pubdate = iso_to_cst(pubdate)
                        # 链接去重
                        if link.get("link") not in link_array:
                            link_array.append(link.get("link"))
                            out_item = {
                                "shareurl": link.get("link"),
                                "taskname": title,
                                "content": content,
                                "datetime": pubdate,
                                "tags": item.get("tags", []),
                                "channel": item.get("channelId", ""),
                                "source": "CloudSaver",
                            }
                            clean_results.append(out_item)
                            logger.debug(
                                "[clean_search_results] extracted shareurl=%s taskname=%s channel=%s cloudType=%s",
                                out_item.get("shareurl", ""),
                                out_item.get("taskname", ""),
                                out_item.get("channel", ""),
                                link.get("cloudType", ""),
                            )
        _debug_dump("[clean_search_results] output", clean_results)
        logger.debug(
            f"[clean_search_results] output_count={len(clean_results)} unique_links={len(link_array)}"
        )
        return clean_results


# 测试示例
if __name__ == "__main__":
    # 创建CloudSaver实例
    server = ""
    username = ""
    password = ""
    token = ""
    cloud_saver = CloudSaver(server)
    cloud_saver.set_auth(username, password, token)
    # 搜索资源
    results = cloud_saver.auto_login_search("黑镜")
    # 提取夸克网盘链接
    clean_results = cloud_saver.clean_search_results(results.get("data", []))
    # 打印结果
    for item in clean_results:
        print(f"标题: {item['taskname']}")
        print(f"描述: {item['content']}")
        print(f"链接: {item['shareurl']}")
        print(f"标签: {' '.join(item['tags'])}")
        print("-" * 50)
