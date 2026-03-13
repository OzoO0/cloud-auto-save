# -*- coding: utf-8 -*-
"""
应用级 TTL 缓存模块（线程安全版）
用于缓存预览功能中的 get_fids 和 ls_dir 结果，避免重复 API 调用。
缓存独立于 adapter 实例，不受 AdapterFactory.clear_cache() 影响（需手动调用 invalidate_all）。
"""
import hashlib
import logging
import threading

from cachetools import TTLCache

logger = logging.getLogger(__name__)

# get_fids 结果缓存：路径→fid 映射变化极少，缓存 10 分钟
_fids_cache: TTLCache = TTLCache(maxsize=200, ttl=600)
_fids_lock = threading.Lock()

# ls_dir 结果缓存：目录内容可能变化，缓存 2 分钟
_lsdir_cache: TTLCache = TTLCache(maxsize=100, ttl=120)
_lsdir_lock = threading.Lock()


def make_cache_key(drive_type: str, cookie: str, *args) -> str:
    """
    生成缓存键：drive_type + cookie_hash + 额外参数
    Args:
        drive_type: 网盘类型标识
        cookie: 认证 cookie（取 MD5 前 12 位）
        *args: 额外的键组成部分（如 'fids'、路径、fid 等）
    Returns:
        缓存键字符串
    """
    cookie_hash = hashlib.md5(cookie.encode("utf-8")).hexdigest()[:12]
    return f"{drive_type}:{cookie_hash}:{':'.join(str(a) for a in args)}"


def get_cached_fids(key: str):
    """获取 get_fids 缓存结果（线程安全）"""
    with _fids_lock:
        result = _fids_cache.get(key)
    if result is not None:
        logger.debug(f"[cache] fids 缓存命中: {key}")
    return result


def set_cached_fids(key: str, value):
    """设置 get_fids 缓存（线程安全）"""
    with _fids_lock:
        _fids_cache[key] = value
    logger.debug(f"[cache] fids 缓存写入: {key}")


def get_cached_lsdir(key: str):
    """获取 ls_dir 缓存结果（线程安全）"""
    with _lsdir_lock:
        result = _lsdir_cache.get(key)
    if result is not None:
        logger.debug(f"[cache] lsdir 缓存命中: {key}")
    return result


def set_cached_lsdir(key: str, value):
    """设置 ls_dir 缓存（线程安全）"""
    with _lsdir_lock:
        _lsdir_cache[key] = value
    logger.debug(f"[cache] lsdir 缓存写入: {key}")


def invalidate_lsdir(key: str = None):
    """
    失效 ls_dir 缓存（线程安全）。
    Args:
        key: 指定缓存键，None 时清空全部 ls_dir 缓存
    """
    with _lsdir_lock:
        if key:
            _lsdir_cache.pop(key, None)
            logger.debug(f"[cache] lsdir 缓存失效: {key}")
        else:
            _lsdir_cache.clear()
            logger.debug("[cache] lsdir 缓存全部清空")


def invalidate_all():
    """清空所有缓存（get_fids + ls_dir）（线程安全）"""
    with _fids_lock:
        _fids_cache.clear()
    with _lsdir_lock:
        _lsdir_cache.clear()
    logger.debug("[cache] 全部缓存已清空")
