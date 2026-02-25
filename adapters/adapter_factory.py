# -*- coding: utf-8 -*-
"""
适配器工厂类
负责根据配置创建不同网盘的适配器实例
"""
import re
import hashlib
from typing import Dict, List, Optional, Type

from adapters.base_adapter import BaseCloudDriveAdapter
from adapters.quark_adapter import QuarkAdapter
from adapters.cloud115_adapter import Cloud115Adapter
from adapters.baidu_adapter import BaiduAdapter
from adapters.xunlei_adapter import XunleiAdapter
from adapters.aliyun_adapter import AliyunAdapter
from adapters.uc_adapter import UCAdapter


class AdapterFactory:
    """适配器工厂"""

    # 适配器映射表
    ADAPTER_MAP: Dict[str, Type[BaseCloudDriveAdapter]] = {
        "quark": QuarkAdapter,
        "115": Cloud115Adapter,
        "baidu": BaiduAdapter,
        "xunlei": XunleiAdapter,
        "aliyun": AliyunAdapter,
        "uc": UCAdapter,
    }

    # URL 模式映射
    URL_PATTERNS: Dict[str, str] = {
        r"pan\.quark\.cn": "quark",
        r"(?:115|anxia|115cdn)\.com": "115",  # 115网盘有多个域名
        r"pan\.baidu\.com": "baidu",
        r"pan\.xunlei\.com": "xunlei",
        r"(?:alipan|aliyundrive)\.com": "aliyun",
        r"drive\.uc\.cn": "uc",
    }

    # 实例缓存: (drive_type, cookie_hash) -> adapter_instance
    _instance_cache: Dict[str, BaseCloudDriveAdapter] = {}

    @classmethod
    def _make_cache_key(cls, drive_type: str, cookie: str) -> str:
        """生成缓存键"""
        cookie_hash = hashlib.md5(cookie.encode("utf-8")).hexdigest()[:16]
        return f"{drive_type}:{cookie_hash}"

    @classmethod
    def register_adapter(cls, drive_type: str, adapter_class: Type[BaseCloudDriveAdapter]):
        """
        注册新的适配器
        Args:
            drive_type: 网盘类型标识
            adapter_class: 适配器类
        """
        cls.ADAPTER_MAP[drive_type] = adapter_class

    @classmethod
    def register_url_pattern(cls, pattern: str, drive_type: str):
        """
        注册 URL 匹配模式
        Args:
            pattern: 正则表达式模式
            drive_type: 网盘类型标识
        """
        cls.URL_PATTERNS[pattern] = drive_type

    @classmethod
    def create_adapter(
        cls, drive_type: str, cookie: str, index: int = 0
    ) -> Optional[BaseCloudDriveAdapter]:
        """
        创建或获取缓存的适配器实例。
        同一 (drive_type, cookie) 组合只创建一次实例，后续返回缓存。
        Args:
            drive_type: 网盘类型（quark, 115, baidu 等）
            cookie: 认证 cookie
            index: 账户索引
        Returns:
            适配器实例，失败返回 None
        """
        adapter_class = cls.ADAPTER_MAP.get(drive_type)
        if not adapter_class:
            print(f"未知的网盘类型: {drive_type}")
            return None

        # 查找缓存
        cache_key = cls._make_cache_key(drive_type, cookie)
        cached = cls._instance_cache.get(cache_key)
        if cached is not None:
            return cached

        # 创建新实例并缓存
        try:
            adapter = adapter_class(cookie=cookie, index=index)
            cls._instance_cache[cache_key] = adapter
            return adapter
        except Exception as e:
            print(f"创建适配器失败: {e}")
            return None

    @classmethod
    def clear_cache(cls):
        """清空实例缓存（配置更新时调用）"""
        cls._instance_cache.clear()

    @classmethod
    def get_drive_type_by_url(cls, url: str) -> Optional[str]:
        """
        根据 URL 判断网盘类型
        Args:
            url: 分享链接
        Returns:
            网盘类型标识，无法识别返回 None
        """
        for pattern, drive_type in cls.URL_PATTERNS.items():
            if re.search(pattern, url):
                return drive_type
        return None

    @classmethod
    def create_adapter_by_url(
        cls, url: str, cookie: str, index: int = 0
    ) -> Optional[BaseCloudDriveAdapter]:
        """
        根据 URL 自动创建适配器
        Args:
            url: 分享链接
            cookie: 认证 cookie
            index: 账户索引
        Returns:
            适配器实例
        """
        drive_type = cls.get_drive_type_by_url(url)
        if not drive_type:
            print(f"无法识别的分享链接: {url}")
            return None
        return cls.create_adapter(drive_type, cookie, index)

    @classmethod
    def get_supported_types(cls) -> List[str]:
        """获取支持的网盘类型列表"""
        return list(cls.ADAPTER_MAP.keys())


class AccountManager:
    """账户管理器"""

    def __init__(self):
        self.adapters: Dict[str, BaseCloudDriveAdapter] = {}
        self.default_adapter: Optional[BaseCloudDriveAdapter] = None

    def load_accounts(self, config_data: Dict) -> bool:
        """
        从配置加载所有账户
        支持新格式（accounts）和旧格式（cookie）
        Args:
            config_data: 配置数据
        Returns:
            是否成功加载
        """
        self.adapters.clear()
        self.default_adapter = None

        # 新格式：accounts 列表
        if "accounts" in config_data:
            for i, account in enumerate(config_data["accounts"]):
                if not account.get("enabled", True):
                    continue

                name = account.get("name", f"账户{i+1}")
                drive_type = account.get("drive_type", "quark")
                cookie = account.get("cookie", "")

                adapter = AdapterFactory.create_adapter(drive_type, cookie, i)
                if adapter:
                    self.adapters[name] = adapter
                    if account.get("default", False) or self.default_adapter is None:
                        self.default_adapter = adapter

        # 旧格式：cookie 字符串或列表（兼容现有配置）
        elif "cookie" in config_data:
            cookies = config_data["cookie"]
            if isinstance(cookies, str):
                cookies = [cookies] if cookies else []
            elif cookies and "\n" in cookies[0]:
                cookies = cookies[0].split("\n")

            for i, cookie in enumerate(cookies):
                if not cookie.strip():
                    continue

                name = f"夸克账户{i+1}"
                adapter = AdapterFactory.create_adapter("quark", cookie.strip(), i)
                if adapter:
                    self.adapters[name] = adapter
                    if self.default_adapter is None:
                        self.default_adapter = adapter

        return bool(self.adapters)

    def get_adapter(self, name: str) -> Optional[BaseCloudDriveAdapter]:
        """根据名称获取适配器"""
        return self.adapters.get(name)

    def get_default_adapter(self) -> Optional[BaseCloudDriveAdapter]:
        """获取默认适配器"""
        return self.default_adapter

    def get_adapter_for_task(self, task: Dict) -> Optional[BaseCloudDriveAdapter]:
        """
        为任务选择适配器
        优先使用任务指定的账户，否则根据 URL 自动选择
        Args:
            task: 任务配置
        Returns:
            适配器实例
        """
        # 1. 任务指定了账户名
        if account_name := task.get("account_name"):
            adapter = self.adapters.get(account_name)
            if adapter:
                return adapter
            print(f"警告：指定的账户 '{account_name}' 不存在，尝试自动选择")

        # 2. 根据 URL 判断网盘类型
        shareurl = task.get("shareurl", "")
        drive_type = AdapterFactory.get_drive_type_by_url(shareurl)

        if drive_type:
            # 查找该类型的第一个可用账户
            for adapter in self.adapters.values():
                if adapter.DRIVE_TYPE == drive_type and adapter.is_active:
                    return adapter

        # 3. 使用默认适配器
        return self.default_adapter

    def get_all_adapters(self) -> Dict[str, BaseCloudDriveAdapter]:
        """获取所有适配器"""
        return self.adapters

    def get_adapters_by_type(self, drive_type: str) -> List[BaseCloudDriveAdapter]:
        """获取指定类型的所有适配器"""
        return [
            adapter
            for adapter in self.adapters.values()
            if adapter.DRIVE_TYPE == drive_type
        ]

    def init_all_adapters(self) -> int:
        """
        初始化所有适配器
        Returns:
            成功初始化的数量
        """
        success_count = 0
        for name, adapter in self.adapters.items():
            if adapter.init():
                success_count += 1
                print(f"✅ 账户 '{name}' ({adapter.DRIVE_TYPE}) 登录成功: {adapter.nickname}")
            else:
                print(f"❌ 账户 '{name}' ({adapter.DRIVE_TYPE}) 登录失败")
        return success_count
