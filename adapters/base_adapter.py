# -*- coding: utf-8 -*-
"""
云盘适配器基类
定义所有网盘适配器必须实现的接口
"""
from abc import ABC, abstractmethod
from typing import Dict, List, Tuple, Optional, Any


class BaseCloudDriveAdapter(ABC):
    """云盘适配器抽象基类"""

    # 网盘类型标识
    DRIVE_TYPE = "base"

    def __init__(self, cookie: str = "", index: int = 0):
        self.cookie = cookie.strip() if cookie else ""
        self.index = index + 1
        self.is_active = False
        self.nickname = ""
        self.savepath_fid: Dict[str, str] = {"/": "0"}

    @abstractmethod
    def init(self) -> Any:
        """
        初始化账户，验证 cookie 有效性
        Returns:
            成功返回账户信息 dict，失败返回 False
        """
        pass

    @abstractmethod
    def get_stoken(self, pwd_id: str, passcode: str = "") -> Dict:
        """
        获取分享令牌，验证资源有效性
        Args:
            pwd_id: 分享ID
            passcode: 提取码
        Returns:
            响应字典，包含 status, data, message 等字段
        """
        pass

    @abstractmethod
    def get_detail(
        self,
        pwd_id: str,
        stoken: str,
        pdir_fid: str,
        _fetch_share: int = 0,
        fetch_share_full_path: int = 0,
    ) -> Dict:
        """
        获取分享文件详情列表
        Args:
            pwd_id: 分享ID
            stoken: 分享令牌
            pdir_fid: 父目录ID
            _fetch_share: 是否获取分享信息
            fetch_share_full_path: 是否获取完整路径
        Returns:
            响应字典，包含 code, data.list 等字段
        """
        pass

    @abstractmethod
    def ls_dir(self, pdir_fid: str, **kwargs) -> Dict:
        """
        列出目录内容
        Args:
            pdir_fid: 目录ID
        Returns:
            响应字典，包含 code, data.list 等字段
        """
        pass

    @abstractmethod
    def save_file(
        self,
        fid_list: List[str],
        fid_token_list: List[str],
        to_pdir_fid: str,
        pwd_id: str,
        stoken: str,
    ) -> Dict:
        """
        转存文件到指定目录
        Args:
            fid_list: 文件ID列表
            fid_token_list: 文件token列表
            to_pdir_fid: 目标目录ID
            pwd_id: 分享ID
            stoken: 分享令牌
        Returns:
            响应字典，包含 code, data.task_id 等字段
        """
        pass

    @abstractmethod
    def query_task(self, task_id: str) -> Dict:
        """
        查询转存任务状态
        Args:
            task_id: 任务ID
        Returns:
            响应字典，包含任务状态信息
        """
        pass

    @abstractmethod
    def mkdir(self, dir_path: str) -> Dict:
        """
        创建目录
        Args:
            dir_path: 目录路径
        Returns:
            响应字典，包含 code, data.fid 等字段
        """
        pass

    @abstractmethod
    def rename(self, fid: str, file_name: str) -> Dict:
        """
        重命名文件
        Args:
            fid: 文件ID
            file_name: 新文件名
        Returns:
            响应字典
        """
        pass

    @abstractmethod
    def delete(self, filelist: List[str]) -> Dict:
        """
        删除文件
        Args:
            filelist: 文件ID列表
        Returns:
            响应字典
        """
        pass

    @abstractmethod
    def get_fids(self, file_paths: List[str]) -> List[Dict]:
        """
        根据路径获取文件ID
        Args:
            file_paths: 文件路径列表
        Returns:
            包含 file_path 和 fid 的字典列表
        """
        pass

    @abstractmethod
    def extract_url(self, url: str) -> Tuple[Optional[str], str, Any, List]:
        """
        解析分享链接
        Args:
            url: 分享链接
        Returns:
            (pwd_id, passcode, pdir_fid, paths) 元组
        """
        pass

    def get_account_info(self) -> Any:
        """获取账户信息（可选实现）"""
        return False

    def update_savepath_fid(self, tasklist: List[Dict]) -> bool:
        """
        更新保存路径的 fid 映射
        Args:
            tasklist: 任务列表
        Returns:
            是否成功
        """
        # 通用实现，子类可重写
        import re
        from datetime import datetime

        dir_paths = [
            re.sub(r"/{2,}", "/", f"/{item['savepath']}")
            for item in tasklist
            if not item.get("enddate")
            or (
                datetime.now().date()
                <= datetime.strptime(item["enddate"], "%Y-%m-%d").date()
            )
        ]
        if not dir_paths:
            return False

        dir_paths_exist_arr = self.get_fids(dir_paths)
        dir_paths_exist = [item["file_path"] for item in dir_paths_exist_arr]

        # 创建不存在的目录
        dir_paths_unexist = list(set(dir_paths) - set(dir_paths_exist) - set(["/"]))
        for dir_path in dir_paths_unexist:
            mkdir_return = self.mkdir(dir_path)
            if mkdir_return.get("code") == 0:
                new_dir = mkdir_return["data"]
                dir_paths_exist_arr.append(
                    {"file_path": dir_path, "fid": new_dir["fid"]}
                )
                print(f"创建文件夹：{dir_path}")
            else:
                print(f"创建文件夹：{dir_path} 失败, {mkdir_return.get('message', '未知错误')}")

        # 储存目标目录的fid
        for dir_path in dir_paths_exist_arr:
            self.savepath_fid[dir_path["file_path"]] = dir_path["fid"]

        return True
