# !/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
日志配置模块
提供统一的日志管理功能，支持多级日志输出和灵活配置
"""
import logging
import sys
import os
from logging.handlers import RotatingFileHandler, TimedRotatingFileHandler
from datetime import datetime


class LogFormatter(logging.Formatter):
    """自定义日志格式化器，支持更丰富的信息展示"""
    
    # 定义颜色代码（仅在终端输出时使用）
    COLORS = {
        'DEBUG': '\033[36m',     # 青色
        'INFO': '\033[32m',      # 绿色
        'WARNING': '\033[33m',   # 黄色
        'ERROR': '\033[31m',     # 红色
        'CRITICAL': '\033[35m',  # 紫色
        'RESET': '\033[0m'       # 重置
    }
    
    def __init__(self, use_color=True):
        super().__init__()
        self.use_color = use_color
    
    def format(self, record):
        # 基础格式
        log_format = (
            "%(asctime)s | %(levelname)-8s | "
            "%(name)s | %(funcName)s:%(lineno)d | "
            "%(message)s"
        )
        
        # 添加颜色支持（仅当输出到终端时）
        if self.use_color and sys.stdout.isatty():
            color = self.COLORS.get(record.levelname, self.COLORS['RESET'])
            reset = self.COLORS['RESET']
            log_format = (
                f"{color}%(asctime)s | %(levelname)-8s{reset} | "
                f"{color}%(name)s{reset} | %(funcName)s:%(lineno)d | "
                f"%(message)s{color}{reset}"
            )
        
        formatter = logging.Formatter(log_format)
        return formatter.format(record)


def setup_logger(name=None, level=None, log_file=None, max_bytes=10*1024*1024, backup_count=5):
    """
    设置并返回 logger 实例
    
    Args:
        name: logger 名称，默认为模块名
        level: 日志级别，默认从环境变量读取
        log_file: 日志文件路径，如果为 None 则只输出到控制台
        max_bytes: 单个日志文件最大大小（字节），默认 10MB
        backup_count: 保留的备份文件数量
    
    Returns:
        logging.Logger: 配置好的 logger 实例
    """
    if name is None:
        # 获取调用者的模块名作为 logger 名
        import inspect
        frame = inspect.currentframe().f_back.f_back
        name = frame.f_globals.get('__name__', 'root')
    
    # 从环境变量读取日志级别
    if level is None:
        level_str = os.getenv('LOG_LEVEL', 'INFO').upper()
        level = getattr(logging, level_str, logging.INFO)
    
    # 检查是否需要彩色输出（仅当输出到终端且未禁用彩色时）
    use_color = sys.stdout.isatty() and os.getenv('NO_COLOR', '').lower() != 'true'
    
    # 创建 logger
    logger = logging.getLogger(name)
    logger.setLevel(level)
    
    # 避免重复添加 handler
    if logger.handlers:
        return logger
    
    # 创建控制台处理器
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)
    console_handler.setFormatter(LogFormatter(use_color=use_color))
    logger.addHandler(console_handler)
    
    # 创建文件处理器（如果指定了日志文件）
    if log_file:
        # 确保日志目录存在
        log_dir = os.path.dirname(log_file)
        if log_dir and not os.path.exists(log_dir):
            os.makedirs(log_dir, exist_ok=True)
        
        # 使用轮转文件处理器（按大小）
        file_handler = RotatingFileHandler(
            log_file, 
            maxBytes=max_bytes, 
            backupCount=backup_count,
            encoding='utf-8'
        )
        file_handler.setLevel(level)
        
        # 文件日志不使用颜色
        file_formatter = LogFormatter(use_color=False)
        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)
    
    return logger


# 便捷的日志函数
def get_logger(name=None):
    """快速获取 logger 实例"""
    return setup_logger(name)


def debug(msg, *args, **kwargs):
    """记录 DEBUG 级别日志"""
    logger = get_logger()
    logger.debug(msg, *args, **kwargs)


def info(msg, *args, **kwargs):
    """记录 INFO 级别日志"""
    logger = get_logger()
    logger.info(msg, *args, **kwargs)


def warning(msg, *args, **kwargs):
    """记录 WARNING 级别日志"""
    logger = get_logger()
    logger.warning(msg, *args, **kwargs)


def error(msg, *args, **kwargs):
    """记录 ERROR 级别日志"""
    logger = get_logger()
    logger.error(msg, *args, **kwargs)


def critical(msg, *args, **kwargs):
    """记录 CRITICAL 级别日志"""
    logger = get_logger()
    logger.critical(msg, *args, **kwargs)


def exception(msg, *args, exc_info=True, **kwargs):
    """记录异常信息，包含完整堆栈跟踪"""
    logger = get_logger()
    logger.exception(msg, *args, exc_info=exc_info, **kwargs)


# 装饰器：自动记录函数调用参数和返回值
def log_function_call(func):
    """
    装饰器：记录函数调用的详细信息
    
    Usage:
        @log_function_call
        def my_function(param1, param2):
            pass
    """
    import functools
    import traceback
    
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        logger = get_logger()
        func_name = f"{func.__module__}.{func.__qualname__}"
        
        # 记录调用参数
        logger.debug(f">>> 调用 {func_name}")
        logger.debug(f"    参数：args={args}, kwargs={kwargs}")
        
        try:
            result = func(*args, **kwargs)
            logger.debug(f"<<< {func_name} 返回：{result}")
            return result
        except Exception as e:
            logger.exception(f"!!! {func_name} 异常：{e}")
            raise
    
    return wrapper


# 上下文管理器：记录代码块执行时间
class log_execution_time:
    """
    上下文管理器：记录代码块执行时间
    
    Usage:
        with log_execution_time("重要操作"):
            # 你的代码
            pass
    """
    def __init__(self, operation_name, logger=None):
        self.operation_name = operation_name
        self.logger = logger or get_logger()
        self.start_time = None
    
    def __enter__(self):
        self.start_time = datetime.now()
        self.logger.debug(f"⏱️ 开始执行：{self.operation_name}")
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        duration = datetime.now() - self.start_time
        duration_ms = duration.total_seconds() * 1000
        
        if exc_type:
            self.logger.error(f"❌ {self.operation_name} 失败，耗时：{duration_ms:.2f}ms")
        else:
            self.logger.debug(f"✅ {self.operation_name} 完成，耗时：{duration_ms:.2f}ms")
        
        return False  # 不抑制异常


if __name__ == "__main__":
    # 测试示例
    logger = setup_logger("test_logger", level=logging.DEBUG)
    
    logger.debug("这是一条 DEBUG 日志")
    logger.info("这是一条 INFO 日志")
    logger.warning("这是一条 WARNING 日志")
    logger.error("这是一条 ERROR 日志")
    
    try:
        1 / 0
    except ZeroDivisionError:
        logger.exception("发生除零错误")
