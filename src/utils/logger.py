#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
日志配置工具
"""

import sys
from typing import Optional
from loguru import logger
from pathlib import Path
from config import config


def setup_logger(name: str = "default", log_file: Optional[str] = None):
    """
    设置日志配置
    
    Args:
        name: 日志名称
        log_file: 日志文件名（可选）
    """
    # 清除默认配置
    logger.remove()
    
    # 控制台输出
    logger.add(
        sys.stdout,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan> | {message}",
        level=config.LOG_LEVEL,
        filter=lambda record: record["extra"].get("name") == name
    )
    
    # 文件输出
    if log_file:
        log_path = config.LOGS_DIR / log_file
    else:
        log_path = config.LOGS_DIR / f"{name}_{'{time:YYYY-MM-DD}'}.log"
    
    logger.add(
        str(log_path),
        format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name} | {message}",
        level=config.LOG_LEVEL,
        rotation="1 day",
        retention=f"{config.LOG_RETENTION_DAYS} days",
        filter=lambda record: record["extra"].get("name") == name
    )
    
    return logger.bind(name=name)


def get_logger(name: str):
    """获取指定名称的日志记录器"""
    return logger.bind(name=name) 