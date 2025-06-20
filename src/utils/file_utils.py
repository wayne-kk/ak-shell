#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
文件工具
"""

import os
from pathlib import Path
from datetime import datetime, timedelta


def ensure_directory(path) -> None:
    """确保目录存在"""
    Path(path).mkdir(parents=True, exist_ok=True)


def cleanup_old_files(directory, days: int = 30, pattern: str = "*") -> int:
    """
    清理指定天数前的文件
    
    Args:
        directory: 目录路径
        days: 保留天数
        pattern: 文件匹配模式
    
    Returns:
        清理的文件数量
    """
    directory = Path(directory)
    if not directory.exists():
        return 0
    
    cutoff_date = datetime.now() - timedelta(days=days)
    count = 0
    
    for file_path in directory.glob(pattern):
        if file_path.is_file():
            file_time = datetime.fromtimestamp(file_path.stat().st_mtime)
            if file_time < cutoff_date:
                file_path.unlink()
                count += 1
    
    return count 