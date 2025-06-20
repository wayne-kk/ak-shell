#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
日期工具
"""

from datetime import datetime, timedelta
from typing import List


def get_trade_dates(start_date: str, end_date: str) -> List[str]:
    """
    获取交易日期列表（简化版，排除周末）
    
    Args:
        start_date: 开始日期 YYYYMMDD
        end_date: 结束日期 YYYYMMDD
    
    Returns:
        交易日期列表
    """
    start = datetime.strptime(start_date, '%Y%m%d')
    end = datetime.strptime(end_date, '%Y%m%d')
    
    dates = []
    current = start
    while current <= end:
        # 排除周末
        if current.weekday() < 5:  # 0-4 是周一到周五
            dates.append(current.strftime('%Y%m%d'))
        current += timedelta(days=1)
    
    return dates


def is_trade_date(date_str: str) -> bool:
    """
    判断是否为交易日（简化版，排除周末）
    
    Args:
        date_str: 日期字符串 YYYYMMDD
    
    Returns:
        是否为交易日
    """
    date = datetime.strptime(date_str, '%Y%m%d')
    return date.weekday() < 5 