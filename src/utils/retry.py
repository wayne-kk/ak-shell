#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
重试机制工具
"""

import time
import functools
from typing import Callable, Any


def retry_with_backoff(
    max_retries: int = 3,
    delay: float = 1.0,
    backoff: float = 2.0,
    exceptions: tuple = (Exception,)
):
    """
    带退避机制的重试装饰器
    
    Args:
        max_retries: 最大重试次数
        delay: 初始延时（秒）
        backoff: 退避倍数
        exceptions: 需要重试的异常类型
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            current_delay = delay
            
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    if attempt == max_retries:
                        raise
                    
                    time.sleep(current_delay)
                    current_delay *= backoff
            
            return None
        return wrapper
    return decorator 