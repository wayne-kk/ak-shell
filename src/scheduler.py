#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
定时任务调度器
"""

import schedule
import time
import os
import sys
from datetime import datetime
from loguru import logger
from main import collect_latest_data, setup_logging
from collectors import stock_basic_collector


def setup_scheduler_logging():
    """设置调度器日志"""
    logger.remove()
    logger.add(
        sys.stdout,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | {message}",
        level="INFO"
    )
    logger.add(
        "logs/scheduler_{time:YYYY-MM-DD}.log",
        format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {message}",
        level="INFO",
        rotation="1 day",
        retention="30 days"
    )


def job_daily_update():
    """每日数据更新任务"""
    logger.info("开始执行每日数据更新任务")
    try:
        collect_latest_data()
        logger.info("每日数据更新任务完成")
    except Exception as e:
        logger.error(f"每日数据更新任务失败: {e}")


def job_weekly_update():
    """每周数据更新任务"""
    logger.info("开始执行每周数据更新任务")
    try:
        # 更新股票基础信息
        stock_basic_collector.collect()
        logger.info("每周数据更新任务完成")
    except Exception as e:
        logger.error(f"每周数据更新任务失败: {e}")


def job_health_check():
    """健康检查任务"""
    logger.info("执行健康检查")
    # 可以在这里添加数据质量检查、系统状态检查等


def setup_schedules():
    """设置定时任务"""
    # 每日16:30执行数据更新（收盘后）
    schedule.every().day.at("16:30").do(job_daily_update)
    
    # 每周日凌晨2点执行基础信息更新
    schedule.every().sunday.at("02:00").do(job_weekly_update)
    
    # 每小时执行健康检查
    schedule.every().hour.do(job_health_check)
    
    logger.info("定时任务设置完成:")
    logger.info("- 每日16:30: 数据更新")
    logger.info("- 每周日02:00: 基础信息更新")
    logger.info("- 每小时: 健康检查")


def run_scheduler():
    """运行调度器"""
    logger.info("启动定时任务调度器...")
    
    # 设置日志
    setup_scheduler_logging()
    
    # 设置定时任务
    setup_schedules()
    
    logger.info("调度器正在运行，按 Ctrl+C 停止")
    
    try:
        while True:
            schedule.run_pending()
            time.sleep(60)  # 每分钟检查一次
    except KeyboardInterrupt:
        logger.info("调度器已停止")


if __name__ == "__main__":
    run_scheduler() 