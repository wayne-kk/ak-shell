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
from main import collect_today_data
from collectors import stock_basic_collector, stock_news_collector
from feishu_notify import send_completion_notice


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
        collect_today_data()
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


def job_news_update():
    """新闻数据更新任务"""
    logger.info("开始执行东方财富全球财经快讯采集任务")
    start_time = datetime.now()
    
    try:
        # 获取采集前统计
        stats_before = stock_news_collector.get_news_stats()
        count_before = stats_before.get('total_news', 0)
        
        # 限制每次处理10条新闻
        success = stock_news_collector.collect_news(max_process_count=10)
        
        # 获取采集后统计
        stats_after = stock_news_collector.get_news_stats()
        count_after = stats_after.get('total_news', 0)
        new_count = count_after - count_before
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        # 准备飞书通知详情
        details = {
            "采集耗时": f"{duration:.1f}秒",
            "采集前总数": f"{count_before}条",
            "采集后总数": f"{count_after}条", 
            "新增新闻": f"{new_count}条",
            "重复数据": f"{10 - new_count}条" if new_count <= 10 else "0条",
            "数据来源": "东方财富全球财经快讯"
        }
        
        if success:
            logger.info(f"东方财富全球财经快讯采集任务完成 - 新增{new_count}条，重复{10-new_count if new_count <= 10 else 0}条")
            # 发送成功通知到飞书
            send_completion_notice("东方财富全球财经快讯采集", True, details)
        else:
            logger.warning("东方财富全球财经快讯采集任务失败")
            # 发送失败通知到飞书
            send_completion_notice("东方财富全球财经快讯采集", False, details)
            
    except Exception as e:
        logger.error(f"东方财富全球财经快讯采集任务异常: {e}")
        # 发送异常通知到飞书
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        error_details = {
            "错误信息": str(e),
            "执行耗时": f"{duration:.1f}秒"
        }
        send_completion_notice("东方财富全球财经快讯采集", False, error_details)


def job_health_check():
    """健康检查任务"""
    logger.info("执行健康检查")
    # 可以在这里添加数据质量检查、系统状态检查等


def setup_schedules():
    """设置定时任务"""
    # 每日18:00执行数据更新（收盘后）
    schedule.every().day.at("18:00").do(job_daily_update)
    
    # 每周日凌晨2点执行基础信息更新
    schedule.every().sunday.at("02:00").do(job_weekly_update)
    
    # 每20分钟执行新闻采集
    schedule.every(20).minutes.do(job_news_update)
    
    # 每小时执行健康检查
    schedule.every().hour.do(job_health_check)
    
    logger.info("定时任务设置完成:")
    logger.info("- 每日18:00: 数据更新")
    logger.info("- 每周日02:00: 基础信息更新")
    logger.info("- 每20分钟: 东方财富全球财经快讯采集")
    logger.info("- 每小时: 健康检查")


def run_scheduler():
    """运行调度器"""
    logger.info("启动定时任务调度器...")
    
    # 设置日志
    setup_scheduler_logging()
    
    # 设置定时任务
    setup_schedules()
    
    # 立即执行一次新闻采集
    logger.info("立即执行一次新闻采集...")
    job_news_update()
    
    logger.info("调度器正在运行，按 Ctrl+C 停止")
    
    try:
        while True:
            schedule.run_pending()
            time.sleep(60)  # 每分钟检查一次
    except KeyboardInterrupt:
        logger.info("调度器已停止")


if __name__ == "__main__":
    run_scheduler() 