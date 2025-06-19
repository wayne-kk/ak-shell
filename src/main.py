#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
A股数据采集主程序
支持历史数据采集和增量更新
"""

import argparse
import sys
from datetime import datetime, timedelta
from loguru import logger
from typing import Optional
from collectors import (
    stock_basic_collector,
    daily_quote_collector,
    index_data_collector,
    stock_hot_rank_collector,
    stock_hot_up_collector
)
from feishu_notify import send_completion_notice, send_daily_summary


def setup_logging():
    """设置日志"""
    logger.remove()
    logger.add(
        sys.stdout,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | {message}",
        level="INFO"
    )
    logger.add(
        "logs/collector_{time:YYYY-MM-DD}.log",
        format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {message}",
        level="INFO",
        rotation="1 day",
        retention="30 days"
    )


def collect_historical_data(start_date: str, end_date: str = '', 
                          enable_resume: bool = True, delay_config: Optional[dict] = None):
    """采集历史数据"""
    logger.info("=" * 60)
    logger.info("开始采集历史数据")
    logger.info("=" * 60)
    
    if not end_date:
        end_date = datetime.now().strftime('%Y%m%d')
    
    logger.info(f"采集时间范围: {start_date} ~ {end_date}")
    logger.info(f"断点续传: {'启用' if enable_resume else '禁用'}")
    
    # 配置延时参数
    if delay_config:
        daily_quote_collector.set_delay_config(**delay_config)
        logger.info("已应用自定义延时配置")
    
    # 采集股票历史行情数据
    logger.info("\n采集股票历史行情数据...")
    daily_quote_collector.collect_all_stocks_history(start_date, end_date, enable_resume)
    
    logger.info("\n" + "=" * 60)
    logger.info("历史数据采集完成")
    logger.info("=" * 60)



def collect_today_data():
    """采集当日数据（收盘后采集当天全量数据）"""
    start_time = datetime.now()
    logger.info("=" * 60)
    logger.info("开始采集当日全量数据")
    logger.info("=" * 60)
    
    today = datetime.now().strftime('%Y%m%d')
    logger.info(f"采集日期: {today}")
    
    stock_success = False
    index_success = False
    hot_rank_success = False
    hot_up_success = False
    
    # 1. 采集当日最新行情数据（批量接口）
    logger.info("\n1. 采集当日最新行情数据...")
    try:
        stock_success = daily_quote_collector.collect_latest_quotes()
        if stock_success:
            logger.info("✅ 当日最新行情数据采集成功")
        else:
            logger.error("❌ 当日最新行情数据采集失败")
    except Exception as e:
        logger.error(f"❌ 当日最新行情数据采集异常: {e}")
    
    # 2. 采集当日指数数据（支持自动重试）
    logger.info(f"\n2. 采集当日指数数据 ({today})...")
    try:
        # 设置重试延迟为1小时，如果数据源未更新会自动重试
        index_success = index_data_collector.collect_all_indexes_history(today, today, retry_delay_hours=1)
        if index_success:
            logger.info("✅ 当日指数数据采集成功")
        else:
            logger.error("❌ 当日指数数据采集失败")
    except Exception as e:
        logger.error(f"❌ 当日指数数据采集异常: {e}")
    
    # 3. 采集股票人气榜数据
    logger.info(f"\n3. 采集股票人气榜数据 ({today})...")
    try:
        hot_rank_success = stock_hot_rank_collector.collect_hot_rank()
        if hot_rank_success:
            logger.info("✅ 股票人气榜数据采集成功")
        else:
            logger.error("❌ 股票人气榜数据采集失败")
    except Exception as e:
        logger.error(f"❌ 股票人气榜数据采集异常: {e}")
    
    # 4. 采集股票飙升榜数据
    logger.info(f"\n4. 采集股票飙升榜数据 ({today})...")
    try:
        hot_up_success = stock_hot_up_collector.collect_hot_up()
        if hot_up_success:
            logger.info("✅ 股票飙升榜数据采集成功")
        else:
            logger.error("❌ 股票飙升榜数据采集失败")
    except Exception as e:
        logger.error(f"❌ 股票飙升榜数据采集异常: {e}")
    
    end_time = datetime.now()
    duration = end_time - start_time
    overall_success = stock_success and index_success and hot_rank_success and hot_up_success
    
    logger.info("\n" + "=" * 60)
    logger.info("当日数据采集完成")
    logger.info("=" * 60)
    
    # 发送飞书通知
    try:
        # 准备汇总数据
        from database import db
        stock_count = len(db.get_stock_list())
        
        success_count = (1 if stock_success else 0) + (1 if index_success else 0) + (1 if hot_rank_success else 0) + (1 if hot_up_success else 0)
        total_tasks = 4
        
        summary_data = {
            "total_stocks": f"{stock_count}只",
            "trade_date": today,
            "updated_records": f"约{stock_count}条" if stock_success else "0条",
            "start_time": start_time.strftime('%H:%M:%S'),
            "end_time": end_time.strftime('%H:%M:%S'),
            "duration": f"{duration.total_seconds():.1f}秒",
            "today_status": "✅ 成功" if stock_success else "❌ 失败",
            "index_status": "✅ 成功" if index_success else "❌ 失败",
            "hot_rank_status": "✅ 成功" if hot_rank_success else "❌ 失败",
            "hot_up_status": "✅ 成功" if hot_up_success else "❌ 失败",
            "overall_status": f"✅ 成功({success_count}/{total_tasks})" if overall_success else f"⚠️ 部分成功({success_count}/{total_tasks})"
        }
        
        # 发送每日汇总
        send_daily_summary(summary_data)
        logger.info("📱 飞书通知已发送")
        
    except Exception as e:
        logger.error(f"发送飞书通知失败: {e}")
        # 发送简单通知
        try:
            success_count = (1 if stock_success else 0) + (1 if index_success else 0) + (1 if hot_rank_success else 0) + (1 if hot_up_success else 0)
            send_completion_notice(
                "当日数据采集", 
                overall_success,
                {"成功任务": f"{success_count}/4", "执行时间": f"{duration.total_seconds():.1f}秒"}
            )
        except:
            pass



def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='A股数据采集工具')
    parser.add_argument('action', choices=[
        'history',      # 采集历史数据
        'today',        # 采集当日数据（包含股票+指数+通知）
    ], help='操作类型')
    
    parser.add_argument('--start-date', type=str, help='开始日期 (YYYYMMDD)')
    parser.add_argument('--end-date', type=str, help='结束日期 (YYYYMMDD)')
    
    # 断点续传控制
    parser.add_argument('--no-resume', action='store_true', help='禁用断点续传功能')
    
    # 延时控制参数
    parser.add_argument('--base-delay', type=float, default=0.2, help='基础延时(秒), 默认0.2')
    parser.add_argument('--random-delay', type=float, default=0.3, help='随机延时范围(秒), 默认0.3')
    parser.add_argument('--batch-delay', type=float, default=2.0, help='批次间延时(秒), 默认2.0')
    parser.add_argument('--batch-size', type=int, default=100, help='批次大小, 默认100')
    
    args = parser.parse_args()
    
    # 设置日志
    setup_logging()
    
    # 构建延时配置
    delay_config = {
        'base_delay': args.base_delay,
        'random_delay': args.random_delay,
        'batch_delay': args.batch_delay,
        'batch_size': args.batch_size
    }
    
    # 断点续传配置
    enable_resume = not args.no_resume
    
    try:
        if args.action == 'history':
            if not args.start_date:
                logger.error("采集历史数据需要指定开始日期 --start-date")
                return
            collect_historical_data(args.start_date, args.end_date, enable_resume, delay_config)
            
        elif args.action == 'today':
            collect_today_data()
            
    except KeyboardInterrupt:
        logger.info("用户中断操作")
    except Exception as e:
        logger.error(f"程序执行失败: {e}")
        raise


if __name__ == "__main__":
    main() 