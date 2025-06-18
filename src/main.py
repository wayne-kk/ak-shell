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
from collectors import (
    stock_basic_collector,
    daily_quote_collector,
    index_data_collector,
    trade_calendar_collector
)


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


def collect_historical_data(start_date: str, end_date: str = None):
    """采集历史数据"""
    logger.info("=" * 60)
    logger.info("开始采集历史数据")
    logger.info("=" * 60)
    
    if end_date is None:
        end_date = datetime.now().strftime('%Y%m%d')
    
    logger.info(f"采集时间范围: {start_date} ~ {end_date}")
    
    # 1. 采集交易日历
    logger.info("\n1. 采集交易日历...")
    trade_calendar_collector.collect()
    
    # 2. 采集股票基础信息
    logger.info("\n2. 采集股票基础信息...")
    stock_basic_collector.collect()
    
    # 3. 采集指数数据
    logger.info("\n3. 采集指数历史数据...")
    index_data_collector.collect_all_indexes_history(start_date, end_date)
    
    # 4. 采集股票历史行情数据
    logger.info("\n4. 采集股票历史行情数据...")
    daily_quote_collector.collect_all_stocks_history(start_date, end_date)
    
    logger.info("\n" + "=" * 60)
    logger.info("历史数据采集完成")
    logger.info("=" * 60)


def collect_latest_data():
    """采集最新数据（增量更新）"""
    logger.info("=" * 60)
    logger.info("开始采集最新数据")
    logger.info("=" * 60)
    
    # 1. 更新股票基础信息（每周更新）
    logger.info("\n1. 更新股票基础信息...")
    stock_basic_collector.collect()
    
    # 2. 采集最新行情数据
    logger.info("\n2. 采集最新行情数据...")
    daily_quote_collector.collect_latest_quotes()
    
    # 3. 采集最新指数数据
    logger.info("\n3. 采集最新指数数据...")
    today = datetime.now().strftime('%Y%m%d')
    yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')
    index_data_collector.collect_all_indexes_history(yesterday, today)
    
    logger.info("\n" + "=" * 60)
    logger.info("最新数据采集完成")
    logger.info("=" * 60)


def collect_one_year_history():
    """采集近一年历史数据"""
    end_date = datetime.now().strftime('%Y%m%d')
    start_date = (datetime.now() - timedelta(days=365)).strftime('%Y%m%d')
    
    logger.info(f"采集近一年历史数据: {start_date} ~ {end_date}")
    collect_historical_data(start_date, end_date)


def collect_stock_data(stock_code: str, start_date: str, end_date: str = None):
    """采集单个股票数据"""
    logger.info(f"采集股票 {stock_code} 的数据")
    
    if end_date is None:
        end_date = datetime.now().strftime('%Y%m%d')
    
    success = daily_quote_collector.collect_stock_history(stock_code, start_date, end_date)
    
    if success:
        logger.info(f"股票 {stock_code} 数据采集完成")
    else:
        logger.error(f"股票 {stock_code} 数据采集失败")


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='A股数据采集工具')
    parser.add_argument('action', choices=[
        'history',      # 采集历史数据
        'latest',       # 采集最新数据
        'one_year',     # 采集近一年数据
        'stock'         # 采集单个股票数据
    ], help='操作类型')
    
    parser.add_argument('--start-date', type=str, help='开始日期 (YYYYMMDD)')
    parser.add_argument('--end-date', type=str, help='结束日期 (YYYYMMDD)')
    parser.add_argument('--stock-code', type=str, help='股票代码')
    
    args = parser.parse_args()
    
    # 设置日志
    setup_logging()
    
    try:
        if args.action == 'history':
            if not args.start_date:
                logger.error("采集历史数据需要指定开始日期 --start-date")
                return
            collect_historical_data(args.start_date, args.end_date)
            
        elif args.action == 'latest':
            collect_latest_data()
            
        elif args.action == 'one_year':
            collect_one_year_history()
            
        elif args.action == 'stock':
            if not args.stock_code or not args.start_date:
                logger.error("采集股票数据需要指定股票代码 --stock-code 和开始日期 --start-date")
                return
            collect_stock_data(args.stock_code, args.start_date, args.end_date)
            
    except KeyboardInterrupt:
        logger.info("用户中断操作")
    except Exception as e:
        logger.error(f"程序执行失败: {e}")
        raise


if __name__ == "__main__":
    main() 