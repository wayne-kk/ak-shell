#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
A股数据采集系统统一启动入口
"""

import sys
import argparse
from pathlib import Path

# 添加项目路径
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(project_root / 'src'))

from src.config import config
from src.utils import setup_logger, cleanup_old_files


def main():
    """主入口函数"""
    parser = argparse.ArgumentParser(description='A股数据采集系统')
    subparsers = parser.add_subparsers(dest='command', help='可用命令')
    
    # 股票数据采集
    stock_parser = subparsers.add_parser('stock', help='股票数据采集')
    stock_parser.add_argument('action', choices=['today', 'history', 'stocks'], 
                             help='today-采集当日完整数据, history-采集历史数据, stocks-仅采集股票数据')
    stock_parser.add_argument('--start-date', help='开始日期 (YYYYMMDD)')
    stock_parser.add_argument('--end-date', help='结束日期 (YYYYMMDD)')
    
    # 新闻采集
    news_parser = subparsers.add_parser('news', help='新闻采集')
    news_parser.add_argument('action', choices=['collect', 'stats'], 
                            help='collect-采集新闻, stats-查看统计')
    
    # 系统管理
    system_parser = subparsers.add_parser('system', help='系统管理')
    system_parser.add_argument('action', choices=['config', 'logs', 'clean'], 
                              help='config-查看配置, logs-查看日志, clean-清理日志')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return
    
    # 验证配置和初始化
    try:
        config.validate_config()
        config.ensure_directories()
    except ValueError as e:
        print(f"❌ 配置错误: {e}")
        return
    
    # 执行命令
    if args.command == 'stock':
        execute_stock_command(args)
    elif args.command == 'news':
        execute_news_command(args)
    elif args.command == 'system':
        execute_system_command(args)


def execute_stock_command(args):
    """执行股票数据采集命令"""
    logger = setup_logger("stock_main")
    logger.info(f"开始执行股票数据采集: {args.action}")
    
    # 导入并执行股票采集
    from src.main import collect_today_data, collect_historical_data
    
    if args.action == 'today':
        collect_today_data()
   
    elif args.action == 'history':
        if not args.start_date:
            print("❌ 历史数据采集需要指定 --start-date")
            return
        end_date = args.end_date or ''
        collect_historical_data(args.start_date, end_date)


def execute_news_command(args):
    """执行新闻采集命令"""
    logger = setup_logger("news_main")
    logger.info(f"开始执行新闻采集: {args.action}")
    
    # 导入新闻采集器
    from src.collectors import StockNewsCollector
    
    if args.action == 'collect':
        collector = StockNewsCollector()
        collector.collect_news()
    elif args.action == 'stats':
        collector = StockNewsCollector()
        stats = collector.get_news_stats()
        print(f"📰 新闻统计: {stats}")


def execute_system_command(args):
    """执行系统管理命令"""
    if args.action == 'config':
        show_config()
    elif args.action == 'logs':
        show_logs()
    elif args.action == 'clean':
        clean_logs()


def show_config():
    """显示系统配置"""
    print("\n=== 系统配置信息 ===")
    print(f"项目根目录: {config.PROJECT_ROOT}")
    print(f"数据库URL: {config.SUPABASE_URL}")
    print(f"飞书通知: {'✅ 已启用' if config.FEISHU_ENABLED else '❌ 未启用'}")
    print(f"日志级别: {config.LOG_LEVEL}")
    print(f"新闻采集间隔: {config.NEWS_COLLECTION_INTERVAL}分钟")
    print(f"日志保留天数: {config.LOG_RETENTION_DAYS}天")
    print("=" * 40)


def show_logs():
    """显示最近日志"""
    import subprocess
    log_file = config.LOGS_DIR / "cron_news.log"
    if log_file.exists():
        print(f"\n=== 最近新闻采集日志 ===")
        subprocess.run(['tail', '-20', str(log_file)])
    else:
        print("📝 暂无新闻采集日志文件")
    
    # 显示其他日志文件
    log_files = list(config.LOGS_DIR.glob("*.log"))
    if log_files:
        print(f"\n📁 可用日志文件: {len(log_files)}个")
        for log_file in sorted(log_files)[-5:]:  # 显示最新5个
            print(f"  - {log_file.name}")


def clean_logs():
    """清理旧日志"""
    count = cleanup_old_files(config.LOGS_DIR, days=config.LOG_RETENTION_DAYS, pattern="*.log")
    print(f"🗑️ 清理了 {count} 个旧日志文件 (超过{config.LOG_RETENTION_DAYS}天)")


if __name__ == "__main__":
    main() 