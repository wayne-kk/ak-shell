#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据采集器模块
"""

import akshare as ak
import pandas as pd
from datetime import datetime, timedelta
from typing import Optional, List, Dict
from loguru import logger
import time
from retry import retry
from database import db


class BaseCollector:
    """基础采集器"""
    
    def __init__(self):
        self.retry_count = 3
        self.retry_delay = 5
    
    @retry(tries=3, delay=5)
    def safe_request(self, func, *args, **kwargs):
        """安全的API请求，带重试机制"""
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logger.error(f"API请求失败: {e}")
            raise
    
    def clean_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """清理DataFrame数据"""
        if df.empty:
            return df
        
        # 替换无穷大值
        df = df.replace([float('inf'), float('-inf')], None)
        
        # 清理字符串列的空格
        for col in df.select_dtypes(include=['object']).columns:
            df[col] = df[col].astype(str).str.strip()
            df[col] = df[col].replace(['nan', 'None', ''], None)
        
        return df


class StockBasicCollector(BaseCollector):
    """股票基础信息采集器"""
    
    def collect(self) -> bool:
        """采集股票基础信息"""
        logger.info("开始采集股票基础信息...")
        
        try:
            # 获取A股股票基础信息
            stock_basic = self.safe_request(ak.stock_info_a_code_name)
            
            if stock_basic.empty:
                logger.warning("股票基础信息为空")
                return False
            
            # 重命名列
            stock_basic = stock_basic.rename(columns={
                'code': 'stock_code',
                'name': 'stock_name'
            })
            
            # 添加更新时间
            stock_basic['update_time'] = datetime.now()
            
            # 清理数据
            stock_basic = self.clean_dataframe(stock_basic)
            
            # 插入数据库
            success = db.upsert_dataframe(
                stock_basic, 
                'stock_basic', 
                ['stock_code']
            )
            
            if success:
                logger.info(f"成功采集股票基础信息 {len(stock_basic)} 条")
            
            return success
            
        except Exception as e:
            logger.error(f"采集股票基础信息失败: {e}")
            return False


class DailyQuoteCollector(BaseCollector):
    """日线行情采集器"""
    
    def collect_stock_history(self, stock_code: str, start_date: str, 
                            end_date: Optional[str] = None) -> bool:
        """采集单个股票的历史数据"""
        try:
            if end_date is None:
                end_date = datetime.now().strftime('%Y%m%d')
            
            logger.info(f"采集股票 {stock_code} 从 {start_date} 到 {end_date} 的行情数据")
            
            # 获取股票历史数据
            df = self.safe_request(
                ak.stock_zh_a_hist,
                symbol=stock_code,
                period="daily",
                start_date=start_date,
                end_date=end_date,
                adjust=""
            )
            
            if df.empty:
                logger.warning(f"股票 {stock_code} 无历史数据")
                return True
            
            # 重命名列
            column_mapping = {
                '日期': 'trade_date',
                '开盘': 'open',
                '最高': 'high',
                '最低': 'low',
                '收盘': 'close',
                '涨跌额': 'change',
                '涨跌幅': 'pct_chg',
                '成交量': 'volume',
                '成交额': 'amount',
                '换手率': 'turnover_rate'
            }
            
            df = df.rename(columns=column_mapping)
            df['stock_code'] = stock_code
            df['update_time'] = datetime.now()
            
            # 数据类型转换
            if 'trade_date' in df.columns:
                df['trade_date'] = pd.to_datetime(df['trade_date']).dt.date
            
            # 清理数据
            df = self.clean_dataframe(df)
            
            # 插入数据库
            success = db.upsert_dataframe(
                df, 
                'daily_quote', 
                ['stock_code', 'trade_date']
            )
            
            if success:
                logger.info(f"成功采集股票 {stock_code} 行情数据 {len(df)} 条")
            
            # 添加延迟避免API限制
            time.sleep(0.1)
            
            return success
            
        except Exception as e:
            logger.error(f"采集股票 {stock_code} 行情数据失败: {e}")
            return False
    
    def collect_all_stocks_history(self, start_date: str, 
                                 end_date: Optional[str] = None) -> bool:
        """采集所有股票的历史数据"""
        logger.info("开始采集所有股票历史行情数据...")
        
        # 获取股票列表
        stock_list = db.get_stock_list()
        if not stock_list:
            logger.error("无法获取股票列表")
            return False
        
        success_count = 0
        total_count = len(stock_list)
        
        for i, stock_code in enumerate(stock_list, 1):
            logger.info(f"处理进度: {i}/{total_count} - {stock_code}")
            
            if self.collect_stock_history(stock_code, start_date, end_date):
                success_count += 1
            
            # 每100只股票打印一次进度
            if i % 100 == 0:
                logger.info(f"已处理 {i}/{total_count} 只股票，成功 {success_count} 只")
        
        logger.info(f"历史数据采集完成: 总计 {total_count} 只股票，成功 {success_count} 只")
        return success_count > 0
    
    def collect_latest_quotes(self) -> bool:
        """采集最新行情数据"""
        logger.info("开始采集最新行情数据...")
        
        # 获取股票列表
        stock_list = db.get_stock_list()
        if not stock_list:
            logger.error("无法获取股票列表")
            return False
        
        today = datetime.now().strftime('%Y%m%d')
        success_count = 0
        
        for stock_code in stock_list:
            # 获取该股票最新日期
            latest_date = db.get_latest_date('daily_quote', 'trade_date', stock_code)
            
            # 如果有最新日期，从下一天开始采集
            if latest_date:
                start_date = (datetime.strptime(latest_date, '%Y%m%d') + timedelta(days=1)).strftime('%Y%m%d')
            else:
                # 如果没有历史数据，采集最近30天
                start_date = (datetime.now() - timedelta(days=30)).strftime('%Y%m%d')
            
            if start_date <= today:
                if self.collect_stock_history(stock_code, start_date, today):
                    success_count += 1
        
        logger.info(f"最新行情采集完成: 成功更新 {success_count} 只股票")
        return success_count > 0


class IndexDataCollector(BaseCollector):
    """指数数据采集器"""
    
    def __init__(self):
        super().__init__()
        self.index_list = [
            ('sh000001', '上证指数'),
            ('sz399001', '深证成指'),
            ('sz399006', '创业板指'),
            ('sh000300', '沪深300'),
            ('sh000905', '中证500')
        ]
    
    def collect_index_history(self, index_code: str, index_name: str,
                            start_date: str, end_date: Optional[str] = None) -> bool:
        """采集指数历史数据"""
        try:
            if end_date is None:
                end_date = datetime.now().strftime('%Y%m%d')
            
            logger.info(f"采集指数 {index_code}({index_name}) 从 {start_date} 到 {end_date} 的数据")
            
            # 获取指数数据
            df = self.safe_request(
                ak.stock_zh_index_daily,
                symbol=index_code
            )
            
            if df.empty:
                logger.warning(f"指数 {index_code} 无历史数据")
                return True
            
            # 过滤日期范围
            df['date'] = pd.to_datetime(df['date'])
            start_dt = datetime.strptime(start_date, '%Y%m%d')
            end_dt = datetime.strptime(end_date, '%Y%m%d')
            df = df[(df['date'] >= start_dt) & (df['date'] <= end_dt)]
            
            if df.empty:
                logger.info(f"指数 {index_code} 在指定日期范围内无数据")
                return True
            
            # 重命名和添加列
            df = df.rename(columns={'date': 'trade_date'})
            df['index_code'] = index_code
            df['index_name'] = index_name
            df['trade_date'] = df['trade_date'].dt.date
            df['update_time'] = datetime.now()
            
            # 计算涨跌额和涨跌幅
            df = df.sort_values('trade_date')
            df['change'] = df['close'].diff()
            df['pct_chg'] = df['close'].pct_change() * 100
            
            # 清理数据
            df = self.clean_dataframe(df)
            
            # 插入数据库
            success = db.upsert_dataframe(
                df,
                'index_data',
                ['index_code', 'trade_date']
            )
            
            if success:
                logger.info(f"成功采集指数 {index_code} 数据 {len(df)} 条")
            
            time.sleep(0.1)
            return success
            
        except Exception as e:
            logger.error(f"采集指数 {index_code} 数据失败: {e}")
            return False
    
    def collect_all_indexes_history(self, start_date: str, 
                                  end_date: Optional[str] = None) -> bool:
        """采集所有指数历史数据"""
        logger.info("开始采集指数历史数据...")
        
        success_count = 0
        for index_code, index_name in self.index_list:
            if self.collect_index_history(index_code, index_name, start_date, end_date):
                success_count += 1
        
        logger.info(f"指数历史数据采集完成: 成功 {success_count}/{len(self.index_list)} 个指数")
        return success_count > 0


class TradeCalendarCollector(BaseCollector):
    """交易日历采集器"""
    
    def collect(self, start_year: Optional[int] = None) -> bool:
        """采集交易日历"""
        logger.info("开始采集交易日历...")
        
        try:
            if start_year is None:
                start_year = datetime.now().year - 5
            
            # 获取交易日历
            trade_dates = self.safe_request(ak.tool_trade_date_hist_sina)
            
            if trade_dates.empty:
                logger.warning("交易日历数据为空")
                return False
            
            # 过滤年份
            trade_dates['trade_date'] = pd.to_datetime(trade_dates['trade_date'])
            trade_dates = trade_dates[trade_dates['trade_date'].dt.year >= start_year]
            
            # 重命名列
            trade_dates = trade_dates.rename(columns={'trade_date': 'calendar_date'})
            trade_dates['calendar_date'] = trade_dates['calendar_date'].dt.date
            trade_dates['is_trade_day'] = True
            trade_dates['week_day'] = pd.to_datetime(trade_dates['calendar_date']).dt.dayofweek
            trade_dates['is_holiday'] = False
            trade_dates['update_time'] = datetime.now()
            
            # 清理数据
            trade_dates = self.clean_dataframe(trade_dates)
            
            # 插入数据库
            success = db.upsert_dataframe(
                trade_dates,
                'trade_calendar',
                ['calendar_date']
            )
            
            if success:
                logger.info(f"成功采集交易日历 {len(trade_dates)} 条")
            
            return success
            
        except Exception as e:
            logger.error(f"采集交易日历失败: {e}")
            return False


# 采集器实例
stock_basic_collector = StockBasicCollector()
daily_quote_collector = DailyQuoteCollector()
index_data_collector = IndexDataCollector()
trade_calendar_collector = TradeCalendarCollector() 