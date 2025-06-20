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
import random


class BaseCollector:
    """基础采集器"""
    
    def __init__(self):
        self.retry_count = 3
        self.retry_delay = 5
        # 延时控制配置 - 更保守的设置
        self.base_delay = 0.5   # 基础延时增加到0.5秒
        self.random_delay = 0.8  # 随机延时增加到0.8秒
        self.batch_delay = 10.0   # 批次间延时增加到10秒
        self.batch_size = 50     # 批次大小减少到50
    
    def set_delay_config(self, base_delay: float = 0.2, random_delay: float = 0.3, 
                        batch_delay: float = 2.0, batch_size: int = 100):
        """设置延时配置"""
        self.base_delay = base_delay
        self.random_delay = random_delay
        self.batch_delay = batch_delay
        self.batch_size = batch_size
        logger.info(f"延时配置已更新: 基础延时={base_delay}s, 随机延时={random_delay}s, 批次延时={batch_delay}s, 批次大小={batch_size}")
    
    def smart_delay(self, index: int = 0):
        """智能延时控制"""
        # 基础延时 + 随机延时
        delay = self.base_delay + random.uniform(0, self.random_delay)
        time.sleep(delay)
        
        # 每批次后增加额外延时
        if index > 0 and index % self.batch_size == 0:
            logger.info(f"批次延时: {self.batch_delay}s")
            time.sleep(self.batch_delay)
    
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
            # 获取更全面的A股股票信息（东财数据源）
            logger.info("正在从东财获取股票列表...")
            stock_em = self.safe_request(ak.stock_zh_a_spot_em)
            
            if stock_em.empty:
                logger.warning("东财股票信息为空，尝试备用数据源...")
                # 备用数据源
                stock_basic = self.safe_request(ak.stock_info_a_code_name)
                if stock_basic.empty:
                    logger.error("所有股票数据源都为空")
                    return False
                
                # 重命名列
                stock_basic = stock_basic.rename(columns={
                    'code': 'stock_code',
                    'name': 'stock_name'
                })
                # 添加缺失字段
                stock_basic['exchange'] = stock_basic['stock_code'].apply(self._get_market_by_code)
                stock_basic['update_time'] = datetime.now()
                
            else:
                # 使用东财数据，映射到模型字段
                logger.info(f"获取到 {len(stock_em)} 只股票的信息")
                
                # 字段映射
                column_mapping = {
                    '代码': 'stock_code',
                    '名称': 'stock_name',
                    '总市值': 'total_share',  # 用总市值近似代替总股本
                    '流通市值': 'float_share',  # 用流通市值近似代替流通股本
                    '市盈率-动态': 'pe_ratio',  # 临时字段，用于判断ST等
                    '市净率': 'pb_ratio'  # 临时字段
                }
                
                # 创建基础信息DataFrame
                stock_basic = pd.DataFrame()
                for old_col, new_col in column_mapping.items():
                    if old_col in stock_em.columns:
                        stock_basic[new_col] = stock_em[old_col]
                
                # 添加派生字段
                if 'stock_code' in stock_basic.columns:
                    # 交易所判断
                    stock_basic['exchange'] = stock_basic['stock_code'].apply(self._get_market_by_code)
                    
                    # ST判断（通过股票名称判断）
                    if 'stock_name' in stock_basic.columns:
                        stock_basic['is_st'] = stock_basic['stock_name'].str.contains('ST|st', regex=True, na=False)
                    
                    # 状态字段（简单设为正常）
                    stock_basic['status'] = '正常'
                
                # 移除临时字段
                temp_fields = ['pe_ratio', 'pb_ratio']
                for field in temp_fields:
                    if field in stock_basic.columns:
                        stock_basic = stock_basic.drop(columns=[field])
                
                # 添加更新时间
                stock_basic['update_time'] = datetime.now()
            
            # 数据类型转换和清理
            if 'total_share' in stock_basic.columns:
                stock_basic['total_share'] = pd.to_numeric(stock_basic['total_share'], errors='coerce')
            if 'float_share' in stock_basic.columns:
                stock_basic['float_share'] = pd.to_numeric(stock_basic['float_share'], errors='coerce')
            
            # 清理数据
            stock_basic = self.clean_dataframe(stock_basic)
            
            # 移除空值行
            stock_basic = stock_basic.dropna(subset=['stock_code'])
            
            logger.info(f"准备插入 {len(stock_basic)} 条股票基础信息")
            logger.info(f"数据列: {stock_basic.columns.tolist()}")
            
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
    
    def _get_market_by_code(self, stock_code: str) -> str:
        """根据股票代码判断所属市场"""
        if not stock_code:
            return '未知'
        
        code = str(stock_code)
        if code.startswith('0') or code.startswith('2') or code.startswith('3'):
            return '深交所'
        elif code.startswith('6') or code.startswith('9'):
            return '上交所'
        elif code.startswith('8'):
            return '北交所'
        else:
            return '其他'


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
                '股票代码': 'stock_code',
                '开盘': 'open',
                '最高': 'high',
                '最低': 'low',
                '收盘': 'close',
                '涨跌额': 'change',
                '涨跌幅': 'pct_chg',
                '成交量': 'volume',
                '成交额': 'amount',
                '振幅': 'amplitude',
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
            
            # 延时控制在调用方处理
            
            return success
            
        except Exception as e:
            logger.error(f"采集股票 {stock_code} 行情数据失败: {e}")
            return False
    
    def collect_all_stocks_history(self, start_date: str, 
                                 end_date: Optional[str] = None, 
                                 enable_resume: bool = True) -> bool:
        """采集所有股票的历史数据，支持断点续传"""
        logger.info("开始采集所有股票历史行情数据...")
        
        # 获取股票列表
        stock_list = db.get_stock_list()
        if not stock_list:
            logger.error("无法获取股票列表")
            return False
        
        success_count = 0
        skip_count = 0
        total_count = len(stock_list)
        logger.info(f"股票列表数量: {total_count}")
        for i, stock_code in enumerate(stock_list, 1):
            logger.info(f"处理进度: {i}/{total_count} - {stock_code}")
            
            # 断点续传检查
            if enable_resume:
                latest_date = db.get_latest_date('daily_quote', 'trade_date', stock_code)
                if latest_date:
                    # 检查是否已经有完整的数据
                    if end_date and latest_date >= end_date:
                        logger.info(f"股票 {stock_code} 数据已存在，跳过")
                        skip_count += 1
                        continue
                    # 从最新日期的下一天开始采集
                    resume_start_date = (datetime.strptime(latest_date, '%Y%m%d') + timedelta(days=1)).strftime('%Y%m%d')
                    if resume_start_date > start_date:
                        logger.info(f"股票 {stock_code} 从 {resume_start_date} 开始续传")
                        start_date_for_stock = resume_start_date
                    else:
                        start_date_for_stock = start_date
                else:
                    start_date_for_stock = start_date
            else:
                start_date_for_stock = start_date
            
            # 采集数据
            if self.collect_stock_history(stock_code, start_date_for_stock, end_date):
                success_count += 1
            
            # 智能延时控制
            self.smart_delay(i)
            
            # 每100只股票打印一次进度
            if i % 100 == 0:
                logger.info(f"已处理 {i}/{total_count} 只股票，成功 {success_count} 只，跳过 {skip_count} 只")
        
        logger.info(f"历史数据采集完成: 总计 {total_count} 只股票，成功 {success_count} 只，跳过 {skip_count} 只")
        return success_count > 0
    
    def collect_latest_quotes_batch(self) -> bool:
        """批量采集最新行情数据（避免频繁API请求）"""
        logger.info("开始批量采集最新行情数据...")
        
        try:
            # 使用更稳定的批量接口获取所有股票最新数据
            logger.info("正在获取所有股票最新行情...")
            df = self.safe_request(ak.stock_zh_a_spot_em)
            
            if df.empty:
                logger.warning("批量行情数据为空")
                return False
            
            logger.info(f"获取到 {len(df)} 只股票的最新行情")
            
            # 只选择数据库中存在的核心行情字段
            basic_columns = {
                '代码': 'stock_code',
                '最新价': 'close',
                '涨跌额': 'change',
                '涨跌幅': 'pct_chg',
                '今开': 'open',
                '最高': 'high',
                '最低': 'low',
                '成交量': 'volume',
                '成交额': 'amount'
            }
            
            # 创建新的DataFrame，只包含核心行情字段
            processed_data = {}
            for old_col, new_col in basic_columns.items():
                if old_col in df.columns:
                    processed_data[new_col] = df[old_col]
                else:
                    processed_data[new_col] = None
            
            # 创建新的DataFrame
            df_clean = pd.DataFrame(processed_data)
            
            # 添加交易日期（使用当前日期）
            today = datetime.now().date()
            df_clean['trade_date'] = today
            df_clean['update_time'] = datetime.now()
            
            # 清理数据
            df_clean = self.clean_dataframe(df_clean)
            
            # 数据类型转换
            try:
                # 转换数值类型字段
                numeric_fields = ['close', 'change', 'pct_chg', 'open', 'high', 'low', 'amount']
                for field in numeric_fields:
                    if field in df_clean.columns:
                        df_clean[field] = pd.to_numeric(df_clean[field], errors='coerce')
                
                # 成交量转换为整数
                if 'volume' in df_clean.columns:
                    df_clean['volume'] = pd.to_numeric(df_clean['volume'], errors='coerce')
                    df_clean['volume'] = df_clean['volume'].fillna(0).astype('int64')
                
            except Exception as e:
                logger.warning(f"数据类型转换时出现警告: {e}")
            
            # 移除空值行
            df_clean = df_clean.dropna(subset=['stock_code'])
            
            logger.info(f"准备插入 {len(df_clean)} 条最新行情数据")
            logger.info(f"数据列: {df_clean.columns.tolist()}")
            
            # 插入数据库
            success = db.upsert_dataframe(
                df_clean, 
                'daily_quote', 
                ['stock_code', 'trade_date']
            )
            
            if success:
                logger.info(f"成功批量采集最新行情数据 {len(df_clean)} 条")
                logger.info("🚀 批量采集避免了数千次单独API请求！")
            
            return success
            
        except Exception as e:
            logger.error(f"批量采集最新行情数据失败: {e}")
            return False

    def collect_latest_quotes(self) -> bool:
        """采集最新行情数据"""
        logger.info("开始采集最新行情数据...")
        
        # 使用重试机制进行批量采集
        max_retries = 3
        retry_delay = 300  # 5分钟 = 300秒
        
        for attempt in range(max_retries):
            logger.info(f"批量采集尝试 {attempt + 1}/{max_retries}")
            
            batch_success = self.collect_latest_quotes_batch()
            if batch_success:
                logger.info("批量采集成功")
                return True
            
            if attempt < max_retries - 1:  # 不是最后一次尝试
                logger.warning(f"批量采集失败，{retry_delay//60}分钟后进行第{attempt + 2}次重试...")
                time.sleep(retry_delay)
            else:
                logger.error(f"批量采集失败，已达到最大重试次数({max_retries})，本次采集结束")
        
        return False


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
    

    
    def collect_all_indexes_history(self, start_date: str, 
                                  end_date: Optional[str] = None,
                                  retry_delay_hours: int = 1) -> bool:
        """采集所有指数历史数据
        
        Args:
            start_date: 开始日期
            end_date: 结束日期
            retry_delay_hours: 当数据源未更新时的重试延迟小时数
        """
        logger.info("开始采集指数历史数据...")
        
        success_count = 0
        has_data_count = 0  # 实际有数据的指数数量
        
        for i, (index_code, index_name) in enumerate(self.index_list, 1):
            result = self.collect_index_history_with_data_check(index_code, index_name, start_date, end_date)
            if result['success']:
                success_count += 1
            if result['has_data']:
                has_data_count += 1
            # 指数数据采集间隔延时
            if i < len(self.index_list):  # 最后一个不需要延时
                time.sleep(1.0)
        
        # 检查是否所有指数都没有数据（可能数据源未更新）
        if has_data_count == 0 and success_count == len(self.index_list):
            logger.warning(f"所有指数在 {start_date} 都没有数据，可能数据源未更新")
            
            # 如果是当日数据且时间还早，建议延迟重试
            current_hour = datetime.now().hour
            if start_date == datetime.now().strftime('%Y%m%d') and current_hour < 20:
                logger.info(f"建议 {retry_delay_hours} 小时后重试，当前时间: {datetime.now().strftime('%H:%M')}")
                logger.info(f"可在 {(datetime.now() + timedelta(hours=retry_delay_hours)).strftime('%H:%M')} 后重新执行")
                
                # 可以选择立即重试一次（等待1小时）
                if self._should_retry_index_collection():
                    logger.info("启动自动重试机制...")
                    return self._retry_index_collection_later(start_date, end_date, retry_delay_hours)
        
        logger.info(f"指数历史数据采集完成: 成功 {success_count}/{len(self.index_list)} 个指数，有数据 {has_data_count} 个")
        return success_count > 0

    def collect_index_history_with_data_check(self, index_code: str, index_name: str,
                                            start_date: str, end_date: Optional[str] = None) -> dict:
        """采集指数历史数据并检查是否有数据"""
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
                return {'success': True, 'has_data': False}
            
            # 过滤日期范围
            df['date'] = pd.to_datetime(df['date'])
            start_dt = datetime.strptime(start_date, '%Y%m%d')
            end_dt = datetime.strptime(end_date, '%Y%m%d')
            df = df[(df['date'] >= start_dt) & (df['date'] <= end_dt)]
            
            if df.empty:
                logger.info(f"指数 {index_code} 在指定日期范围内无数据")
                return {'success': True, 'has_data': False}
            
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
            
            return {'success': success, 'has_data': True}
            
        except Exception as e:
            logger.error(f"采集指数 {index_code} 数据失败: {e}")
            return {'success': False, 'has_data': False}

    def _should_retry_index_collection(self) -> bool:
        """判断是否应该重试指数数据采集"""
        current_time = datetime.now()
        current_hour = current_time.hour
        
        # 只在交易日的特定时间段内重试
        if current_time.weekday() >= 5:  # 周末不重试
            return False
        
        # 在18:00-21:00之间可以重试
        if 18 <= current_hour <= 21:
            return True
        
        return False

    def _retry_index_collection_later(self, start_date: str, end_date: Optional[str], 
                                    delay_hours: int) -> bool:
        """延迟重试指数数据采集"""
        import threading
        import time as time_module
        
        def delayed_retry():
            logger.info(f"等待 {delay_hours} 小时后重试指数数据采集...")
            time_module.sleep(delay_hours * 3600)  # 转换为秒
            
            logger.info("开始重试指数数据采集...")
            retry_result = self.collect_all_indexes_history(start_date, end_date, retry_delay_hours=0)
            
            if retry_result:
                logger.info("重试成功：指数数据采集完成")
                # 发送成功通知
                try:
                    from feishu_notify import send_completion_notice
                    send_completion_notice(
                        "指数数据重试采集",
                        True,
                        {
                            "重试时间": datetime.now().strftime('%H:%M:%S'),
                            "采集日期": start_date,
                            "状态": "✅ 重试成功"
                        }
                    )
                except:
                    pass
            else:
                logger.warning("重试失败：指数数据仍然无法获取")
        
        # 在后台线程中执行延迟重试
        retry_thread = threading.Thread(target=delayed_retry, daemon=True)
        retry_thread.start()
        
        logger.info(f"已启动后台重试任务，将在 {delay_hours} 小时后自动重试")
        return True  # 返回True表示已安排重试


class TradeCalendarCollector(BaseCollector):
    """交易日历采集器"""
    
    def collect(self, start_year: Optional[int] = None, end_year: Optional[int] = None) -> bool:
        """采集完整交易日历（只区分交易日和非交易日）"""
        logger.info("开始采集完整交易日历...")
        
        try:
            if start_year is None:
                start_year = datetime.now().year - 2
            if end_year is None:
                end_year = datetime.now().year + 1
            
            logger.info(f"采集年份范围: {start_year} - {end_year}")
            
            # 1. 获取交易日数据
            trade_dates_df = self.safe_request(ak.tool_trade_date_hist_sina)
            
            if trade_dates_df.empty:
                logger.warning("交易日历数据为空")
                return False
            
            # 转换交易日期格式并创建集合用于快速查找
            trade_dates_df['trade_date'] = pd.to_datetime(trade_dates_df['trade_date'])
            trade_dates_set = set(trade_dates_df['trade_date'].dt.date)
            
            logger.info(f"获取到 {len(trade_dates_set)} 个交易日")
            
            # 2. 生成完整日历范围
            start_date = datetime(start_year, 1, 1).date()
            end_date = datetime(end_year, 12, 31).date()
            
            calendar_data = []
            current_date = start_date
            
            logger.info(f"生成日期范围: {start_date} 到 {end_date}")
            
            while current_date <= end_date:
                # 判断是否为交易日
                is_trade_day = current_date in trade_dates_set
                
                calendar_data.append({
                    'calendar_date': current_date,
                    'is_trade_day': is_trade_day,
                    'update_time': datetime.now()
                })
                
                current_date += timedelta(days=1)
            
            # 3. 转换为DataFrame
            calendar_df = pd.DataFrame(calendar_data)
            
            # 统计信息
            total_days = len(calendar_df)
            trade_days = sum(calendar_df['is_trade_day'])
            non_trade_days = total_days - trade_days
            
            logger.info(f"生成完整日历数据 {total_days} 条")
            logger.info(f"其中交易日 {trade_days} 条")
            logger.info(f"非交易日 {non_trade_days} 条")
            
            # 4. 插入数据库
            success = db.upsert_dataframe(
                calendar_df,
                'trade_calendar',
                ['calendar_date']
            )
            
            if success:
                logger.info(f"✅ 成功采集完整交易日历 {len(calendar_df)} 条")
                
                # 显示一些示例数据
                logger.info("示例数据预览:")
                sample_data = calendar_df.head(10)
                for _, row in sample_data.iterrows():
                    status = "交易日" if bool(row['is_trade_day']) else "非交易日"
                    logger.info(f"  {row['calendar_date']} - {status}")
            
            return success
            
        except Exception as e:
            logger.error(f"❌ 采集交易日历失败: {e}")
            return False


class StockHotRankCollector(BaseCollector):
    """股票人气榜采集器"""
    
    def collect_hot_rank(self) -> bool:
        """采集股票人气榜数据"""
        logger.info("开始采集股票人气榜数据...")
        
        try:
            # 获取人气榜数据
            df = self.safe_request(ak.stock_hot_rank_em)
            
            if df.empty:
                logger.warning("人气榜数据为空")
                return False
            
            logger.info(f"获取到 {len(df)} 条人气榜数据")
            
            # 字段映射
            column_mapping = {
                '当前排名': 'current_rank',
                '代码': 'stock_code',
                '股票名称': 'stock_name',
                '最新价': 'latest_price',
                '涨跌额': 'change',
                '涨跌幅': 'pct_chg'
            }
            
            # 重命名列
            df = df.rename(columns=column_mapping)
            
            # 添加交易日期和更新时间
            today = datetime.now().date()
            df['trade_date'] = today
            df['update_time'] = datetime.now()
            
            # 数据类型转换
            numeric_fields = ['current_rank', 'latest_price', 'change', 'pct_chg']
            for field in numeric_fields:
                if field in df.columns:
                    df[field] = pd.to_numeric(df[field], errors='coerce')
            
            # 清理数据
            df = self.clean_dataframe(df)
            
            # 移除空值行
            df = df.dropna(subset=['stock_code', 'current_rank'])
            
            logger.info(f"准备插入 {len(df)} 条人气榜数据")
            logger.info(f"数据列: {df.columns.tolist()}")
            
            # 插入数据库
            success = db.upsert_dataframe(
                df,
                'stock_hot_rank',
                ['trade_date', 'current_rank']
            )
            
            if success:
                logger.info(f"✅ 成功采集股票人气榜数据 {len(df)} 条")
            
            return success
            
        except Exception as e:
            logger.error(f"❌ 采集股票人气榜数据失败: {e}")
            return False


class StockHotUpCollector(BaseCollector):
    """股票飙升榜采集器"""
    
    def collect_hot_up(self) -> bool:
        """采集股票飙升榜数据"""
        logger.info("开始采集股票飙升榜数据...")
        
        try:
            # 获取飙升榜数据
            df = self.safe_request(ak.stock_hot_up_em)
            
            if df.empty:
                logger.warning("飙升榜数据为空")
                return False
            
            logger.info(f"获取到 {len(df)} 条飙升榜数据")
            
            # 字段映射
            column_mapping = {
                '排名较昨日变动': 'rank_change',
                '当前排名': 'current_rank',
                '代码': 'stock_code',
                '股票名称': 'stock_name',
                '最新价': 'latest_price',
                '涨跌额': 'change',
                '涨跌幅': 'pct_chg'
            }
            
            # 重命名列
            df = df.rename(columns=column_mapping)
            
            # 添加交易日期和更新时间
            today = datetime.now().date()
            df['trade_date'] = today
            df['update_time'] = datetime.now()
            
            # 数据类型转换
            numeric_fields = ['rank_change', 'current_rank', 'latest_price', 'change', 'pct_chg']
            for field in numeric_fields:
                if field in df.columns:
                    df[field] = pd.to_numeric(df[field], errors='coerce')
            
            # 清理数据
            df = self.clean_dataframe(df)
            
            # 移除空值行
            df = df.dropna(subset=['stock_code', 'current_rank'])
            
            logger.info(f"准备插入 {len(df)} 条飙升榜数据")
            logger.info(f"数据列: {df.columns.tolist()}")
            
            # 插入数据库
            success = db.upsert_dataframe(
                df,
                'stock_hot_up',
                ['trade_date', 'current_rank']
            )
            
            if success:
                logger.info(f"✅ 成功采集股票飙升榜数据 {len(df)} 条")
            
            return success
            
        except Exception as e:
            logger.error(f"❌ 采集股票飙升榜数据失败: {e}")
            return False


class HsgtFundFlowCollector(BaseCollector):
    """沪深港通资金流向采集器"""
    
    def collect_hsgt_fund_flow(self) -> bool:
        """采集沪深港通资金流向数据"""
        logger.info("开始采集沪深港通资金流向数据...")
        
        try:
            # 获取沪深港通资金流向数据
            df = self.safe_request(ak.stock_hsgt_fund_flow_summary_em)
            
            if df.empty:
                logger.warning("沪深港通资金流向数据为空")
                return False
            
            logger.info(f"获取到 {len(df)} 条沪深港通资金流向数据")
            
            # 字段映射
            column_mapping = {
                '交易日': 'trade_date',
                '类型': 'type',
                '板块': 'sector',
                '资金方向': 'direction',
                '交易状态': 'trade_status',
                '成交净买额': 'net_buy_amount',
                '资金净流入': 'net_inflow',
                '当日资金余额': 'day_balance',
                '上涨数': 'up_count',
                '持平数': 'flat_count',
                '下跌数': 'down_count',
                '相关指数': 'related_index',
                '指数涨跌幅': 'index_pct_chg'
            }
            
            # 重命名列
            df = df.rename(columns=column_mapping)
            
            # 数据类型转换
            if 'trade_date' in df.columns:
                df['trade_date'] = pd.to_datetime(df['trade_date']).dt.date
            
            # 数值字段转换
            numeric_fields = ['trade_status', 'net_buy_amount', 'net_inflow', 'day_balance', 
                            'up_count', 'flat_count', 'down_count', 'index_pct_chg']
            for field in numeric_fields:
                if field in df.columns:
                    df[field] = pd.to_numeric(df[field], errors='coerce')
            
            # 添加更新时间
            df['update_time'] = datetime.now()
            
            # 清理数据
            df = self.clean_dataframe(df)
            
            # 移除关键字段为空的行
            df = df.dropna(subset=['trade_date', 'type', 'sector', 'direction'])
            
            logger.info(f"准备插入 {len(df)} 条沪深港通资金流向数据")
            logger.info(f"数据列: {df.columns.tolist()}")
            
            # 插入数据库
            success = db.upsert_dataframe(
                df,
                'hsgt_fund_flow',
                ['trade_date', 'type', 'sector', 'direction']
            )
            
            if success:
                logger.info(f"✅ 成功采集沪深港通资金流向数据 {len(df)} 条")
            
            return success
            
        except Exception as e:
            logger.error(f"❌ 采集沪深港通资金流向数据失败: {e}")
            return False


class StockFundFlowRankCollector(BaseCollector):
    """个股资金流排名采集器"""
    
    def collect_fund_flow_rank(self, indicators: Optional[list] = None) -> bool:
        """采集个股资金流排名数据"""
        if indicators is None:
            indicators = ["今日", "3日", "5日", "10日"]
        
        logger.info("开始采集个股资金流排名数据...")
        
        overall_success = True
        today = datetime.now().date()
        
        for indicator in indicators:
            logger.info(f"采集 {indicator} 资金流排名数据...")
            
            try:
                # 获取指定周期的个股资金流排名
                df = self.safe_request(ak.stock_individual_fund_flow_rank, indicator=indicator)
                
                if df.empty:
                    logger.warning(f"{indicator} 个股资金流排名数据为空")
                    overall_success = False
                    continue
                
                logger.info(f"获取到 {len(df)} 条 {indicator} 个股资金流排名数据")
                
                # 字段映射 - 根据不同周期动态调整字段名
                column_mapping = {
                    '序号': 'rank',
                    '代码': 'stock_code',
                    '名称': 'stock_name',
                    '最新价': 'latest_price'
                }
                
                # 根据不同周期设置相应的字段映射
                period_prefix = indicator if indicator != "今日" else "今日"
                
                # 涨跌幅字段
                if f'{period_prefix}涨跌幅' in df.columns:
                    column_mapping[f'{period_prefix}涨跌幅'] = 'pct_chg'
                elif '今日涨跌幅' in df.columns:
                    column_mapping['今日涨跌幅'] = 'pct_chg'
                
                # 资金流向字段映射
                fund_flow_mappings = [
                    (f'{period_prefix}主力净流入-净额', 'main_net_inflow_amount'),
                    (f'{period_prefix}主力净流入-净占比', 'main_net_inflow_rate'),
                    (f'{period_prefix}超大单净流入-净额', 'super_large_net_amount'),
                    (f'{period_prefix}超大单净流入-净占比', 'super_large_net_rate'),
                    (f'{period_prefix}大单净流入-净额', 'large_net_amount'),
                    (f'{period_prefix}大单净流入-净占比', 'large_net_rate'),
                    (f'{period_prefix}中单净流入-净额', 'medium_net_amount'),
                    (f'{period_prefix}中单净流入-净占比', 'medium_net_rate'),
                    (f'{period_prefix}小单净流入-净额', 'small_net_amount'),
                    (f'{period_prefix}小单净流入-净占比', 'small_net_rate')
                ]
                
                for old_col, new_col in fund_flow_mappings:
                    if old_col in df.columns:
                        column_mapping[old_col] = new_col
                
                # 重命名列
                df = df.rename(columns=column_mapping)
                
                # 添加周期指标和交易日期
                df['indicator'] = indicator
                df['trade_date'] = today
                df['update_time'] = datetime.now()
                
                # 数据类型转换
                numeric_fields = ['rank', 'latest_price', 'pct_chg', 'main_net_inflow_amount', 
                                'main_net_inflow_rate', 'super_large_net_amount', 'super_large_net_rate',
                                'large_net_amount', 'large_net_rate', 'medium_net_amount', 'medium_net_rate',
                                'small_net_amount', 'small_net_rate']
                
                for field in numeric_fields:
                    if field in df.columns:
                        df[field] = pd.to_numeric(df[field], errors='coerce')
                
                # 清理数据
                df = self.clean_dataframe(df)
                
                # 移除关键字段为空的行
                df = df.dropna(subset=['stock_code', 'stock_name'])
                
                # 过滤掉在stock_basic表中不存在的股票代码，避免外键约束错误
                from database import db
                valid_stocks = db.get_stock_list()
                before_count = len(df)
                df_filtered = df[df['stock_code'].isin(valid_stocks)].copy()
                after_count = len(df_filtered)
                
                if before_count > after_count:
                    logger.info(f"过滤掉 {before_count - after_count} 只不在基础表中的股票")
                    logger.info(f"剩余有效股票数据: {after_count} 条")
                
                df = pd.DataFrame(df_filtered)
                
                # 数据去重 - 按唯一约束字段去重，保留第一条记录
                unique_fields = ['stock_code', 'indicator', 'trade_date']
                before_dedup_count = len(df)
                df = df.drop_duplicates(subset=unique_fields, keep='first')
                after_dedup_count = len(df)
                
                if before_dedup_count > after_dedup_count:
                    logger.info(f"数据去重: 删除 {before_dedup_count - after_dedup_count} 条重复记录")
                    logger.info(f"去重后剩余数据: {after_dedup_count} 条")
                
                logger.info(f"准备插入 {len(df)} 条 {indicator} 个股资金流排名数据")
                logger.info(f"数据列: {df.columns.tolist()}")
                
                # 插入数据库
                success = db.upsert_dataframe(
                    df,
                    'stock_fund_flow_rank',
                    ['stock_code', 'indicator', 'trade_date']
                )
                
                if success:
                    logger.info(f"✅ 成功采集 {indicator} 个股资金流排名数据 {len(df)} 条")
                else:
                    overall_success = False
                    logger.error(f"❌ {indicator} 个股资金流排名数据插入失败")
                
                # 延时控制
                self.smart_delay()
                
            except Exception as e:
                logger.error(f"❌ 采集 {indicator} 个股资金流排名数据失败: {e}")
                overall_success = False
        
        if overall_success:
            logger.info("✅ 个股资金流排名数据采集完成")
        else:
            logger.warning("⚠️ 个股资金流排名数据部分采集失败")
        
        return overall_success


class StockNewsCollector(BaseCollector):
    """东方财富全球财经快讯采集器"""
    
    def __init__(self):
        super().__init__()
        # 设置适合新闻采集的延时配置
        self.set_delay_config(
            base_delay=0.1,      # 基础延时0.1秒（更快）
            random_delay=0.2,    # 随机延时最多0.2秒
            batch_delay=2.0,     # 批次延时2秒
            batch_size=50        # 每50条处理一批
        )
    
    def collect_news(self, max_process_count: int = 10) -> bool:
        """采集东方财富全球财经快讯"""
        logger.info("开始采集东方财富全球财经快讯...")
        
        try:
            # 使用 akshare 获取东方财富全球财经快讯数据
            logger.info("正在从东方财富获取全球财经快讯...")
            news_df = self.safe_request(ak.stock_info_global_em)
            
            if news_df.empty:
                logger.warning("东方财富新闻数据为空")
                return False
            
            original_count = len(news_df)
            logger.info(f"获取到 {original_count} 条新闻数据")
            
            # 限制处理数量 - 只处理最新的数据
            if original_count > max_process_count:
                news_df = news_df.head(max_process_count)
                logger.info(f"限制处理数量，只处理最新的 {max_process_count} 条新闻")
            
            # 数据清理和转换
            news_df = self.clean_dataframe(news_df)
            
            # 字段映射和处理
            processed_news = []
            new_news_count = 0
            duplicate_count = 0
            processed_urls = set()  # 记录已处理的URL，避免批次内重复
            
            for idx, (index, row) in enumerate(news_df.iterrows()):
                try:
                    # 提取基础数据（新接口字段：标题、摘要、发布时间、链接）
                    url = str(row.get('链接', '')).strip()
                    if not url:
                        continue
                    
                    # 检查批次内是否重复
                    if url in processed_urls:
                        duplicate_count += 1
                        continue
                    
                    # 快速检查数据库中是否已存在
                    try:
                        existing_check = db.supabase.table('stock_news').select('id').eq('url', url).limit(1).execute()
                        if existing_check.data:
                            duplicate_count += 1
                            # 每100个重复新闻输出一次进度
                            if duplicate_count % 100 == 0:
                                logger.info(f"已跳过 {duplicate_count} 条重复新闻...")
                            continue
                    except:
                        pass  # 检查失败就继续处理
                    
                    # 记录URL到已处理集合
                    processed_urls.add(url)
                    
                    # 准备数据（字段映射：标题->tag, 摘要->summary, 发布时间->pub_time等）
                    title_value = row.get('标题')
                    summary_value = row.get('摘要')
                    pub_time_value = row.get('发布时间')
                    
                    news_data = {
                        'url': url,
                        'tag': str(title_value)[:100] if title_value is not None and not pd.isna(title_value) else None,
                        'summary': str(summary_value) if summary_value is not None and not pd.isna(summary_value) else None,
                        'pub_time': str(pub_time_value)[:50] if pub_time_value is not None and not pd.isna(pub_time_value) else None,
                        'pub_date_time': self._parse_pub_time(pub_time_value),
                        'create_time': datetime.now(),
                        'update_time': datetime.now()
                    }
                    
                    processed_news.append(news_data)
                    new_news_count += 1
                    
                    # 每50条新新闻输出一次进度
                    if new_news_count % 50 == 0:
                        logger.info(f"已处理 {new_news_count} 条新新闻，跳过 {duplicate_count} 条重复新闻")
                    
                    # 如果连续100条都是重复的，提前结束
                    if duplicate_count > 100 and new_news_count == 0:
                        logger.info("连续发现大量重复新闻，提前结束采集")
                        break
                    
                    # 智能延时控制 - 减少延时频率
                    if idx % 10 == 0:  # 每10条延时一次
                        self.smart_delay(idx // 10)
                    
                except Exception as e:
                    logger.warning(f"处理第 {idx} 条新闻失败: {e}")
                    continue
            
            if not processed_news:
                logger.warning("没有有效的新闻数据")
                return False
            
            # 转换为DataFrame
            news_insert_df = pd.DataFrame(processed_news)
            
            logger.info(f"准备插入 {len(news_insert_df)} 条新闻数据")
            
            # 插入数据库（基于URL去重）
            success = db.upsert_dataframe(
                news_insert_df,
                'stock_news',
                ['url']  # 使用URL作为唯一标识
            )
            
            if success:
                logger.info(f"成功采集东方财富全球财经快讯 {len(news_insert_df)} 条")
                
                # 清理超过一周的旧新闻
                self._cleanup_old_news()
            
            return success
            
        except Exception as e:
            logger.error(f"采集东方财富全球财经快讯失败: {e}")
            return False
    
    def _parse_pub_time(self, pub_time_str) -> Optional[datetime]:
        """解析发布时间字符串为datetime对象"""
        if not pub_time_str or pd.isna(pub_time_str):
            return None
            
        try:
            import re
            pub_time_str = str(pub_time_str).strip()
            
            # 格式1: YYYY-MM-DD HH:MM:SS.mmm (带毫秒)
            if re.match(r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+', pub_time_str):
                # 移除毫秒部分
                base_time = re.sub(r'\.\d+$', '', pub_time_str)
                return datetime.strptime(base_time, '%Y-%m-%d %H:%M:%S')
            
            # 格式2: YYYY-MM-DD HH:MM:SS (不带毫秒)
            elif re.match(r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$', pub_time_str):
                return datetime.strptime(pub_time_str, '%Y-%m-%d %H:%M:%S')
            
            # 格式3: YYYY-MM-DD
            elif re.match(r'\d{4}-\d{2}-\d{2}$', pub_time_str):
                return datetime.strptime(pub_time_str, '%Y-%m-%d')
            
            # 格式4: MM月DD日
            elif '月' in pub_time_str and '日' in pub_time_str:
                month_match = re.search(r'(\d+)月', pub_time_str)
                day_match = re.search(r'(\d+)日', pub_time_str)
                if month_match and day_match:
                    current_year = datetime.now().year
                    month = int(month_match.group(1))
                    day = int(day_match.group(1))
                    return datetime(current_year, month, day)
            
            return None
            
        except Exception as e:
            logger.warning(f"解析时间失败: {pub_time_str}, 错误: {e}")
            return None
    
    def _cleanup_old_news(self):
        """清理超过一周的旧新闻"""
        try:
            one_week_ago = datetime.now() - timedelta(days=7)
            one_week_ago_str = one_week_ago.isoformat()
            
            # 查询要删除的记录数量
            old_news = db.supabase.table('stock_news').select('id').lt('create_time', one_week_ago_str).execute()
            delete_count = len(old_news.data) if old_news.data else 0
            
            if delete_count > 0:
                # 执行删除
                db.supabase.table('stock_news').delete().lt('create_time', one_week_ago_str).execute()
                logger.info(f"已清理 {delete_count} 条超过一周的旧新闻")
            
        except Exception as e:
            logger.error(f"清理旧新闻失败: {e}")
    
    def get_latest_news(self, limit: int = 50, tag_filter: Optional[str] = None) -> List[Dict]:
        """获取最新新闻数据"""
        try:
            query_builder = db.supabase.table('stock_news').select('*')
            
            if tag_filter:
                query_builder = query_builder.ilike('tag', f"%{tag_filter}%")
            
            result = query_builder.order('create_time', desc=True).limit(limit).execute()
            
            return result.data if result.data else []
            
        except Exception as e:
            logger.error(f"获取最新新闻失败: {e}")
            return []
    
    def search_news(self, keyword: str, limit: int = 50) -> List[Dict]:
        """搜索新闻"""
        try:
            result = db.supabase.table('stock_news').select('*').or_(
                f"tag.ilike.%{keyword}%,summary.ilike.%{keyword}%"
            ).order('create_time', desc=True).limit(limit).execute()
            
            return result.data if result.data else []
            
        except Exception as e:
            logger.error(f"搜索新闻失败: {e}")
            return []
    
    def get_news_stats(self) -> Dict:
        """获取新闻统计信息"""
        try:
            # 总新闻数
            total_result = db.count_records('stock_news')
            
            # 今日新增新闻数
            today_start = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
            today_start_str = today_start.isoformat()
            today_result = db.supabase.table('stock_news').select('id').gte('create_time', today_start_str).execute()
            today_count = len(today_result.data) if today_result.data else 0
            
            # 本周新增新闻数
            week_start = today_start - timedelta(days=today_start.weekday())
            week_start_str = week_start.isoformat()
            week_result = db.supabase.table('stock_news').select('id').gte('create_time', week_start_str).execute()
            week_count = len(week_result.data) if week_result.data else 0
            
            # 最新新闻时间
            latest_result = db.supabase.table('stock_news').select('create_time').order('create_time', desc=True).limit(1).execute()
            latest_time = latest_result.data[0]['create_time'] if latest_result.data else None
            
            # 热门标签统计（最近一周）
            week_ago = datetime.now() - timedelta(days=7)
            week_ago_str = week_ago.isoformat()
            tag_result = db.supabase.table('stock_news').select('tag').gte('create_time', week_ago_str).execute()
            
            # 统计标签频次
            tag_counts = {}
            if tag_result.data:
                for record in tag_result.data:
                    tag = record.get('tag')
                    if tag:
                        tag_counts[tag] = tag_counts.get(tag, 0) + 1
            
            # 取前10个热门标签
            hot_tags = [{'tag': tag, 'count': count} for tag, count in 
                       sorted(tag_counts.items(), key=lambda x: x[1], reverse=True)[:10]]
            
            return {
                'total_news': total_result,
                'today_news': today_count,
                'week_news': week_count,
                'latest_update': latest_time,
                'hot_tags': hot_tags
            }
            
        except Exception as e:
            logger.error(f"获取新闻统计失败: {e}")
            return {}


# 采集器实例
stock_basic_collector = StockBasicCollector()
daily_quote_collector = DailyQuoteCollector()
index_data_collector = IndexDataCollector()
trade_calendar_collector = TradeCalendarCollector()
stock_hot_rank_collector = StockHotRankCollector()
stock_hot_up_collector = StockHotUpCollector()
hsgt_fund_flow_collector = HsgtFundFlowCollector()
stock_fund_flow_rank_collector = StockFundFlowRankCollector()
stock_news_collector = StockNewsCollector() 