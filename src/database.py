#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Supabase 数据库连接和操作模块
"""

import os
from typing import Optional, List, Dict, Any
import pandas as pd
from loguru import logger
from dotenv import load_dotenv
from supabase import create_client, Client

load_dotenv()


class Database:
    """Supabase 数据库操作类"""
    
    def __init__(self):
        # 获取 Supabase 配置
        self.supabase_url = os.getenv('SUPABASE_URL')
        self.supabase_key = os.getenv('SUPABASE_SERVICE_ROLE_KEY')
        
        if not self.supabase_url or not self.supabase_key:
            raise ValueError("请设置 SUPABASE_URL 和 SUPABASE_SERVICE_ROLE_KEY 环境变量")
        
        # 创建 Supabase 客户端
        try:
            self.supabase: Client = create_client(self.supabase_url, self.supabase_key)
            logger.info("成功连接到 Supabase")
        except Exception as e:
            logger.error(f"Supabase 连接失败: {e}")
            raise
    
    def insert_dataframe(self, df: pd.DataFrame, table_name: str, 
                        if_exists: str = 'append', method: str = 'multi') -> bool:
        """将DataFrame插入Supabase表"""
        try:
            if df.empty:
                logger.warning(f"数据为空，跳过插入到 {table_name}")
                return True
            
            # 将 DataFrame 转换为字典列表，处理 NaN 值
            records = df.fillna('').to_dict('records')
            
            # 清理数据：将空字符串转换为 None
            for record in records:
                for key, value in record.items():
                    if value == '' or pd.isna(value):
                        record[key] = None
            
            # 分批插入以避免请求过大
            batch_size = 1000
            total_inserted = 0
            
            for i in range(0, len(records), batch_size):
                batch = records[i:i + batch_size]
                
                try:
                    result = self.supabase.table(table_name).insert(batch).execute()
                    
                    if hasattr(result, 'error') and result.error:
                        logger.error(f"Supabase 插入错误: {result.error}")
                        return False
                    
                    total_inserted += len(batch)
                    logger.info(f"成功插入批次 {i//batch_size + 1}: {len(batch)} 条记录")
                    
                except Exception as e:
                    logger.error(f"插入批次 {i//batch_size + 1} 失败: {e}")
                    return False
            
            logger.info(f"成功插入 {total_inserted} 条记录到 {table_name} 表")
            return True
            
        except Exception as e:
            logger.error(f"插入数据失败: {e}")
            return False
    
    def upsert_dataframe(self, df: pd.DataFrame, table_name: str, 
                        conflict_columns: List[str]) -> bool:
        """使用 Supabase upsert 进行数据插入或更新"""
        try:
            if df.empty:
                logger.warning(f"数据为空，跳过 upsert 到 {table_name}")
                return True
            
            # 将 DataFrame 转换为字典列表，处理 NaN 值
            records = df.fillna('').to_dict('records')
            
            # 清理数据：将空字符串转换为 None
            for record in records:
                for key, value in record.items():
                    if value == '' or pd.isna(value):
                        record[key] = None
            
            # 分批处理
            batch_size = 1000
            total_upserted = 0
            
            for i in range(0, len(records), batch_size):
                batch = records[i:i + batch_size]
                
                try:
                    # Supabase 的 upsert 方法
                    result = self.supabase.table(table_name).upsert(
                        batch, 
                        on_conflict=','.join(conflict_columns)
                    ).execute()
                    
                    if hasattr(result, 'error') and result.error:
                        logger.error(f"Supabase upsert 错误: {result.error}")
                        return False
                    
                    total_upserted += len(batch)
                    logger.info(f"成功 upsert 批次 {i//batch_size + 1}: {len(batch)} 条记录")
                    
                except Exception as e:
                    logger.error(f"Upsert 批次 {i//batch_size + 1} 失败: {e}")
                    return False
            
            logger.info(f"成功 upsert {total_upserted} 条记录到 {table_name} 表")
            return True
            
        except Exception as e:
            logger.error(f"Upsert 数据失败: {e}")
            return False
    
    def get_latest_date(self, table_name: str, date_column: str, 
                       stock_code: Optional[str] = None) -> Optional[str]:
        """获取表中最新的日期"""
        try:
            # 构建查询
            query_builder = self.supabase.table(table_name).select(date_column)
            
            if stock_code:
                query_builder = query_builder.eq('stock_code', stock_code)
            
            # 按日期降序排列，取第一条
            result = query_builder.order(date_column, desc=True).limit(1).execute()
            
            if result.data and len(result.data) > 0:
                date_value = result.data[0][date_column]
                if date_value:
                    # 处理不同的日期格式
                    if isinstance(date_value, str):
                        if len(date_value) == 8:  # YYYYMMDD
                            return date_value
                        else:  # YYYY-MM-DD 或其他格式
                            from datetime import datetime
                            dt = datetime.strptime(date_value.split('T')[0], '%Y-%m-%d')
                            return dt.strftime('%Y%m%d')
                    else:
                        return date_value.strftime('%Y%m%d')
            
            return None
            
        except Exception as e:
            logger.error(f"获取最新日期失败: {e}")
            return None
    
    def get_stock_list(self) -> List[str]:
        """获取所有股票代码列表"""
        try:
            # 查询股票代码，排除退市股票
            result = self.supabase.table('stock_basic').select('stock_code').or_('status.is.null,status.neq.退市').execute()
            
            if result.data:
                return [row['stock_code'] for row in result.data if row['stock_code']]
            
            return []
            
        except Exception as e:
            logger.error(f"获取股票列表失败: {e}")
            return []
    
    def check_table_exists(self, table_name: str) -> bool:
        """检查表是否存在"""
        try:
            # 尝试查询表，如果失败则表不存在
            result = self.supabase.table(table_name).select('*').limit(1).execute()
            return True
            
        except Exception as e:
            logger.warning(f"表 {table_name} 不存在或无法访问: {e}")
            return False
    
    def execute_query(self, table_name: str, select_columns: str = '*', 
                     filters: Optional[Dict] = None, 
                     order_by: Optional[str] = None,
                     limit: Optional[int] = None) -> List[Dict]:
        """执行Supabase查询"""
        try:
            # 构建查询
            query_builder = self.supabase.table(table_name).select(select_columns)
            
            # 添加过滤条件
            if filters:
                for key, value in filters.items():
                    query_builder = query_builder.eq(key, value)
            
            # 添加排序
            if order_by:
                if order_by.startswith('-'):
                    query_builder = query_builder.order(order_by[1:], desc=True)
                else:
                    query_builder = query_builder.order(order_by)
            
            # 添加限制
            if limit:
                query_builder = query_builder.limit(limit)
            
            # 执行查询
            result = query_builder.execute()
            
            return result.data if result.data else []
            
        except Exception as e:
            logger.error(f"查询执行失败: {e}")
            return []
    
    def count_records(self, table_name: str, filters: Optional[Dict] = None) -> int:
        """统计记录数量"""
        try:
            query_builder = self.supabase.table(table_name).select('*', count='exact')
            
            # 添加过滤条件
            if filters:
                for key, value in filters.items():
                    query_builder = query_builder.eq(key, value)
            
            result = query_builder.execute()
            return result.count if hasattr(result, 'count') else 0
            
        except Exception as e:
            logger.error(f"统计记录数量失败: {e}")
            return 0


# 全局数据库实例
db = Database() 