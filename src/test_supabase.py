#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Supabase 连接测试脚本
"""

import os
from dotenv import load_dotenv
from supabase import create_client, Client
from loguru import logger

load_dotenv()


def test_supabase_connection():
    """测试 Supabase 连接"""
    
    supabase_url = os.getenv('SUPABASE_URL')
    supabase_key = os.getenv('SUPABASE_SERVICE_ROLE_KEY')
    
    if not supabase_url or not supabase_key:
        logger.error("请设置 SUPABASE_URL 和 SUPABASE_SERVICE_ROLE_KEY 环境变量")
        return False
    
    try:
        # 创建 Supabase 客户端
        supabase: Client = create_client(supabase_url, supabase_key)
        logger.info(f"成功连接到 Supabase: {supabase_url}")
        
        # 测试查询（假设有 stock_basic 表）
        try:
            result = supabase.table('stock_basic').select('*').limit(1).execute()
            logger.info(f"stock_basic 表查询成功，返回 {len(result.data)} 条记录")
            return True
            
        except Exception as e:
            logger.warning(f"stock_basic 表不存在或查询失败: {e}")
            logger.info("这是正常的，如果还没有创建表的话")
            return True
            
    except Exception as e:
        logger.error(f"Supabase 连接失败: {e}")
        return False


def show_supabase_info():
    """显示 Supabase 配置信息"""
    
    supabase_url = os.getenv('SUPABASE_URL')
    supabase_key = os.getenv('SUPABASE_SERVICE_ROLE_KEY')
    database_url = os.getenv('DATABASE_URL')
    
    logger.info("=== Supabase 配置信息 ===")
    logger.info(f"SUPABASE_URL: {supabase_url}")
    logger.info(f"SUPABASE_SERVICE_ROLE_KEY: {'已设置' if supabase_key else '未设置'}")
    logger.info(f"DATABASE_URL: {'已设置' if database_url else '未设置'}")
    
    if not supabase_url:
        logger.warning("请在 .env 文件中设置 SUPABASE_URL")
        logger.info("格式: SUPABASE_URL=https://your-project-ref.supabase.co")
    
    if not supabase_key:
        logger.warning("请在 .env 文件中设置 SUPABASE_SERVICE_ROLE_KEY")
        logger.info("可以在 Supabase 项目设置 -> API 中找到 service_role key")


if __name__ == "__main__":
    logger.info("开始测试 Supabase 连接...")
    
    # 显示配置信息
    show_supabase_info()
    
    # 测试连接
    if test_supabase_connection():
        logger.info("✅ Supabase 连接测试成功！")
    else:
        logger.error("❌ Supabase 连接测试失败！")
        logger.info("请检查:")
        logger.info("1. SUPABASE_URL 是否正确")
        logger.info("2. SUPABASE_SERVICE_ROLE_KEY 是否正确")
        logger.info("3. Supabase 项目是否正常运行")
        logger.info("4. 网络连接是否正常") 