#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
配置管理模块
统一管理项目所有配置项
"""

import os
from typing import Dict, Any
from pathlib import Path
from dotenv import load_dotenv

# 加载环境变量
load_dotenv()

class Config:
    """项目配置类"""
    
    # 项目路径
    PROJECT_ROOT = Path(__file__).parent.parent
    LOGS_DIR = PROJECT_ROOT / "logs"
    DOCS_DIR = PROJECT_ROOT / "docs"
    SCRIPTS_DIR = PROJECT_ROOT / "scripts"
    
    # 数据库配置
    SUPABASE_URL = os.getenv('SUPABASE_URL')
    SUPABASE_SERVICE_ROLE_KEY = os.getenv('SUPABASE_SERVICE_ROLE_KEY')
    DATABASE_URL = os.getenv('DATABASE_URL')
    
    # AKShare配置
    AKSHARE_TIMEOUT = int(os.getenv('AKSHARE_TIMEOUT', 30))
    AKSHARE_RETRY_COUNT = int(os.getenv('AKSHARE_RETRY_COUNT', 3))
    AKSHARE_RATE_LIMIT = int(os.getenv('AKSHARE_RATE_LIMIT', 10))
    
    # 新闻采集配置
    NEWS_COLLECTION_INTERVAL = int(os.getenv('NEWS_COLLECTION_INTERVAL', 20))  # 分钟
    NEWS_MAX_PROCESS_COUNT = int(os.getenv('NEWS_MAX_PROCESS_COUNT', 10))
    NEWS_CLEANUP_DAYS = int(os.getenv('NEWS_CLEANUP_DAYS', 7))
    
    # 飞书通知配置
    FEISHU_WEBHOOK_URL = os.getenv('FEISHU_WEBHOOK_URL')
    FEISHU_ENABLED = bool(FEISHU_WEBHOOK_URL)
    
    # 系统配置
    TIMEZONE = os.getenv('TIMEZONE', 'Asia/Shanghai')
    LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO').upper()
    LOG_RETENTION_DAYS = int(os.getenv('LOG_RETENTION_DAYS', 30))
    
    # 采集延时配置
    DELAY_CONFIG = {
        'base_delay': float(os.getenv('BASE_DELAY', 0.1)),
        'random_delay': float(os.getenv('RANDOM_DELAY', 0.2)),
        'batch_delay': float(os.getenv('BATCH_DELAY', 2.0)),
        'batch_size': int(os.getenv('BATCH_SIZE', 50))
    }
    
    @classmethod
    def get_database_config(cls) -> Dict[str, Any]:
        """获取数据库配置"""
        return {
            'supabase_url': cls.SUPABASE_URL,
            'supabase_key': cls.SUPABASE_SERVICE_ROLE_KEY,
            'database_url': cls.DATABASE_URL
        }
    
    @classmethod
    def get_akshare_config(cls) -> Dict[str, Any]:
        """获取AKShare配置"""
        return {
            'timeout': cls.AKSHARE_TIMEOUT,
            'retry_count': cls.AKSHARE_RETRY_COUNT,
            'rate_limit': cls.AKSHARE_RATE_LIMIT
        }
    
    @classmethod
    def get_news_config(cls) -> Dict[str, Any]:
        """获取新闻采集配置"""
        return {
            'interval': cls.NEWS_COLLECTION_INTERVAL,
            'max_process_count': cls.NEWS_MAX_PROCESS_COUNT,
            'cleanup_days': cls.NEWS_CLEANUP_DAYS
        }
    
    @classmethod
    def get_feishu_config(cls) -> Dict[str, Any]:
        """获取飞书配置"""
        return {
            'webhook_url': cls.FEISHU_WEBHOOK_URL,
            'enabled': cls.FEISHU_ENABLED
        }
    
    @classmethod
    def ensure_directories(cls):
        """确保必要目录存在"""
        cls.LOGS_DIR.mkdir(exist_ok=True)
        cls.DOCS_DIR.mkdir(exist_ok=True)
    
    @classmethod
    def validate_config(cls) -> bool:
        """验证配置完整性"""
        required_configs = [
            ('SUPABASE_URL', cls.SUPABASE_URL),
            ('SUPABASE_SERVICE_ROLE_KEY', cls.SUPABASE_SERVICE_ROLE_KEY),
        ]
        
        missing_configs = []
        for name, value in required_configs:
            if not value:
                missing_configs.append(name)
        
        if missing_configs:
            raise ValueError(f"缺少必要配置: {', '.join(missing_configs)}")
        
        return True

# 全局配置实例
config = Config() 