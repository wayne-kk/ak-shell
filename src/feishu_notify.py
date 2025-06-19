#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
飞书消息推送模块
用于A股数据采集任务完成后的状态通知
"""

import json
import os
import requests
from datetime import datetime
from typing import Optional, Dict, Any
from loguru import logger


class FeishuNotifier:
    """飞书消息推送器"""
    
    def __init__(self, webhook_url: str):
        """
        初始化飞书推送器
        
        Args:
            webhook_url: 飞书机器人webhook地址
        """
        self.webhook_url = webhook_url
        self.timeout = 10  # 请求超时时间
    
    def send_text_message(self, content: str) -> bool:
        """
        发送纯文本消息
        
        Args:
            content: 消息内容
            
        Returns:
            bool: 发送是否成功
        """
        try:
            data = {
                "msg_type": "text",
                "content": {
                    "text": content
                }
            }
            
            response = requests.post(
                self.webhook_url,
                json=data,
                timeout=self.timeout,
                headers={'Content-Type': 'application/json'}
            )
            
            if response.status_code == 200:
                result = response.json()
                if result.get('code') == 0:
                    logger.info("飞书消息发送成功")
                    return True
                else:
                    logger.error(f"飞书消息发送失败: {result.get('msg', 'Unknown error')}")
                    return False
            else:
                logger.error(f"飞书API请求失败: HTTP {response.status_code}")
                return False
                
        except Exception as e:
            logger.error(f"发送飞书消息异常: {e}")
            return False
    
    def send_card_message(self, title: str, content: Dict[str, Any]) -> bool:
        """
        发送卡片消息
        
        Args:
            title: 卡片标题
            content: 卡片内容数据
            
        Returns:
            bool: 发送是否成功
        """
        try:
            # 构建卡片内容
            elements = []
            
            # 添加内容项
            for key, value in content.items():
                if isinstance(value, dict):
                    # 嵌套内容
                    for sub_key, sub_value in value.items():
                        elements.append({
                            "tag": "div",
                            "text": {
                                "content": f"**{sub_key}**: {sub_value}",
                                "tag": "lark_md"
                            }
                        })
                else:
                    elements.append({
                        "tag": "div", 
                        "text": {
                            "content": f"**{key}**: {value}",
                            "tag": "lark_md"
                        }
                    })
            
            # 添加时间戳
            elements.append({
                "tag": "div",
                "text": {
                    "content": f"**执行时间**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
                    "tag": "lark_md"
                }
            })
            
            data = {
                "msg_type": "interactive",
                "card": {
                    "elements": elements,
                    "header": {
                        "title": {
                            "content": title,
                            "tag": "lark_md"
                        },
                        "template": "blue"
                    }
                }
            }
            
            response = requests.post(
                self.webhook_url,
                json=data,
                timeout=self.timeout,
                headers={'Content-Type': 'application/json'}
            )
            
            if response.status_code == 200:
                result = response.json()
                if result.get('code') == 0:
                    logger.info("飞书卡片消息发送成功")
                    return True
                else:
                    logger.error(f"飞书卡片消息发送失败: {result.get('msg', 'Unknown error')}")
                    return False
            else:
                logger.error(f"飞书API请求失败: HTTP {response.status_code}")
                return False
                
        except Exception as e:
            logger.error(f"发送飞书卡片消息异常: {e}")
            return False
    
    def notify_task_completion(self, task_name: str, success: bool, 
                             details: Optional[Dict[str, Any]] = None) -> bool:
        """
        发送任务完成通知
        
        Args:
            task_name: 任务名称
            success: 任务是否成功
            details: 任务详情
            
        Returns:
            bool: 发送是否成功
        """
        status_emoji = "✅" if success else "❌"
        status_text = "成功" if success else "失败"
        
        if details:
            # 发送详细卡片消息
            title = f"{status_emoji} A股数据采集 - {task_name}{status_text}"
            
            content = {
                "任务状态": f"{status_emoji} {status_text}",
                "任务名称": task_name,
            }
            
            # 添加详细信息
            if details:
                content.update(details)
            
            return self.send_card_message(title, content)
        else:
            # 发送简单文本消息
            message = f"{status_emoji} A股数据采集 - {task_name}{status_text}\n时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
            return self.send_text_message(message)
    
    def notify_daily_summary(self, summary_data: Dict[str, Any]) -> bool:
        """
        发送每日数据采集汇总
        
        Args:
            summary_data: 汇总数据
            
        Returns:
            bool: 发送是否成功
        """
        title = "📊 A股数据采集 - 每日汇总报告"
        
        content = {
            "📈 数据概况": {
                "股票总数": summary_data.get('total_stocks', 'N/A'),
                "采集日期": summary_data.get('trade_date', 'N/A'),
                "更新记录": summary_data.get('updated_records', 'N/A'),
            },
            "🕐 执行时间": {
                "开始时间": summary_data.get('start_time', 'N/A'),
                "结束时间": summary_data.get('end_time', 'N/A'),
                "执行耗时": summary_data.get('duration', 'N/A'),
            },
            "📋 任务状态": {
                "当日数据": summary_data.get('today_status', 'N/A'),
                "指数数据": summary_data.get('index_status', 'N/A'),
                "整体状态": summary_data.get('overall_status', 'N/A'),
            }
        }
        
        return self.send_card_message(title, content)


# 全局飞书通知器实例
# 从环境变量读取webhook URL，必须设置环境变量
FEISHU_WEBHOOK_URL = os.getenv('FEISHU_WEBHOOK_URL')

if not FEISHU_WEBHOOK_URL:
    logger.warning("⚠️ 未设置 FEISHU_WEBHOOK_URL 环境变量，飞书通知功能将不可用")
    feishu_notifier = None
else:
    feishu_notifier = FeishuNotifier(FEISHU_WEBHOOK_URL)


def send_completion_notice(task_name: str, success: bool, details: Optional[Dict[str, Any]] = None) -> bool:
    """
    快捷发送任务完成通知
    
    Args:
        task_name: 任务名称
        success: 是否成功
        details: 详细信息
        
    Returns:
        bool: 发送是否成功
    """
    if feishu_notifier is None:
        logger.warning("飞书通知器未初始化，跳过通知发送")
        return False
    return feishu_notifier.notify_task_completion(task_name, success, details)


def send_daily_summary(summary_data: Dict[str, Any]) -> bool:
    """
    快捷发送每日汇总报告
    
    Args:
        summary_data: 汇总数据
        
    Returns:
        bool: 发送是否成功
    """
    if feishu_notifier is None:
        logger.warning("飞书通知器未初始化，跳过汇总报告发送")
        return False
    return feishu_notifier.notify_daily_summary(summary_data)


if __name__ == "__main__":
    # 测试飞书推送功能
    print("测试飞书消息推送...")
    
    if feishu_notifier is None:
        print("❌ 无法测试：FEISHU_WEBHOOK_URL 环境变量未设置")
        print("请设置环境变量: export FEISHU_WEBHOOK_URL='your_webhook_url'")
        exit(1)
    
    # 测试文本消息
    test_success = feishu_notifier.send_text_message("🧪 A股数据采集系统测试消息")
    print(f"文本消息测试: {'成功' if test_success else '失败'}")
    
    # 测试卡片消息
    test_details = {
        "测试项目": "飞书推送功能",
        "测试时间": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        "测试状态": "正常"
    }
    
    card_success = feishu_notifier.notify_task_completion("功能测试", True, test_details)
    print(f"卡片消息测试: {'成功' if card_success else '失败'}") 