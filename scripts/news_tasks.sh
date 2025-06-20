#!/bin/bash
# A股数据采集 - 新闻采集任务脚本
# 用于定时采集东方财富全球财经快讯

# 设置工作目录
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
cd "$PROJECT_DIR"

# 加载环境变量
if [ -f ".env" ]; then
    export $(grep -v '^#' .env | xargs)
fi

# 设置日志
LOG_FILE="logs/news_$(date +%Y%m%d).log"
mkdir -p logs

# 日志函数
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

# 错误处理
handle_error() {
    log "ERROR: $1"
    # 发送失败通知
    python3 -c "
import sys
sys.path.append('src')
from feishu_notify import send_completion_notice
try:
    send_completion_notice('东方财富全球财经快讯采集', False, {'错误信息': '$1'})
except:
    pass
"
    exit 1
}

# 解析命令行参数
ACTION=${1:-collect}

case $ACTION in
    "collect")
        log "开始执行新闻采集任务..."
        
        # 执行新闻采集
        python3 -c "
import sys
sys.path.append('src')
from collectors import stock_news_collector
from feishu_notify import send_completion_notice
from datetime import datetime

start_time = datetime.now()

try:
    # 获取采集前统计
    stats_before = stock_news_collector.get_news_stats()
    count_before = stats_before.get('total_news', 0)
    
    # 执行采集
    success = stock_news_collector.collect_news(max_process_count=10)
    
    # 获取采集后统计
    stats_after = stock_news_collector.get_news_stats()
    count_after = stats_after.get('total_news', 0)
    new_count = count_after - count_before
    
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    
    if success:
        print(f'SUCCESS: 新增{new_count}条新闻，耗时{duration:.1f}秒')
        
        # 发送成功通知
        details = {
            '采集耗时': f'{duration:.1f}秒',
            '采集前总数': f'{count_before}条',
            '采集后总数': f'{count_after}条',
            '新增新闻': f'{new_count}条',
            '重复数据': f'{10 - new_count}条' if new_count <= 10 else '0条',
            '数据来源': '东方财富全球财经快讯'
        }
        send_completion_notice('东方财富全球财经快讯采集', True, details)
    else:
        print('FAILED: 新闻采集失败')
        send_completion_notice('东方财富全球财经快讯采集', False, {'错误信息': '采集失败'})
        
except Exception as e:
    print(f'ERROR: {e}')
    send_completion_notice('东方财富全球财经快讯采集', False, {'错误信息': str(e)})
    sys.exit(1)
" || handle_error "新闻采集任务执行失败"
        
        log "新闻采集任务执行完成"
        ;;
        
    "stats")
        log "获取新闻统计信息..."
        
        python3 -c "
import sys
sys.path.append('src')
from collectors import stock_news_collector

try:
    stats = stock_news_collector.get_news_stats()
    
    print()
    print('=== 东方财富全球财经快讯统计 ===')
    print(f'总新闻数量: {stats.get(\"total_news\", 0)} 条')
    print(f'今日新增: {stats.get(\"today_news\", 0)} 条')
    print(f'本周新增: {stats.get(\"week_news\", 0)} 条')
    print(f'最后更新: {stats.get(\"latest_update\", \"N/A\")}')
    
    hot_tags = stats.get('hot_tags', [])
    if hot_tags:
        print()
        print('热门标签 (最近一周):')
        for i, tag_info in enumerate(hot_tags[:10], 1):
            print(f'  {i}. {tag_info[\"tag\"]}: {tag_info[\"count\"]} 条')
    print('=' * 40)
    
except Exception as e:
    print(f'获取统计信息失败: {e}')
    sys.exit(1)
"
        ;;
        
    *)
        echo "用法: $0 [collect|stats]"
        echo "  collect  - 采集新闻 (默认)"
        echo "  stats    - 查看统计"
        exit 1
        ;;
esac 