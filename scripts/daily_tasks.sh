#!/bin/bash
# A股数据采集 - 日常任务脚本
# 用于每日自动采集最新数据

# 设置工作目录
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
cd "$PROJECT_DIR"

# 加载环境变量
if [ -f ".env" ]; then
    export $(grep -v '^#' .env | xargs)
fi

# 设置日志
LOG_FILE="logs/daily_$(date +%Y%m%d).log"
mkdir -p logs

# 日志函数
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

# 错误处理
handle_error() {
    log "ERROR: $1"
    exit 1
}

log "========================================="
log "开始执行日常数据采集任务"
log "========================================="

# 1. 采集当日股票数据（仅股票，不含指数和通知）
log "1. 开始采集当日股票数据..."
python3 src/main.py today || handle_error "当日数据采集失败"
log "1. 当日股票数据采集完成"

# 2. 更新指数数据（支持自动重试）
log "2. 开始更新指数数据..."
today=$(date +%Y%m%d)
# macOS兼容的date命令
yesterday=$(date -v-1d +%Y%m%d)
cd "$PROJECT_DIR" && python3 -c "
import sys
sys.path.append('src')
from collectors import index_data_collector
from datetime import datetime

# 使用新的重试机制，如果数据源未更新会自动推迟1小时重试
success = index_data_collector.collect_all_indexes_history('$yesterday', '$today', retry_delay_hours=1)
print(f'指数数据更新结果: {success}')

# 如果当前时间是收盘后，且采集的是当日数据，记录重试信息
current_hour = datetime.now().hour
if '$today' == datetime.now().strftime('%Y%m%d') and current_hour >= 15:
    print('注意：如果指数数据源未更新，系统将在1小时后自动重试')
" || handle_error "指数数据更新失败"
log "2. 指数数据更新完成"

# 3. 发送飞书通知
log "3. 发送飞书完成通知..."
cd "$PROJECT_DIR" && python3 -c "
import sys
sys.path.append('src')
from feishu_notify import send_completion_notice
from datetime import datetime
import os

# 读取日志文件检查执行状态
log_file = 'logs/daily_$(date +%Y%m%d).log'
success = True  # 如果执行到这里说明前面的任务都成功了

try:
    send_completion_notice(
        '日常数据采集任务',
        success,
        {
            '执行日期': '$(date +%Y-%m-%d)',
            '执行时间': '$(date +%H:%M:%S)',
            '任务类型': '当日数据 + 指数数据',
            '执行状态': '✅ 全部完成'
        }
    )
    print('✅ 飞书通知发送成功')
except Exception as e:
    print(f'❌ 飞书通知发送失败: {e}')
" || log "WARNING: 飞书通知发送失败"
log "3. 飞书通知发送完成"

log "========================================="
log "日常数据采集任务执行完成"
log "=========================================" 