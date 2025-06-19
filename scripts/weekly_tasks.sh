#!/bin/bash
# A股数据采集 - 周度任务脚本
# 用于每周更新股票基础信息和系统维护

# 设置工作目录
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
cd "$PROJECT_DIR"

# 加载环境变量
if [ -f ".env" ]; then
    export $(grep -v '^#' .env | xargs)
fi

# 设置日志
LOG_FILE="logs/weekly_$(date +%Y%m%d).log"
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
log "开始执行周度维护任务"
log "========================================="

# 1. 更新股票基础信息（可能有新股上市或退市）
log "1. 开始更新股票基础信息..."
cd "$PROJECT_DIR" && python3 -c "
import sys
sys.path.append('src')
from collectors import stock_basic_collector
success = stock_basic_collector.collect()
print(f'股票基础信息更新结果: {success}')
" || handle_error "股票基础信息更新失败"
log "1. 股票基础信息更新完成"

# 2. 更新交易日历
log "2. 开始更新交易日历..."
cd "$PROJECT_DIR" && python3 -c "
import sys
sys.path.append('src')
from collectors import trade_calendar_collector
success = trade_calendar_collector.collect()
print(f'交易日历更新结果: {success}')
" || handle_error "交易日历更新失败"
log "2. 交易日历更新完成"

# 3. 清理旧日志文件（保留30天）
log "3. 开始清理旧日志文件..."
find logs/ -name "*.log" -mtime +30 -delete
log "3. 旧日志文件清理完成"

# 4. 补充历史数据（如果有缺失）
log "4. 开始补充近期历史数据..."
start_date=$(date -v-7d +%Y%m%d)
python3 src/main.py history --start-date "$start_date" --base-delay 1.0 --batch-delay 30.0 --batch-size 20 || log "WARNING: 历史数据补充可能失败，请检查"
log "4. 历史数据补充完成"

# 5. 发送飞书通知
log "5. 发送飞书完成通知..."
cd "$PROJECT_DIR" && python3 -c "
import sys
sys.path.append('src')
from feishu_notify import send_completion_notice
from datetime import datetime

try:
    send_completion_notice(
        '周度维护任务',
        True,
        {
            '执行日期': '$(date +%Y-%m-%d)',
            '执行时间': '$(date +%H:%M:%S)',
            '维护内容': '基础信息 + 交易日历 + 历史数据补充',
            '执行状态': '✅ 维护完成'
        }
    )
    print('✅ 飞书通知发送成功')
except Exception as e:
    print(f'❌ 飞书通知发送失败: {e}')
" || log "WARNING: 飞书通知发送失败"
log "5. 飞书通知发送完成"

log "========================================="
log "周度维护任务执行完成"
log "=========================================" 