#!/bin/bash
# A股数据采集 - 月度任务脚本
# 用于大规模历史数据补充和系统优化

# 设置工作目录
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
cd "$PROJECT_DIR"

# 设置日志
LOG_FILE="logs/monthly_$(date +%Y%m%d).log"
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
log "开始执行月度数据补充任务"
log "========================================="

# 1. 智能数据缺失检查和补充
log "1. 开始检查并补充缺失的历史数据..."
python3 -c "
from src.database import db
from src.collectors import daily_quote_collector
from datetime import datetime, timedelta
import akshare as ak

# 获取最近30天的交易日
try:
    trade_dates = ak.tool_trade_date_hist_sina()
    recent_dates = trade_dates[trade_dates['trade_date'] >= (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')]
    trade_date_list = [d.strftime('%Y%m%d') for d in recent_dates['trade_date']]
    print(f'最近30天交易日: {len(trade_date_list)} 天')
    
    # 随机抽取50只股票检查数据完整性
    import random
    stock_list = db.get_stock_list()
    sample_stocks = random.sample(stock_list, min(50, len(stock_list)))
    
    missing_dates = set()
    
    for stock_code in sample_stocks:
        for trade_date in trade_date_list:
            # 检查该股票在该交易日是否有数据
            result = db.supabase.table('daily_quote').select('trade_date').eq('stock_code', stock_code).eq('trade_date', trade_date).execute()
            if not result.data:
                missing_dates.add(trade_date)
    
    print(f'发现缺失的交易日: {sorted(missing_dates)}')
    
    # 如果有缺失数据，进行补充
    if missing_dates:
        print(f'开始补充 {len(missing_dates)} 个缺失交易日的数据...')
        for missing_date in sorted(missing_dates):
            print(f'补充日期: {missing_date}')
            daily_quote_collector.collect_all_stocks_history(missing_date, missing_date, enable_resume=True)
            # 保守延时
            import time
            time.sleep(60)
    else:
        print('✅ 近期数据完整，无需补充')
        
except Exception as e:
    print(f'数据完整性检查失败: {e}')
" || log "WARNING: 数据缺失检查可能失败"
log "1. 数据缺失检查和补充完成"

# 2. 数据质量检查
log "2. 开始数据质量检查..."
python3 -c "
from src.database import db
import pandas as pd
from datetime import datetime, timedelta

# 检查最近30天的数据完整性
stock_list = db.get_stock_list()
print(f'总股票数: {len(stock_list)}')

# 随机抽取100只股票检查数据完整性
import random
sample_stocks = random.sample(stock_list, min(100, len(stock_list)))

missing_data = []
for stock_code in sample_stocks:
    latest_date = db.get_latest_date('daily_quote', 'trade_date', stock_code)
    if not latest_date:
        missing_data.append(stock_code)

print(f'抽样检查: {len(sample_stocks)} 只股票')
print(f'缺失数据: {len(missing_data)} 只股票')
if missing_data:
    print(f'缺失股票代码: {missing_data[:10]}...')
" || log "WARNING: 数据质量检查失败"
log "2. 数据质量检查完成"

# 3. 清理超过90天的日志文件
log "3. 开始清理超过90天的日志文件..."
find logs/ -name "*.log" -mtime +90 -delete
log "3. 超旧日志文件清理完成"

# 4. 系统状态报告
log "4. 生成系统状态报告..."
python3 -c "
from src.database import db
import json
from datetime import datetime

report = {
    'date': datetime.now().isoformat(),
    'stock_count': len(db.get_stock_list()),
    'latest_collection': {
        'daily_quote': 'checking...',
        'stock_basic': 'checking...',
    }
}

print(json.dumps(report, indent=2, ensure_ascii=False))
" > "logs/monthly_report_$(date +%Y%m).json" || log "WARNING: 系统状态报告生成失败"
log "4. 系统状态报告生成完成"

# 5. 发送飞书月度汇总报告
log "5. 发送飞书月度汇总报告..."
python3 -c "
from src.feishu_notify import send_completion_notice
from src.database import db
from datetime import datetime

try:
    # 获取数据统计
    stock_count = len(db.get_stock_list())
    
    send_completion_notice(
        '月度数据补充任务',
        True,
        {
            '执行日期': '$(date +%Y-%m-%d)',
            '执行时间': '$(date +%H:%M:%S)',
            '股票总数': f'{stock_count}只',
            '补充内容': '缺失数据检查 + 质量检查 + 日志清理',
            '执行状态': '✅ 月度维护完成',
            '下次执行': '下月第一个周六'
        }
    )
    print('✅ 飞书月度报告发送成功')
except Exception as e:
    print(f'❌ 飞书通知发送失败: {e}')
" || log "WARNING: 飞书通知发送失败"
log "5. 飞书月度报告发送完成"

log "========================================="
log "月度数据补充任务执行完成"
log "=========================================" 