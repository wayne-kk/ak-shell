#!/bin/bash
# 自动设置A股数据采集定时任务

# 获取项目路径
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

echo "正在为A股数据采集系统设置定时任务..."
echo "项目路径: $PROJECT_DIR"

# 检查飞书webhook环境变量
if [ -z "$FEISHU_WEBHOOK_URL" ]; then
    echo ""
    echo "⚠️  警告: 未设置 FEISHU_WEBHOOK_URL 环境变量"
    echo "飞书通知功能将不可用，建议设置后重新运行此脚本"
    echo ""
    echo "设置方法："
    echo "export FEISHU_WEBHOOK_URL='your_webhook_url_here'"
    echo ""
    read -p "是否继续设置定时任务？(y/N): " continue_setup
    if [[ ! $continue_setup =~ ^[Yy]$ ]]; then
        echo "已取消设置"
        exit 0
    fi
fi

# 为脚本添加执行权限
chmod +x "$SCRIPT_DIR/daily_tasks.sh"
chmod +x "$SCRIPT_DIR/weekly_tasks.sh"
chmod +x "$SCRIPT_DIR/monthly_tasks.sh"
chmod +x "$SCRIPT_DIR/news_tasks.sh"

# 创建临时crontab文件
TEMP_CRONTAB="/tmp/akshare_crontab"

# 备份当前crontab
echo "备份当前crontab..."
crontab -l > "${TEMP_CRONTAB}.backup" 2>/dev/null || echo "# 当前无crontab任务" > "${TEMP_CRONTAB}.backup"

# 生成新的crontab配置
cat > "$TEMP_CRONTAB" << EOF
# A股数据采集系统定时任务
# 项目路径: $PROJECT_DIR

# 环境变量配置 - 脚本会自动从.env文件加载

# ============== 日常任务 ==============
# 每个交易日收盘后采集当日最新数据 (周一到周五 18:00)
0 18 * * 1-5 $SCRIPT_DIR/daily_tasks.sh >> $PROJECT_DIR/logs/cron_daily.log 2>&1

# 夜间补充采集 (每天 23:00)
0 23 * * * $SCRIPT_DIR/daily_tasks.sh >> $PROJECT_DIR/logs/cron_night.log 2>&1

# ============== 周度任务 ==============
# 每周日凌晨执行维护任务 (周日 02:00)
0 2 * * 0 $SCRIPT_DIR/weekly_tasks.sh >> $PROJECT_DIR/logs/cron_weekly.log 2>&1

# ============== 月度任务 ==============
# 每月第一个周六执行大规模数据补充 (每月1-7号的周六 03:00)
0 3 1-7 * 6 $SCRIPT_DIR/monthly_tasks.sh >> $PROJECT_DIR/logs/cron_monthly.log 2>&1

# ============== 当日数据采集 ==============
# 每个交易日收盘后采集当天全量数据 (工作日 18:00 - 确保数据完整性)
0 18 * * 1-5 cd $PROJECT_DIR && python3 src/main.py today >> logs/cron_today.log 2>&1

# ============== 新闻采集 ==============
# 东方财富全球财经快讯采集 (分时段采集)
# 白天时段每20分钟采集 (6:00-21:59)
*/20 6-21 * * * $SCRIPT_DIR/news_tasks.sh collect >> $PROJECT_DIR/logs/cron_news.log 2>&1

# 夜晚时段每2小时采集 (22:00, 0:00, 2:00, 4:00)
0 22,0,2,4 * * * $SCRIPT_DIR/news_tasks.sh collect >> $PROJECT_DIR/logs/cron_news.log 2>&1

# ============== 系统监控 ==============
# 每小时检查系统状态 (仅在工作时间)
0 9-17 * * 1-5 echo "[$(date)] 系统运行正常" >> $PROJECT_DIR/logs/system_status.log

EOF

echo ""
echo "定时任务配置预览:"
echo "================================="
cat "$TEMP_CRONTAB"
echo "================================="
echo ""

# 询问用户确认
read -p "是否应用以上定时任务配置？(y/N): " confirm

if [[ $confirm =~ ^[Yy]$ ]]; then
    # 应用新的crontab
    crontab "$TEMP_CRONTAB"
    echo "✅ 定时任务配置成功！"
    
    # 显示当前crontab
    echo ""
    echo "当前定时任务列表:"
    crontab -l
    
    # 创建logs目录
    mkdir -p "$PROJECT_DIR/logs"
    
    echo ""
    echo "📋 定时任务说明:"
    echo "• 当日任务: 每个交易日18:00采集当天全量数据"
    echo "• 日常任务: 每个交易日18:00和23:00采集最新数据"
    echo "• 新闻采集: 白天(6:00-21:59)每20分钟，夜晚(22:00-5:59)每2小时"
    echo "• 周度任务: 每周日02:00执行系统维护"
    echo "• 月度任务: 每月第一个周六03:00执行大规模数据补充"
    echo ""
    echo "📁 日志文件位置: $PROJECT_DIR/logs/"
    echo "🔧 如需修改: 运行 'crontab -e'"
    echo "❌ 如需删除: 运行 'crontab -r'"
    
else
    echo "❌ 已取消定时任务配置"
    echo "💡 如需手动配置，请参考: $TEMP_CRONTAB"
fi

# 清理临时文件
rm -f "$TEMP_CRONTAB"

echo ""
echo "设置完成！" 