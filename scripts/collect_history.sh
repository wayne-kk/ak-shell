#!/bin/bash
# A股历史数据采集脚本

set -e

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# 日志函数
log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# 检查Python环境
check_python() {
    if ! command -v python3 &> /dev/null; then
        log_error "Python3 未安装"
        exit 1
    fi
    
    if ! python3 -c "import akshare" &> /dev/null; then
        log_error "akshare 库未安装，请运行: pip install -r requirements.txt"
        exit 1
    fi
    
    log_info "Python环境检查通过"
}

# 检查 Supabase 连接
check_supabase() {
    if [ -z "$SUPABASE_URL" ] || [ -z "$SUPABASE_SERVICE_ROLE_KEY" ]; then
        log_error "SUPABASE_URL 或 SUPABASE_SERVICE_ROLE_KEY 环境变量未设置"
        log_info "请在 .env 文件中设置 Supabase 连接配置"
        exit 1
    fi
    
    log_info "Supabase 连接配置检查通过"
}

# 创建必要的目录
create_directories() {
    mkdir -p logs
    log_info "创建日志目录"
}

# 采集近一年历史数据
collect_one_year() {
    log_info "开始采集近一年历史数据..."
    python3 src/main.py one_year
    
    if [ $? -eq 0 ]; then
        log_info "近一年历史数据采集完成"
    else
        log_error "历史数据采集失败"
        exit 1
    fi
}

# 采集指定时间范围的历史数据
collect_custom_range() {
    start_date=$1
    end_date=$2
    
    if [ -z "$start_date" ]; then
        log_error "请指定开始日期"
        echo "用法: $0 custom YYYYMMDD [YYYYMMDD]"
        exit 1
    fi
    
    log_info "开始采集自定义时间范围历史数据: $start_date - ${end_date:-今天}"
    
    if [ -n "$end_date" ]; then
        python3 src/main.py history --start-date "$start_date" --end-date "$end_date"
    else
        python3 src/main.py history --start-date "$start_date"
    fi
    
    if [ $? -eq 0 ]; then
        log_info "自定义时间范围历史数据采集完成"
    else
        log_error "历史数据采集失败"
        exit 1
    fi
}

# 采集最新数据
collect_latest() {
    log_info "开始采集最新数据..."
    python3 src/main.py latest
    
    if [ $? -eq 0 ]; then
        log_info "最新数据采集完成"
    else
        log_error "最新数据采集失败"
        exit 1
    fi
}

# 启动定时任务
start_scheduler() {
    log_info "启动定时任务调度器..."
    python3 src/scheduler.py
}

# 显示帮助信息
show_help() {
    echo "A股数据采集脚本"
    echo ""
    echo "用法:"
    echo "  $0 one_year                        采集近一年历史数据"
    echo "  $0 custom START_DATE [END_DATE]    采集指定时间范围数据"
    echo "  $0 latest                          采集最新数据"
    echo "  $0 schedule                        启动定时任务"
    echo "  $0 help                            显示帮助信息"
    echo ""
    echo "参数:"
    echo "  START_DATE    开始日期，格式: YYYYMMDD"
    echo "  END_DATE      结束日期，格式: YYYYMMDD (可选)"
    echo ""
    echo "示例:"
    echo "  $0 one_year                        # 采集近一年数据"
    echo "  $0 custom 20230101                 # 从2023年1月1日到今天"
    echo "  $0 custom 20230101 20231231        # 采集2023年全年数据"
    echo "  $0 latest                          # 采集最新数据"
    echo "  $0 schedule                        # 启动定时任务"
}

# 主函数
main() {
    log_info "A股数据采集系统启动"
    
    # 加载环境变量
    if [ -f .env ]; then
        export $(cat .env | grep -v '^#' | xargs)
        log_info "加载环境变量"
    else
        log_warn ".env 文件不存在，使用系统环境变量"
    fi
    
    # 环境检查
    check_python
    check_supabase
    create_directories
    
    # 根据参数执行不同操作
    case "$1" in
        "one_year")
            collect_one_year
            ;;
        "custom")
            collect_custom_range "$2" "$3"
            ;;
        "latest")
            collect_latest
            ;;
        "schedule")
            start_scheduler
            ;;
        "help"|"--help"|"-h"|"")
            show_help
            ;;
        *)
            log_error "未知命令: $1"
            show_help
            exit 1
            ;;
    esac
    
    log_info "操作完成"
}

# 执行主函数
main "$@" 