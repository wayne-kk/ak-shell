#!/bin/bash
# 日志清理脚本 - 自动清理过期的日志文件

set -e

# 设置工作目录
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
cd "$PROJECT_DIR"

# 颜色定义
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

# 日志函数
log() {
    echo -e "${GREEN}[$(date '+%Y-%m-%d %H:%M:%S')]${NC} $1"
}

warn() {
    echo -e "${YELLOW}[$(date '+%Y-%m-%d %H:%M:%S')] WARN:${NC} $1"
}

error() {
    echo -e "${RED}[$(date '+%Y-%m-%d %H:%M:%S')] ERROR:${NC} $1"
}

# 清理函数
cleanup_logs() {
    log "开始清理日志文件..."
    
    if [[ ! -d "logs" ]]; then
        warn "logs目录不存在，跳过清理"
        return
    fi
    
    cd logs
    
    # 清理30天前的日志文件
    log "清理30天前的日志文件..."
    find . -name "*.log" -type f -mtime +30 -delete 2>/dev/null || true
    
    # 清理大于100MB的日志文件（保留最新的）
    log "检查过大的日志文件..."
    large_files=$(find . -name "*.log" -type f -size +100M 2>/dev/null || true)
    if [[ -n "$large_files" ]]; then
        warn "发现大型日志文件："
        echo "$large_files" | while read -r file; do
            size=$(du -h "$file" | cut -f1)
            warn "  $file ($size)"
        done
    fi
    
    # 只保留最近7天的cron日志
    log "清理旧的cron日志..."
    find . -name "cron_*.log" -type f -mtime +7 -delete 2>/dev/null || true
    
    # 压缩7天前的collector日志
    log "压缩旧的collector日志..."
    find . -name "collector_*.log" -type f -mtime +7 -exec gzip {} \; 2>/dev/null || true
    
    cd ..
    
    # 显示清理后的状态
    log "清理完成，当前日志目录状态："
    if [[ -d "logs" ]]; then
        ls -lah logs/ | head -20
        
        # 计算总大小
        total_size=$(du -sh logs/ | cut -f1)
        log "日志目录总大小: $total_size"
    fi
}

# 显示帮助信息
show_help() {
    echo "日志清理脚本"
    echo ""
    echo "用法: $0 [选项]"
    echo ""
    echo "选项:"
    echo "  --dry-run    仅显示将要删除的文件，不实际删除"
    echo "  --force      强制清理所有日志（保留今天的）"
    echo "  --help       显示此帮助信息"
    echo ""
    echo "默认行为:"
    echo "  - 删除30天前的日志文件"
    echo "  - 删除7天前的cron日志"
    echo "  - 压缩7天前的collector日志"
}

# 主函数
main() {
    case "${1:-}" in
        "--dry-run")
            log "模拟模式：显示将要清理的文件"
            cd logs 2>/dev/null || { warn "logs目录不存在"; exit 0; }
            echo "30天前的日志文件:"
            find . -name "*.log" -type f -mtime +30 2>/dev/null || echo "  无"
            echo "7天前的cron日志:"
            find . -name "cron_*.log" -type f -mtime +7 2>/dev/null || echo "  无"
            echo "7天前的collector日志:"
            find . -name "collector_*.log" -type f -mtime +7 2>/dev/null || echo "  无"
            ;;
        "--force")
            warn "强制清理模式！"
            read -p "确定要删除所有旧日志吗？(y/N): " -n 1 -r
            echo
            if [[ $REPLY =~ ^[Yy]$ ]]; then
                cleanup_logs
            else
                log "取消操作"
            fi
            ;;
        "--help"|"-h"|"")
            show_help
            ;;
        *)
            cleanup_logs
            ;;
    esac
}

# 执行主函数
main "$@" 