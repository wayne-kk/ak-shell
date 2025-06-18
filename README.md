# A 股数据采集系统

基于 Prisma + AKShare + Supabase 的 A 股数据采集与管理系统，支持历史数据采集和定时增量更新。

## 功能特性

- ✅ **完整的数据模型**: 股票基础信息、日线行情、财务指标、指数数据等
- ✅ **自动数据采集**: 支持历史数据批量采集和增量更新
- ✅ **定时任务调度**: 自动化数据更新和维护
- ✅ **数据质量保证**: 异常处理、重试机制、数据清洗
- ✅ **Supabase 存储**: 云端数据库存储和查询
- ✅ **灵活配置**: 支持环境变量配置

## 数据表结构

### 核心表

- `stock_basic` - 股票基础信息
- `daily_quote` - 日线行情数据
- `index_data` - 指数数据
- `trade_calendar` - 交易日历

### 财务表

- `financial_indicator` - 财务指标
- `financial_statement` - 财务报表

### 资金流向表

- `money_flow` - 个股资金流向
- `northbound_capital` - 北向资金

## 系统架构

本系统采用以下技术栈：

- **数据源**: AKShare - 免费的 A 股数据接口
- **数据库**: Supabase - 云端 PostgreSQL 数据库
- **数据模型**: Prisma - 现代化 ORM 和数据建模
- **采集器**: Python - 自定义数据采集和调度系统

## 快速开始

### 1. 环境准备

```bash
# 安装Python依赖
pip install -r requirements.txt

# 复制环境配置文件
cp env.example .env

# 编辑 Supabase 连接配置
vim .env
```

#### Supabase 配置

1. 在 [Supabase](https://supabase.com) 创建项目
2. 在项目设置 -> API 中获取：
   - `Project URL` (SUPABASE_URL)
   - `service_role key` (SUPABASE_SERVICE_ROLE_KEY)
3. 在 `.env` 文件中配置：

```bash
SUPABASE_URL="https://your-project-ref.supabase.co"
SUPABASE_SERVICE_ROLE_KEY="your-service-role-key"
```

#### 测试 Supabase 连接

```bash
# 测试连接
python3 test_supabase_connection.py
```

### 2. Supabase 数据库设置

```bash
# 生成Prisma客户端
npm run db:generate

# 推送数据库结构到 Supabase
npm run db:push
```

### 3. 数据采集

```bash
# 给脚本执行权限
chmod +x scripts/collect_history.sh

# 采集近一年历史数据
./scripts/collect_history.sh one_year

# 采集指定时间范围数据
./scripts/collect_history.sh custom 20230101 20231231

# 采集最新数据
./scripts/collect_history.sh latest

# 启动定时任务
./scripts/collect_history.sh schedule
```

## 使用说明

### 命令行工具

#### 主采集脚本 (`src/main.py`)

```bash
# 采集近一年历史数据
python3 src/main.py one_year

# 采集指定时间范围历史数据
python3 src/main.py history --start-date 20230101 --end-date 20231231

# 采集最新数据（增量更新）
python3 src/main.py latest

# 采集单个股票数据
python3 src/main.py stock --stock-code 000001 --start-date 20230101
```

#### Shell 脚本 (`scripts/collect_history.sh`)

```bash
# 查看帮助
./scripts/collect_history.sh help

# 采集近一年数据
./scripts/collect_history.sh one_year

# 自定义时间范围
./scripts/collect_history.sh custom 20230101 20231231

# 最新数据更新
./scripts/collect_history.sh latest

# 启动定时任务
./scripts/collect_history.sh schedule
```

### 定时任务

系统支持自动化定时任务：

- **每日 16:30**: 采集最新行情数据（收盘后）
- **每周日 02:00**: 更新股票基础信息
- **每小时**: 系统健康检查

```bash
# 启动定时任务调度器
python3 src/scheduler.py
```

### 数据采集流程

1. **交易日历** - 获取交易日历信息
2. **股票基础信息** - 获取所有 A 股基础信息
3. **指数数据** - 采集主要指数历史数据
4. **股票行情** - 逐个采集所有股票的历史行情

## 配置说明

### 环境变量 (`.env`)

```bash
# Supabase 数据库配置
SUPABASE_URL="https://your-project-ref.supabase.co"
SUPABASE_SERVICE_ROLE_KEY="your-service-role-key"

# 采集配置（可选）
AKSHARE_TIMEOUT=30
AKSHARE_RETRY_COUNT=3
AKSHARE_RATE_LIMIT=10

# 时区配置（可选）
TIMEZONE="Asia/Shanghai"
```

### 数据采集配置

系统使用 AKShare 库进行数据采集，支持以下配置：

- **请求超时**: 设置 API 请求超时时间
- **重试次数**: 失败重试机制
- **限流设置**: 控制请求频率
- **时区设置**: 数据时间处理

## 性能优化

### 数据采集优化

- **API 限流**: 控制请求频率避免被限制
- **重试机制**: 失败自动重试，提高成功率
- **批量处理**: 批量插入提高数据库性能
- **增量更新**: 只采集新数据，避免重复

### Supabase 优化

- **RLS 策略**: 合理配置行级安全策略
- **批量操作**: 分批插入和更新数据
- **UPSERT**: 使用 Supabase 的 upsert 功能避免重复数据
- **索引优化**: 对查询字段建立合适的索引
- **实时订阅**: 利用 Supabase 的实时功能

## 数据质量

### 数据清洗

- 无效值处理
- 异常数据过滤
- 数据类型转换
- 字符串标准化

### 数据验证

- 必填字段检查
- 数据范围验证
- 逻辑一致性检查
- 重复数据处理

## 监控与日志

### 日志系统

- 分级日志记录
- 文件自动轮转
- 结构化日志格式
- 异常详细追踪

### 监控指标

- 采集成功率
- 数据质量指标
- 系统性能指标
- 错误统计分析

## 故障排除

### 常见问题

1. **Supabase 连接失败**

   ```bash
   # 测试 Supabase 连接
   python3 src/test_supabase.py
   ```

2. **AKShare API 错误**

   ```bash
   # 检查网络连接和API限制
   python3 -c "import akshare as ak; print(ak.stock_info_a_code_name().head())"
   ```

3. **依赖安装失败**
   ```bash
   # 升级pip并重新安装
   pip install --upgrade pip
   pip install -r requirements.txt
   ```

### 日志查看

```bash
# 查看采集日志
tail -f logs/collector_$(date +%Y-%m-%d).log

# 查看调度器日志
tail -f logs/scheduler_$(date +%Y-%m-%d).log
```

## 扩展开发

### 添加新的数据源

1. 在 `src/collectors.py` 中创建新的采集器类
2. 在 `prisma/schema.prisma` 中定义数据模型
3. 在 `src/main.py` 中集成新的采集逻辑

### 自定义数据处理

1. 扩展 `BaseCollector` 类
2. 实现自定义的数据清洗逻辑
3. 添加特定的验证规则

## 许可证

MIT License

## 支持

如有问题请提交 Issue 或联系维护者。
