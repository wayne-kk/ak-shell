# AKShare 新闻相关 API 汇总

## 概述

本文档汇总了 AKShare 库中所有与新闻、资讯、公告、研报相关的 API 接口，方便开发者快速查找和使用。

## 主要分类

### 1. 股票新闻

#### 1.1 东方财富个股新闻
```python
import akshare as ak

# 获取平安银行(000001)的最近100条新闻
df = ak.stock_news_em(symbol='000001')
print(df.columns)  # ['关键词', '新闻标题', '新闻内容', '发布时间', '文章来源', '新闻链接']
```

#### 1.2 股票资讯（主站）
```python
# 获取主要股票资讯
df = ak.stock_news_main_cx()
# 返回主要财经媒体的股票新闻
```

#### 1.3 微博股票情绪
```python
# 获取股票微博讨论数据
df = ak.stock_js_weibo_report()
# 包含股票在微博上的讨论热度和情绪分析
```

### 2. 宏观经济新闻

#### 2.1 百度经济数据
```python
# 获取指定日期的经济数据新闻
df = ak.news_economic_baidu(date='20241107')
# 返回当日重要经济数据和事件
```

#### 2.2 央视新闻联播
```python
# 获取新闻联播文字稿
df = ak.news_cctv(date='20240424')
# 返回指定日期的新闻联播完整文字稿
```

### 3. 公告和报告

#### 3.1 股票公告
```python
# 获取股票公告报告
df = ak.stock_notice_report()
# 包含上市公司的各类公告信息
```

#### 3.2 信息披露报告
```python
# 获取A股信息披露报告
df = ak.stock_zh_a_disclosure_report_cninfo()
# 从中国证监会指定披露网站获取报告

# 获取股票报告披露
df = ak.stock_report_disclosure()
# 包含定期报告、临时公告等
```

#### 3.3 研究报告
```python
# 获取个股研报
df = ak.stock_research_report_em(symbol='000001')
# 返回指定股票的研究报告列表
```

### 4. 财务报告

#### 4.1 资产负债表
```python
# 获取资产负债表(按报告期)
df = ak.stock_balance_sheet_by_report_em(symbol='000001')
# 退市股票资产负债表
df = ak.stock_balance_sheet_by_report_delisted_em(symbol='000001')
```

#### 4.2 利润表
```python
# 获取利润表(按报告期)
df = ak.stock_profit_sheet_by_report_em(symbol='000001')
# 退市股票利润表
df = ak.stock_profit_sheet_by_report_delisted_em(symbol='000001')
```

#### 4.3 现金流量表
```python
# 获取现金流量表(按报告期)
df = ak.stock_cash_flow_sheet_by_report_em(symbol='000001')
# 退市股票现金流量表
df = ak.stock_cash_flow_sheet_by_report_delisted_em(symbol='000001')
```

#### 4.4 港股和美股财报
```python
# 港股财务报告
df = ak.stock_financial_hk_report_em()

# 美股财务报告
df = ak.stock_financial_us_report_em()

# 新浪财务报告
df = ak.stock_financial_report_sina()
```

### 5. 基金相关

#### 5.1 基金公告
```python
# 基金人事公告
df = ak.fund_announcement_personnel_em()

# 基金资产配置报告
df = ak.fund_report_asset_allocation_cninfo()

# 基金行业配置报告
df = ak.fund_report_industry_allocation_cninfo()

# 基金股票持仓报告
df = ak.fund_report_stock_cninfo()
```

#### 5.2 基金持股
```python
# 股票基金持股
df = ak.stock_report_fund_hold()

# 基金持股明细
df = ak.stock_report_fund_hold_detail()
```

### 6. 期货新闻

```python
# 上海金属交易所新闻
df = ak.futures_news_shmet()
```

### 7. 加密货币新闻

```python
# 比特币持仓报告
df = ak.crypto_bitcoin_hold_report()
```

### 8. 专业新闻

#### 8.1 情绪指数
```python
# 新闻情绪指数
df = ak.index_news_sentiment_scope()
```

#### 8.2 交易提醒
```python
# 分红派息提醒
df = ak.news_trade_notify_dividend_baidu()

# 停牌复牌提醒  
df = ak.news_trade_notify_suspend_baidu()

# 报告时间提醒
df = ak.news_report_time_baidu()
```

#### 8.3 宏观报告
```python
# 中国黄金报告
df = ak.macro_china_au_report()
```

## 使用示例

### 综合新闻采集器

```python
import akshare as ak
import pandas as pd
from datetime import datetime, timedelta

class NewsCollector:
    """新闻数据采集器"""
    
    def __init__(self):
        self.today = datetime.now().strftime('%Y%m%d')
    
    def collect_stock_news(self, stock_code: str):
        """采集个股新闻"""
        try:
            news_df = ak.stock_news_em(symbol=stock_code)
            research_df = ak.stock_research_report_em(symbol=stock_code)
            
            return {
                'news': news_df,
                'research': research_df
            }
        except Exception as e:
            print(f"采集股票 {stock_code} 新闻失败: {e}")
            return None
    
    def collect_macro_news(self):
        """采集宏观新闻"""
        try:
            cctv_news = ak.news_cctv(date=self.today)
            economic_news = ak.news_economic_baidu(date=self.today)
            
            return {
                'cctv': cctv_news,
                'economic': economic_news
            }
        except Exception as e:
            print(f"采集宏观新闻失败: {e}")
            return None
    
    def collect_announcements(self):
        """采集公告信息"""
        try:
            notices = ak.stock_notice_report()
            disclosures = ak.stock_report_disclosure()
            
            return {
                'notices': notices,
                'disclosures': disclosures
            }
        except Exception as e:
            print(f"采集公告信息失败: {e}")
            return None

# 使用示例
collector = NewsCollector()

# 采集平安银行新闻
stock_news = collector.collect_stock_news('000001')
if stock_news:
    print("平安银行最新新闻:")
    print(stock_news['news'].head())

# 采集宏观新闻
macro_news = collector.collect_macro_news()
if macro_news:
    print("今日宏观新闻:")
    print(macro_news['economic'].head())
```

## 数据字段说明

### 股票新闻字段
- **关键词**: 股票代码
- **新闻标题**: 新闻标题
- **新闻内容**: 新闻正文内容
- **发布时间**: 新闻发布时间
- **文章来源**: 新闻来源媒体
- **新闻链接**: 原文链接

### 央视新闻字段
- **date**: 日期
- **title**: 新闻标题
- **content**: 新闻内容

## 注意事项

1. **频率限制**: 避免过于频繁的API调用，建议添加延时
2. **数据时效性**: 某些接口的数据可能有延迟
3. **错误处理**: 建议添加异常处理机制
4. **数据清洗**: 返回的数据可能需要进一步清洗和处理

## 建议的采集策略

1. **定时采集**: 建议每小时或每天定时采集新闻数据
2. **增量更新**: 避免重复采集已有数据
3. **分类存储**: 按新闻类型分别存储到不同数据表
4. **关键词提取**: 对新闻内容进行关键词提取和情感分析
5. **去重处理**: 对相同内容的新闻进行去重

## 数据库存储建议

```sql
-- 股票新闻表
CREATE TABLE stock_news (
    id BIGSERIAL PRIMARY KEY,
    stock_code VARCHAR(10),
    title TEXT,
    content TEXT,
    publish_time TIMESTAMP,
    source VARCHAR(100),
    url TEXT,
    create_time TIMESTAMP DEFAULT NOW(),
    UNIQUE(stock_code, title, publish_time)
);

-- 宏观新闻表  
CREATE TABLE macro_news (
    id BIGSERIAL PRIMARY KEY,
    news_type VARCHAR(50), -- cctv, economic, etc
    title TEXT,
    content TEXT,
    publish_time TIMESTAMP,
    source VARCHAR(100),
    create_time TIMESTAMP DEFAULT NOW()
);

-- 研报表
CREATE TABLE research_reports (
    id BIGSERIAL PRIMARY KEY,
    stock_code VARCHAR(10),
    title TEXT,
    institution VARCHAR(100),
    analyst VARCHAR(100),
    rating VARCHAR(50),
    target_price DECIMAL(10,2),
    publish_time TIMESTAMP,
    url TEXT,
    create_time TIMESTAMP DEFAULT NOW()
);
```

这样您就可以全面地使用 AKShare 的新闻相关功能来构建完整的金融资讯系统了！ 