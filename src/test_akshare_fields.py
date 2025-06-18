import akshare as ak
import pandas as pd
from datetime import datetime

print("\n==== 股票基础信息 ====")
try:
    df = ak.stock_info_a_code_name()
    print("字段:", list(df.columns))
    print(df.head())
except Exception as e:
    print("股票基础信息接口异常:", e)

print("\n==== 股票日线行情 ====")
try:
    # 以000001为例，近5天
    df = ak.stock_zh_a_hist(symbol="000001", period="daily", start_date=(datetime.now().strftime('%Y%m%d')), end_date=(datetime.now().strftime('%Y%m%d')))
    print("字段:", list(df.columns))
    print(df.head())
except Exception as e:
    print("股票日线行情接口异常:", e)

print("\n==== 指数日线行情 ====")
try:
    df = ak.stock_zh_index_daily(symbol="sh000001")
    print("字段:", list(df.columns))
    print(df.head())
except Exception as e:
    print("指数日线行情接口异常:", e)

print("\n==== 交易日历 ====")
try:
    df = ak.tool_trade_date_hist_sina()
    print("字段:", list(df.columns))
    print(df.head())
except Exception as e:
    print("交易日历接口异常:", e) 