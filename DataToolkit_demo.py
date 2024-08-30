import DataAPI.DataToolkit as Dtk
from DataAPI import load_factor
import datetime as dt
import pandas as pd

# 取完整股票列表
complete_stock_list = Dtk.get_complete_stock_list()

# 取alpha_universe
df0 = Dtk.get_panel_daily_info(complete_stock_list, 20150701, 20180630, 'alpha_universe')
print(df0)
# 统计一下每天有多少股票
df0_1 = df0.sum(axis=1)
print(df0_1)

# 取risk_universe
df1 = Dtk.get_panel_daily_info(complete_stock_list, 20150701, 20180630, 'risk_universe')
print(df1)
# 统计一下每天有多少股票
df1_1 = df1.sum(axis=1)
print(df1_1)

# 取所有股票每天所属的中信一级行业 （关键字就叫'industry3'，来自金工团队，不知道3是什么意思）
df2 = Dtk.get_panel_daily_info(complete_stock_list, 20150701, 20180630, 'industry3')
print(df2)
# 统计一下每天有多少个行业（应该都是31，原本中信一级行业是有29个行业，因为非银金融太大了，又拆成了3个子行业）
df2_1 = df2.nunique(axis=1)
print(df2_1)
# 每个行业的对应表可参考 http://phabricator.htsc.com/w/alpha-中信一级行业对照表/

# 取每天所有沪深300的成分股（是成分股则为1，不是则为0）
df3 = Dtk.get_panel_daily_info(complete_stock_list, 20150701, 20180630, 'index_300')
print(df3)
# 统计一下每天有多少股票（发现不是每天都是300，说明complete_stock_list有股票缺失，还需补齐）
df3_1 = df3.sum(axis=1)
print(df3_1)
# 取某天沪深300所有成分股、变为list，例如取20180102吧
df3_20180102 = df3.loc[20180102]
df3_20180102 = list(df3_20180102[df3_20180102 == 1].index)

# 取每天沪深300成分股的权重
df3b = Dtk.get_panel_daily_info(complete_stock_list, 20150701, 20180630, 'index_weight_hs300')
print(df3b)
df3b_1 = df3b.sum(axis=1)
# 若要取某天沪深300所有成分股的权重，例如20180102，可以这样（会得到一个长度为300的Series，index是股票代码、值是权重）
df3b_20180102 = df3b.loc[20180102]
df3b_20180102 = df3b_20180102[df3b_20180102 > 0]
# 如要变成list的话，可以这样，两个list的内容是对应的
df3b_code_list = list(df3b_20180102.index)
df3b_weight_list = list(df3b_20180102.values)
print(df3b_weight_list)


# 取每天所有中证500的成分股（是成分股则为1，不是则为0）
df4 = Dtk.get_panel_daily_info(complete_stock_list, 20150701, 20180630, 'index_500')
df4b = Dtk.get_panel_daily_info(complete_stock_list, 20150701, 20180630, 'index_weight_zz500')
print(df4b)
# 取每天所有上证50的成分股（是成分股则为1，不是则为0）
df5 = Dtk.get_panel_daily_info(complete_stock_list, 20150701, 20180630, 'index_50')
df5b = Dtk.get_panel_daily_info(complete_stock_list, 20150701, 20180630, 'index_weight_sh50')
print(df5b)

# 按stock universe过滤因子值，先取因子值
factor = load_factor('F_D_CloseCutGrowth_20', complete_stock_list, Dtk.convert_date_or_time_int_to_datetime(20150701),
                     Dtk.convert_date_or_time_int_to_datetime(20180630))
# stock_universe_df是一个矩阵，值为1或0，如某只股票在某日在股票池内则值为1、否则为0
stock_universe_df = Dtk.get_panel_daily_info(complete_stock_list, 20150701, 20180630, info_type='index_300',
                                             output_index_type='timestamp')
# factor_data_df 乘以 stock_universe_df 再除以 stock_universe_df，就会把不在股票池内的因子值调整为nan
factor2 = factor * stock_universe_df / stock_universe_df
# 可以比较一下factor 和 factor2


df6 = Dtk.get_panel_daily_info(complete_stock_list, 20150101, 20180630, 'adjfactor')
print(df6)

df7 = Dtk.get_panel_daily_info(complete_stock_list, 20150101, 20180630, 'apturn')
print(df7)

df8 = Dtk.get_panel_daily_info(complete_stock_list, 20150101, 20180630, 'Beta')
print(df8)

df9 = Dtk.get_panel_daily_info(complete_stock_list, 20150101, 20180630, 'Beta')
print(df9)

df10 = Dtk.get_panel_daily_info(complete_stock_list, 20150101, 20180630, 'yoyroe')
print(df10)

stock_code_list = Dtk.get_complete_stock_list()
info1 = Dtk.get_stock_latest_info(stock_code_list, 'Listing_date')  # 批量获取上市日期，输出为字典
info2 = Dtk.get_stock_latest_info(stock_code_list, 'Delisting_date')  # 批量获取退市日期，输出为字典

# twap超额收益标签####
start_date = 20150101
end_date = 20180630
holding_period = 1
benchmark = "000300.SH"

valid_end_date = Dtk.get_n_days_off(end_date, holding_period + 2)[-1]
complete_stock_list = Dtk.get_complete_stock_list()
price_df = Dtk.get_panel_daily_pv_df(complete_stock_list, start_date, valid_end_date,
                                     pv_type='twap', adj_type='FORWARD')
benchmark_price_df = Dtk.get_panel_daily_pv_df([benchmark], start_date, valid_end_date,
                                               pv_type='twap')
return_rate_df = price_df.shift(-holding_period) / price_df - 1
return_rate_benchmark_df = benchmark_price_df.shift(-holding_period) / benchmark_price_df - 1
excess_return_array = return_rate_df.values - return_rate_benchmark_df.values
excess_return_df = pd.DataFrame(excess_return_array, price_df.index, price_df.columns)
#############################
