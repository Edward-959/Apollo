import datetime as dt
from DataAPI import load, load_factor
import DataAPI.DataToolkit as Dtk

start = '2018-06-05 09:20:00'
end = '2018-06-30 15:00:00'
date_time_start = dt.datetime.strptime(start, '%Y-%m-%d %H:%M:%S')
date_time_end = dt.datetime.strptime(end, '%Y-%m-%d %H:%M:%S')

# # 读取多个分钟级因子的范例
# factor_list = ["Factor_MinGrowth180_20180601_20180630_fast", "TagMinHighGrowth242_20180601_20180630_fast"]
# stock_list = ["601318.SH", "000858.SZ", "600276.SH"]
# x_array_data = load(factor_list, stock_list, date_time_start, date_time_end)
# print(x_array_data)
#
# # 读取单个分钟级因子的范例
# factor_name = "Factor_MinGrowth180_20180601_20180630_fast"
# stock_list2 = Dtk.get_complete_stock_list()
# single_factor_data = load_factor(factor_name, stock_list2, date_time_start, date_time_end)
# print(single_factor_data)

# 读取单个日级别因子的范例，其实和读取分钟因子的方法是一样的
factor_name = "F_D_CloseCutGrowth_1"
stock_list2 = Dtk.get_complete_stock_list()
single_daily_factor_data = load_factor(factor_name, stock_list2, date_time_start, date_time_end)
print(single_daily_factor_data)
