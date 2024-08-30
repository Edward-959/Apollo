# -*- coding: utf-8 -*-
"""
@author: 006566
Created on 2019/02/19
"""

import DataAPI.DataToolkit as Dtk
from importlib import import_module
import os
import pandas as pd
import platform
import time

# -----------------------------------------------------------------------------
# 需要更新的因子名写在下列列表中，内容依次是模块名、参数列表、因子文件名
# 这部分可以写成类似AlgoConfig的形式
factors_need_updated_list = \
    [
        ["DataDailyMinTwap", {}, "Data_twap.h5"],
        ["DataDailyMinBuyTwap", {}, "Data_buy_twap.h5"],
        ["DataDailyMinBuyTwapFillRate", {}, "Data_buy_twap_fill_rate.h5"],
        ["DataDailyMinBuyableVolume", {}, "Data_buyable_volume.h5"],
        ["DataDailyMinSellTwap", {}, "Data_sell_twap.h5"],
        ["DataDailyMinSellTwapFillRate", {}, "Data_sell_twap_fill_rate.h5"],
        ["DataDailyMinSellableVolume", {}, "Data_sellable_volume.h5"]
    ]

today_int = int(time.strftime("%Y%m%d"))
day_before_yesterday = Dtk.get_n_days_off(today_int, -3)[0]
yesterday = Dtk.get_n_days_off(today_int, -3)[1]
start_date = day_before_yesterday
end_date = yesterday

if platform.system() == "Windows":
    alpha_data_root_path = "S:/xquant_data_backup"
elif os.system("nvidia-smi") == 0:
    alpha_data_root_path = "/vipzrz/Apollo"
else:
    alpha_data_root_path = "/app/data/666889/Apollo"

# ------------需要设定的部分到此为止-----------------------------------------
data_path_dir = os.path.join(alpha_data_root_path, "AlphaDataBase")

if not os.path.exists(alpha_data_root_path):
    os.mkdir(alpha_data_root_path)
if not os.path.exists(data_path_dir):
    os.mkdir(data_path_dir)

for i_factor in factors_need_updated_list:
    file_name = i_factor[2]
    factor_module = import_module("DataBase." + i_factor[0])
    output_file_path = os.path.join(data_path_dir, file_name)
    valid_start_date = start_date
    class_name = getattr(factor_module, i_factor[0])
    # 初始化因子类
    if i_factor[0][0:12] == "DataDailyMin":
        complete_stock_list_and_index = Dtk.get_complete_stock_list()
        complete_stock_list_and_index.extend(['000016.SH', '000300.SH', '000905.SH', '000906.SH', '000001.SH',
                                              '399001.SZ', '399006.SZ'])
        factor_obj = class_name(alpha_data_root_path, complete_stock_list_and_index, start_date, end_date,
                                i_factor[1])
    else:
        factor_obj = class_name(alpha_data_root_path, start_date, end_date, i_factor[1])
    # 计算因子
    ans_df = factor_obj.factor_calc()
    # index是timestamp, 转为date_int
    ans_df = Dtk.convert_df_index_type(ans_df, 'timestamp', 'date_int')
    # 如没有因子文件则创设之
    if not os.path.exists(output_file_path):
        pd.set_option('io.hdf.default_format', 'table')
        store = pd.HDFStore(output_file_path)
        stock_list = list(ans_df.columns)
        store.put("stock_list", pd.DataFrame(stock_list, columns=['code']))
        store.put("factor", ans_df, format="table")
        store.flush()
        store.close()
        print("Factor file", file_name, "was created.")
    # 如已有因子文件，则更新之；如遇日期重叠的部分，以新计算的为准
    else:
        store = pd.HDFStore(output_file_path)
        original_data_df = store.select("/factor")
        if original_data_df.index[-1] < ans_df.index[0]:
            ans_df2 = pd.concat([original_data_df, ans_df])
        else:
            ans_df2 = pd.concat([original_data_df.loc[:ans_df.index[0] - 1], ans_df])
        new_stock_list = list(ans_df2.columns)
        if new_stock_list.__len__() > list(original_data_df.columns).__len__():
            store.put("stock_list", pd.DataFrame(new_stock_list, columns=['code']))
        store.put("factor", ans_df2, format="table")
        store.flush()
        store.close()
        print("Factor_file", file_name, "was updated to", end_date, ".")
