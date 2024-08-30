# -*- coding: utf-8 -*-
"""
Created on 2018/10/31 19:03

@author: 006547
"""
"""
这是一个范例，用于计算k日（交易日）收盘价的涨幅

本代码用于：
1）生成新的因子文件（一般只用一次）
2）将因子文件根据行情更新到最新日期（添加行），如有新股上市、则也需更新因子文件（添加列）
    （会被反复调用） —— 还未写好
"""


import DataAPI.DataToolkit as Dtk
import pandas as pd
import os
import datetime as dt
import platform
import numpy as np


def factor_generator(stock_list, start_date, end_date, factor_file_dir_path, factor_file_name, ma_k):
    ############################################
    # 以下是因子计算逻辑的部分，需要用户自定义 #
    # 计算因子时，最后应得到factor_data这个对象，类型应当是DataFrame，涵盖的时间段是start_date至end_date（前闭后闭）；
    # factor_data的每一列是相应的股票，每一行是每一天的因子值；
    # 最后factor_data的索引，建议与获得的原始行情的索引（index）一致，
    # 如通过reset_index撤销了原始行情的索引，那么不要删除'index'这一列，最后也不要设置别的索引。
    ############################################
    # 因为要计算相比ma_k个交易日前的收益率，这里往前回溯(ma_k+2)个交易日保险一些
    start_date_minus_k_2 = Dtk.get_n_days_off(start_date, -(ma_k+2))[0]
    # 获取后复权数据，是DataFrame，每一列的列名是股票代码，每一行的标签则是日期（例如20180829，是8位数的int）
    data_high: pd.DataFrame = Dtk.get_panel_daily_pv_df(stock_list, start_date_minus_k_2, end_date,
                                                         pv_type='high', adj_type='FORWARD')
    data_low: pd.DataFrame = Dtk.get_panel_daily_pv_df(stock_list, start_date_minus_k_2, end_date,
                                                             pv_type='low', adj_type='FORWARD')
    data_amount: pd.DataFrame = Dtk.get_panel_daily_pv_df(stock_list, start_date_minus_k_2, end_date,
                                                             pv_type='amt', adj_type='FORWARD')
    # 计算因子值
    data_high_div_low =data_high / data_low
    factor_data = np.tile(np.nan, [data_high.shape[0], data_high.shape[1]])
    for i in range(ma_k-1, factor_data.shape[0]):
        for k in range(factor_data.shape[1]):
            a = data_high_div_low.iloc[i-ma_k+1:i+1, k].values
            b = data_amount.iloc[i-ma_k+1:i+1, k].values
            temp_corr = np.corrcoef(a, b)[0, 1]
            factor_data[i, k] = temp_corr
    factor_data = pd.DataFrame(factor_data, index=data_high.index, columns=data_high.columns)
    # 保留start_date至end_date（前闭后闭）期间的数据
    factor_data = factor_data.loc[start_date: end_date].copy()
    ########################################
    # 因子计算逻辑到此为止，以下勿随意变更 #
    ########################################
    # 行情中获取原始的索引是20180829这种整形，保存因子文件时我们要转成timestamp；reset_index后，索引会变成普通的列'index'
    factor_data = factor_data.reset_index()
    date_list = Dtk.convert_date_or_time_int_to_datetime(factor_data['index'].tolist())
    timestamp_list = [i_date.timestamp() for i_date in date_list]
    factor_data['timestamp'] = timestamp_list
    # 将timestamp设为索引
    factor_data = factor_data.set_index(['timestamp'])
    # factor_data仅保留股票列表的列，删除其他无关的列
    factor_data = factor_data[stock_list].copy()
    if not factor_file_name[-3:] == ".h5":
        factor_file_name = factor_file_name + ".h5"
    file_full_path = os.path.join(factor_file_dir_path, factor_file_name)
    pd.set_option('io.hdf.default_format', 'table')
    store = pd.HDFStore(file_full_path)
    store.put("stock_list", pd.DataFrame(stock_list, columns=['code']))
    store.put("factor", factor_data, format="table")
    store.flush()
    store.close()
    print("Factor file", factor_file_name, "was created")


def main():
    stock_code_list = Dtk.get_complete_stock_list()
    if platform.system() == 'Windows':
        save_dir = "S:\\Apollo\\Factors\\"  # 保存于S盘的地址
    else:
        save_dir = "/app/data/006566/Apollo/Factors"  # 保存于XQuant的地址
    #################################################
    # 以下3行及产生因子的函数名需要自行改写 #
    #################################################
    istart_date_int = 20150101
    iend_date_int = 20180630
    for i_lag in [3, 5, 10, 20]:
        k = i_lag
        factor_name = "F_D_VolumeAmpCorr" + '_' + str(k)
        file_name = factor_name
        factor_generator(stock_code_list, istart_date_int, iend_date_int, save_dir, file_name, k)


if __name__ == '__main__':
    main()