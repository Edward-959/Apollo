# -*- coding: utf-8 -*-
"""
Created on 2019/1/22
@author: 006566

(RANK(CORR(VWAP, VOLUME, 4)) * RANK(CORR(RANK(LOW), RANK(MEAN(VOLUME, 50)), 12)))
corr_df1 = CORR(VWAP, VOLUME, 4)
rank_df1 = RANK(corr_df1) = RANK(CORR(VWAP, VOLUME, 4))

mean_df = MEAN(VOLUME, 50)
rank_df4 = rank(mean_df)
rank_df3 = rank(low)
corr_df2 = CORR(rank_df3, rank_df4, 12) = CORR(RANK(LOW), RANK(MEAN(VOLUME, 50)), 12))
rank_df2 = rank(corr_df2) = RANK(CORR(RANK(LOW), RANK(MEAN(VOLUME, 50)), 12)))

ans_df = corr_df1 * rank_df2
"""

import DataAPI.DataToolkit as Dtk
import pandas as pd
import numpy as np
import os
import platform
import datetime as dt


def factor_generator(stock_list, start_date, end_date, factor_file_dir_path, factor_file_name):
    ############################################
    # 以下是因子计算逻辑的部分，需要用户自定义 #
    # 计算因子时，最后应得到factor_data这个对象，类型应当是DataFrame，涵盖的时间段是start_date至end_date（前闭后闭）；
    # factor_data的每一列是相应的股票，每一行是每一天的因子值；
    # 最后factor_data的索引，建议与获得的原始行情的索引（index）一致，
    # 如通过reset_index撤销了原始行情的索引，那么不要删除'index'这一列，最后也不要设置别的索引。
    ############################################
    t1 = dt.datetime.now()
    valid_date_minus_2 = Dtk.get_n_days_off(start_date, -(50 + 2))[0]
    amt_df = Dtk.get_panel_daily_pv_df(stock_list, valid_date_minus_2, end_date, pv_type='amt')
    volume_df = Dtk.get_panel_daily_pv_df(stock_list, valid_date_minus_2, end_date, pv_type='volume')
    low_df = Dtk.get_panel_daily_pv_df(stock_list, valid_date_minus_2, end_date, pv_type='low')
    t2 = dt.datetime.now()
    print("Raw data was downloaded successfully and it cost", t2 - t1)
    # 计算因子值
    vwap_df: pd.DataFrame = amt_df / volume_df
    corr_df1 = vwap_df.rolling(4).corr(volume_df)
    mean_df = volume_df.rolling(50).mean()
    rank_df3 = mean_df.rank(axis=1)
    rank_df4 = low_df.rank(axis=1)
    corr_df2 = rank_df4.rolling(12).corr(rank_df3)
    rank_df2 = corr_df2.rank(axis=1)
    ans_df = corr_df1 * rank_df2
    factor_data = ans_df.ewm(span=3).mean()
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
    if platform.system() == 'Windows':
        istart_date_int = 20141201
        iend_date_int = 20180630
    else:
        istart_date_int = 20130101
        iend_date_int = 20180630
    factor_name = "F_D_GTJA179_ema3"
    file_name = factor_name
    factor_generator(stock_code_list, istart_date_int, iend_date_int, save_dir, file_name)


if __name__ == '__main__':
    main()
