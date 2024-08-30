# -*- coding: utf-8 -*-
"""
Created on 2019/1/22
@author: 006566

(-1 * ((RANK(((SIGN((CLOSE - DELAY(CLOSE, 1))) + SIGN((DELAY(CLOSE, 1) - DELAY(CLOSE, 2))))
+ SIGN((DELAY(CLOSE, 2) - DELAY(CLOSE, 3))))) * SUM(VOLUME, 5)) / SUM(VOLUME, 20))

diff1_sign = SIGN((CLOSE - DELAY(CLOSE, 1)))
diff2_sign = SIGN((DELAY(CLOSE, 1) - DELAY(CLOSE, 2)))
diff3_sign = SIGN((DELAY(CLOSE, 2) - DELAY(CLOSE, 3)))

(-1 * ((RANK(((diff1_sign + diff2_sign) + diff_3_sign)) * SUM(VOLUME, 5)) / SUM(VOLUME, 20))

sum_diff = ((diff1_sign + diff2_sign) + diff3_sign))

ans_df = (RANK(sum_diff) * SUM(VOLUME, 5)) / SUM(VOLUME, 20))
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
    start_date_minus_k_2 = Dtk.get_n_days_off(start_date, -(20 + 2))[0]
    volume_df = Dtk.get_panel_daily_pv_df(stock_list, start_date_minus_k_2, end_date, pv_type='volume')
    close = Dtk.get_panel_daily_pv_df(stock_list, start_date_minus_k_2, end_date, pv_type='close', adj_type='FORWARD')
    t2 = dt.datetime.now()
    print("Raw data was downloaded successfully and it cost", t2 - t1)
    # 计算因子值
    diff1_sign = np.sign(close - close.shift(1))
    diff2_sign = np.sign(close.shift(1) - close.shift(2))
    diff3_sign = np.sign(close.shift(2) - close.shift(3))
    sum_diff = diff1_sign + diff2_sign + diff3_sign
    rank_df = sum_diff.rank(axis=1)
    sum_vol5 = volume_df.rolling(5).sum()
    sum_vol20 = volume_df.rolling(20).sum()
    ans_df = rank_df * sum_vol5 / sum_vol20
    factor_data = -1 * ans_df
    factor_data = factor_data.ewm(span=3).mean()
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
    factor_name = "F_D_GTJA48_ema3"
    file_name = factor_name
    factor_generator(stock_code_list, istart_date_int, iend_date_int, save_dir, file_name)


if __name__ == '__main__':
    main()
