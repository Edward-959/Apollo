# -*- coding: utf-8 -*-
"""
@author: 006688
根据FactorDailyStdId2中回归得到的残差，分别求出上行波动率和下行波动率，两者相减
"""

import DataAPI.DataToolkit as Dtk
import pandas as pd
import numpy as np
import os
import datetime as dt
import platform


def factor_generator(stock_list, start_date, end_date, factor_file_dir_path, factor_file_name, n):
    print("Start downloading raw data for calculating the factor")
    t1 = dt.datetime.now()
    ############################################
    # 以下是因子计算逻辑的部分，需要用户自定义 #
    # 计算因子时，最后应得到factor_data这个对象，类型应当是DataFrame，涵盖的时间段是start_date至end_date（前闭后闭）；
    # factor_data的每一列是相应的股票，每一行是每一天的因子值；
    # 最后factor_data的索引，建议与获得的原始行情的索引（index）一致，
    # 如通过reset_index撤销了原始行情的索引，那么不要删除'index'这一列，最后也不要设置别的索引。
    ############################################
    start_date_minus_lag_2 = Dtk.get_n_days_off(start_date, -(n + 2))[0]
    # 获取close的后复权数据，是DataFrame，每一列的列名是股票代码，每一行的标签则是日期（例如20180829，是8位数的int）
    stock_pct_chg: pd.DataFrame = Dtk.get_panel_daily_pv_df(stock_list, start_date_minus_lag_2, end_date,
                                                            pv_type='pct_chg')
    stock_mkt_cap: pd.DataFrame = Dtk.get_panel_daily_info(stock_list, start_date_minus_lag_2, end_date, 'mkt_cap_ard')
    stock_pb: pd.DataFrame = Dtk.get_panel_daily_info(stock_list, start_date_minus_lag_2, end_date, 'pb_lf')
    stock_suspend: pd.DataFrame = Dtk.get_panel_daily_info(stock_list, start_date_minus_lag_2, end_date, 'filter_suspend')
    index_close: pd.DataFrame = Dtk.get_panel_daily_pv_df(['000300.SH'], start_date_minus_lag_2, end_date,
                                                          pv_type='close')
    t2 = dt.datetime.now()
    print("Raw data was downloaded successfully and it cost", t2 - t1)
    # 计算因子值
    index_pct_chg = (index_close / index_close.shift(1) - 1) * 100
    stock_mkt_cap_rank = stock_mkt_cap.shift(1)[stock_suspend == 1].rank(axis=1)
    stock_pb[stock_pb <= 0] = np.nan
    stock_pb_rank = stock_pb.shift(1)[stock_suspend == 1].rank(axis=1)
    stock_num = stock_suspend.sum(axis=1)
    stock_num_pb = stock_suspend[stock_pb > 0].sum(axis=1)
    SMB = stock_pct_chg[stock_mkt_cap_rank.apply(lambda x: x <= round(stock_num * 0.3))].mean(axis=1) - \
        stock_pct_chg[stock_mkt_cap_rank.apply(lambda x: x > round(stock_num * 0.7))].mean(axis=1)
    HML = stock_pct_chg[stock_pb_rank.apply(lambda x: x > round(stock_num_pb * 0.7))].mean(axis=1) - \
        stock_pct_chg[stock_pb_rank.apply(lambda x: x <= round(stock_num_pb * 0.3))].mean(axis=1)
    trading_days = Dtk.get_trading_day(start_date, end_date)
    factor_data = stock_pct_chg.copy()
    factor_data[:] = np.nan
    for date in trading_days:
        i = stock_pct_chg.index.tolist().index(date)
        stock_pct_chg_i = stock_pct_chg.iloc[i-n+1: i+1]
        index_pct_chg_i = index_pct_chg.iloc[i-n+1: i+1]
        index_pct_chg_i = index_pct_chg_i['000300.SH']
        SMB_i = SMB.iloc[i-n+1: i+1]
        HML_i = HML.iloc[i-n+1: i+1]
        ff = np.vstack([np.array(index_pct_chg_i), np.array(SMB_i), np.array(HML_i), np.ones(len(index_pct_chg_i))]).T
        for code in stock_pct_chg_i.columns.tolist():
            if not stock_pct_chg_i[code].isnull().any():
                reg_result = np.linalg.lstsq(ff, stock_pct_chg_i[code], rcond=None)
                factor_data.at[date, code] = np.std(np.maximum(np.array(stock_pct_chg_i[code]) - np.dot(ff, reg_result[0]), 0)) - \
                                             np.std(np.minimum(np.array(stock_pct_chg_i[code]) - np.dot(ff, reg_result[0]), 0))
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
    istart_date_int = 20180601
    iend_date_int = 20180630
    for n in [20]:
        factor_name = "F_D_StdId2UmD_" + str(n)
        file_name = factor_name
        factor_generator(stock_code_list, istart_date_int, iend_date_int, save_dir, file_name, n)


if __name__ == '__main__':
    main()
