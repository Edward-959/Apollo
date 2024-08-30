# -*- coding: utf-8 -*-
"""
@author: 006688
((((SUM(CLOSE,7)/7)-CLOSE))+((CORR(VWAP,DELAY(CLOSE,5),230))))
加号前是价格的差，未去量纲，加号后是相关系数
"""

import DataAPI.DataToolkit as Dtk
import pandas as pd
import os
import datetime as dt
import platform


def factor_generator(stock_list, start_date, end_date, factor_file_dir_path, factor_file_name, para1, para2, para3):
    print("Start downloading raw data for calculating the factor")
    t1 = dt.datetime.now()
    ############################################
    # 以下是因子计算逻辑的部分，需要用户自定义 #
    # 计算因子时，最后应得到factor_data这个对象，类型应当是DataFrame，涵盖的时间段是start_date至end_date（前闭后闭）；
    # factor_data的每一列是相应的股票，每一行是每一天的因子值；
    # 最后factor_data的索引，建议与获得的原始行情的索引（index）一致，
    # 如通过reset_index撤销了原始行情的索引，那么不要删除'index'这一列，最后也不要设置别的索引。
    ############################################
    start_date_minus_lag_2 = Dtk.get_n_days_off(start_date, -(para1 + para3 + 2))[0]
    # 获取close的后复权数据，是DataFrame，每一列的列名是股票代码，每一行的标签则是日期（例如20180829，是8位数的int）
    data_close: pd.DataFrame = Dtk.get_panel_daily_pv_df(stock_list, start_date_minus_lag_2, end_date,
                                                         pv_type='close', adj_type='FORWARD')
    data_amt: pd.DataFrame = Dtk.get_panel_daily_pv_df(stock_list, start_date_minus_lag_2, end_date,
                                                       pv_type='amt', adj_type='NONE')
    data_volume: pd.DataFrame = Dtk.get_panel_daily_pv_df(stock_list, start_date_minus_lag_2, end_date,
                                                          pv_type='volume', adj_type='NONE')
    date_adj_factor: pd.DataFrame = Dtk.get_panel_daily_info(stock_list, start_date_minus_lag_2, end_date,
                                                             info_type="adjfactor")
    t2 = dt.datetime.now()
    print("Raw data was downloaded successfully and it cost", t2 - t1)
    # 计算因子值
    data_vwap = data_amt / data_volume * date_adj_factor
    data_vwap = data_vwap.fillna(method='ffill')
    date_mean_close_dist = data_close.rolling(para1).mean() - data_close
    data_close_delay = data_close.shift(para2)
    data_vwap_close_corr = data_vwap.rolling(para3).corr(data_close_delay)
    factor_data = date_mean_close_dist + data_vwap_close_corr
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
    para1 = 7
    para2 = 5
    para3 = 230
    factor_name = "F_D_GTJA26"
    file_name = factor_name
    factor_generator(stock_code_list, istart_date_int, iend_date_int, save_dir, file_name, para1, para2, para3)


if __name__ == '__main__':
    main()