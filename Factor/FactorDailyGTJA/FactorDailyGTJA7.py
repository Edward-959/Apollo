# -*- coding: utf-8 -*-
"""
Created on 2018/11/14
@author: 006566


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


def factor_generator(stock_list, start_date, end_date, factor_file_dir_path, factor_file_name, param_k):
    print("Start downloading raw data for calculating the factor")
    t1 = dt.datetime.now()
    ############################################
    # 以下是因子计算逻辑的部分，需要用户自定义 #
    # 计算因子时，最后应得到factor_data这个对象，类型应当是DataFrame，涵盖的时间段是start_date至end_date（前闭后闭）；
    # factor_data的每一列是相应的股票，每一行是每一天的因子值；
    # 最后factor_data的索引，建议与获得的原始行情的索引（index）一致，
    # 如通过reset_index撤销了原始行情的索引，那么不要删除'index'这一列，最后也不要设置别的索引。
    ############################################
    start_date_minus_k_2 = Dtk.get_n_days_off(start_date, -(param_k + 2))[0]
    # 获取close的后复权数据，是DataFrame，每一列的列名是股票代码，每一行的标签则是日期（例如20180829，是8位数的int）
    vol_df = Dtk.get_panel_daily_pv_df(stock_list, start_date_minus_k_2, end_date, pv_type='volume')
    amt_df = Dtk.get_panel_daily_pv_df(stock_list, start_date_minus_k_2, end_date, pv_type='amt')
    adj_df = Dtk.get_panel_daily_info(stock_list, start_date_minus_k_2, end_date, info_type='adjfactor')
    close_df = Dtk.get_panel_daily_pv_df(stock_list, start_date_minus_k_2, end_date, pv_type='close',
                                         adj_type='FORWARD')
    alpha_universe: pd.DataFrame = Dtk.get_panel_daily_info(stock_list, start_date_minus_k_2, end_date,
                                                            info_type="alpha_universe")
    t2 = dt.datetime.now()
    print("Raw data was downloaded successfully and it cost", t2 - t1)
    # 计算因子值
    vwap_df = amt_df.div(vol_df).mul(adj_df)
    temp_df1 = vwap_df - close_df
    temp_df1[temp_df1 < param_k] = param_k
    ans_part1 = temp_df1[alpha_universe == 1].rank(axis=1)

    temp_df2 = vwap_df - close_df
    temp_df2[temp_df2 > param_k] = param_k
    ans_part2 = temp_df2[alpha_universe == 1].rank(axis=1)

    temp_df3 = vol_df.diff(param_k)
    temp_df3 = temp_df3[alpha_universe == 1].rank(axis=1)

    ans_df = (ans_part1 + ans_part2).mul(temp_df3)
    factor_data = ans_df.loc[start_date: end_date].copy()
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
    # 以下4行及产生因子的函数名需要自行改写 #
    #################################################
    istart_date_int = 20171201
    iend_date_int = 20180630
    for param in [3]:
        factor_name = "F_D_GTJA7"
        file_name = factor_name
        factor_generator(stock_code_list, istart_date_int, iend_date_int, save_dir, file_name, param)


if __name__ == '__main__':
    main()
