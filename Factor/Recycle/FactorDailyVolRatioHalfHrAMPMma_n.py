# -*- coding: utf-8 -*-
"""
@author: 006566
FactorDailyMinVolRatioHalfHrAMPM的过去n日的ma
"""

import DataAPI.DataToolkit as Dtk
import pandas as pd
import os
import datetime as dt
import platform
from DataAPI.AlphaFactorLoader import load_factor


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
    raw = load_factor("F_D_MinVolRatioHalfHrAMPM", stock_list, dt.datetime.strptime(str(start_date), "%Y%m%d"),
                      dt.datetime.strptime(str(end_date), "%Y%m%d"))
    raw = Dtk.convert_df_index_type(raw, 'timestamp', 'date_int')
    raw.index.name = 'index'
    t2 = dt.datetime.now()
    print("Raw data was downloaded successfully and it cost", t2 - t1)
    # 计算因子值
    factor_data = raw.rolling(window=n).mean()
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
        save_dir = "D:\\AlphaFactors\\"    # 保存于云桌面的地址
    else:
        user_id = os.environ['USER_ID']
        save_dir = "/app/data/" + user_id + "/AlphaFactors"  # 保存于XQuant的地址
    #################################################
    # 以下3行及产生因子的函数名需要自行改写 #
    #################################################
    istart_date_int = 20141201
    iend_date_int = 20180630
    for n in [5, 10, 20]:
        factor_name = "F_D_VolRatioHalfHrAMPMma_" + str(n)
        file_name = factor_name
        factor_generator(stock_code_list, istart_date_int, iend_date_int, save_dir, file_name, n)


if __name__ == '__main__':
    main()
