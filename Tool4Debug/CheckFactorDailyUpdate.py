#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019/3/11 10:27
# @Author  : 011673
# !/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019/3/11 10:27
# @Author  : 011673
import DataAPI.DataToolkit as Dtk
import pandas as pd
import numpy as np

warnning_rate = 0.4


def dateint_timestamp(dateint):
    temp = Dtk.convert_date_or_time_int_to_datetime(dateint)
    return temp.timestamp()


def check_index_loss(data_df: pd.DataFrame):
    """
    检查有没有漏或者重复天的
    :param data_df: 读取因子文件
    :return: 提示和是否没有问题
    """
    sat = True
    data_df = Dtk.convert_df_index_type(data_df, 'timestamp', 'date_int')
    result = ''
    index_date_list = list(data_df.index)
    if len(index_date_list) != len(set(index_date_list)):
        result += 'same index in df!  '
        sat = False
    date_list = Dtk.get_trading_day(data_df.index[0], data_df.index[-1])
    for date in date_list:
        if date not in index_date_list:
            result += '{} loss  '.format(date)
            sat = False
    return result, sat


def check_data(dateint: int, data_df: pd.DataFrame):
    """
    check更新的最后一天因子是不是有问题
    :param dateint: 最有一天更新的日期
    :param data_df: 因子读取文件
    :return: 检查结果
    """
    result, sat = check_index_loss(data_df)
    index_number = dateint_timestamp(dateint)
    index_list = list(data_df.index)
    if index_number != index_list[-1]:
        result = result + 'last index is {}, not match ! '.format(index_list[-1])
        sat = False
        last_data = data_df.iloc[-1]
    else:
        last_data = data_df.loc[index_number]
    if np.isnan(last_data).all():
        result = result + 'last day data is all nan! '
        sat = False
    if np.isnan(last_data).sum() / last_data.shape[0] > warnning_rate:
        result = result + 'lasy day data has {} % of nan or above'.format(100 * warnning_rate)
    if sat:
        result = result + 'good !'
    return result


def check_all_factors(date_int):  # 输入更新的最后日期
    #每日因子更新完成之后运行一下，检查因子更新是否有问题
    import platform, os
    if platform.system() == "Windows":
        alpha_factor_root_path = "D:\\NewFactorData"
    elif os.system("nvidia-smi") == 0:
        alpha_factor_root_path = "/data/NewFactorData"
    else:
        user_id = os.environ['USER_ID']
        alpha_factor_root_path = "/app/data/" + user_id + "/Apollo/AlphaFactors"  #根据存储具体路径需要修改一下
    alpha_factor_root_path += '/AlphaFactors'
    for root, dirs, files in os.walk(alpha_factor_root_path):
        for file in files:
            file_path = os.path.join(root, file)
            print('check {} ing***'.format(file))
            store = pd.HDFStore(file_path)
            data = store.select('/factor')
            store.close()
            print(check_data(date_int, data))
    print('DONE')


if __name__=='__main__':
    check_all_factors(20180312)
