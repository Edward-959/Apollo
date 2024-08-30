#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019/3/4 14:18
# @Author  : 011673
import DataAPI.DataToolkit as Dtk
import os
import pandas as pd
import platform
import numpy as np


def get_path():
    """
    读取存储路径
    :return:
    """
    if platform.system() == "Windows":
        alpha_factor_root_path = "D:\\NewFactorData\\AlphaNonFactors\\"
    elif os.system("nvidia-smi") == 0:
        alpha_factor_root_path = "/data/NewFactorData/AlphaNonFactors/"
    else:
        user_id = os.environ['USER_ID']
        alpha_factor_root_path = "/app/data/" + user_id + "/NewFactorData/AlphaNonFactors"
    return alpha_factor_root_path


def get_industry_min_data(start, end):
    """
    把等权行业走势计算出来，计算方法是去量纲后，分别计算sum和计数，之后除得到平均，去除所有nan和0行业。
    :param start: 开始日期
    :param end: 结束日期
    :return:
    """
    codes = Dtk.get_complete_stock_list()
    industry = Dtk.get_panel_daily_info(codes, start, end, info_type='industry3')
    result = {}
    number = {}
    for i in range(1, 32):
        result[i] = {}
        number[i] = {}
    for n, code in enumerate(codes):
        print('process {}/{}'.format(n, len(codes)))
        stock_minute_data = Dtk.get_single_stock_minute_data(code, start, end, fill_nan=True, append_pre_close=True,
                                                             adj_type='None', drop_nan=False, full_length_padding=True)
        if len(stock_minute_data.dropna().index) == 0:
            continue
        stock_minute_data = stock_minute_data.loc[:, 'close'] / stock_minute_data.loc[:, 'pre_close']
        stock_minute_data: pd.DataFrame = stock_minute_data.unstack()
        stock_minute_data = stock_minute_data.dropna()
        for date in stock_minute_data.index:
            industry_number = industry.loc[date, code]
            if np.isnan(industry_number):
                continue
            industry_number = int(industry_number)
            data = stock_minute_data.loc[date]
            if np.isnan(data.sum()):
                continue
            if industry_number not in range(1, 32):
                continue
            if date not in result[industry_number].keys():
                result[industry_number][date] = data
                number[industry_number][date] = 1
            else:
                result[industry_number][date] = result[industry_number][date] + data
                number[industry_number][date] = number[industry_number][date] + 1
        del stock_minute_data
    if not os.path.exists(get_path()+'Industry.h5'):
        A=pd.HDFStore(get_path()+'Industry.h5')
        A.close()
    for i in result.keys():
        result_i = pd.DataFrame(result[i])
        number_i = pd.Series(number[i])
        result_i: pd.DataFrame = (result_i / number_i).T
        save_data(get_path()+'Industry.h5', '/industry_' + str(i), result_i)


def get_industry_min_amt_data(start, end):
    """
    /计算行业的成交额，方法和上面一个函数类似
    :param start: 开始日期
    :param end: 结束日期
    :return:
    """
    codes = Dtk.get_complete_stock_list()
    industry = Dtk.get_panel_daily_info(codes, start, end, info_type='industry3')
    result = {}
    for i in range(1, 32):
        result[i] = {}
    for n, code in enumerate(codes):
        print('process {}/{}'.format(n, len(codes)))
        stock_minute_data = Dtk.get_single_stock_minute_data(code, start, end, fill_nan=True, append_pre_close=False,
                                                             adj_type='None', drop_nan=False, full_length_padding=True)
        if len(stock_minute_data.dropna().index) == 0:
            continue
        stock_minute_data = stock_minute_data.loc[:, 'amt']
        stock_minute_data: pd.DataFrame = stock_minute_data.unstack()
        stock_minute_data = stock_minute_data.dropna()
        for date in stock_minute_data.index:
            industry_number = industry.loc[date, code]
            if np.isnan(industry_number):
                continue
            industry_number = int(industry_number)
            data = stock_minute_data.loc[date]
            if np.isnan(data.sum()):
                continue
            if industry_number not in range(1, 32):
                continue
            if date not in result[industry_number].keys():
                result[industry_number][date] = data
            else:
                result[industry_number][date] = result[industry_number][date] + data
        del stock_minute_data
    if not os.path.exists(get_path()+'IndustryAmt.h5'):
        A=pd.HDFStore(get_path()+'IndustryAmt.h5')
        A.close()
    for i in result.keys():
        result_i = pd.DataFrame(result[i]).T
        save_data(get_path()+'IndustryAmt.h5', '/industry_' + str(i), result_i)



def save_data(address: str, industry_number: str, result: pd.DataFrame):
    """
    存储和更新储存用函数
    :param address:
    :param industry_number:
    :param result:
    :return:
    """
    store = pd.HDFStore(address)
    if industry_number not in store.keys():
        store.put(industry_number, result)
        store.flush()
        store.close()
    else:
        original_data_df = store.select(industry_number)
        if original_data_df.index[-1] < result.index[0]:
            ans_df2 = pd.concat([original_data_df, result])
        else:
            ans_df2 = pd.concat([original_data_df.loc[:result.index[0]-1], result])
        store.put(industry_number, ans_df2)
        store.flush()
        store.close()
    return True



if __name__ == '__main__':
    #一般不需要单独运行，在因子更新之前会自动调用，这里写出来以防存储的数据意外丢失
    get_industry_min_data(20131001, 20181101)
    get_industry_min_amt_data(20181001,20181030)
