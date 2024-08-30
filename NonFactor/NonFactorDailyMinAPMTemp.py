#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019/2/27 13:26
# @Author  : 011673
from Factor.DailyMinFactorBase import DailyMinFactorBase
import DataAPI.DataToolkit as Dtk
import pandas as pd
import copy
from scipy import stats
from scipy import optimize
import math
import numpy as np

def divide_to_half_ret(minute_data):
    minute_close = minute_data.loc[:, 'close'].unstack()
    minute_pre_close = minute_data.loc[:, 'pre_close'].unstack()
    ret_half_day = pd.DataFrame([], index=minute_close.index, columns=['am', 'pm'])
    ret_half_day.loc[:, 'am'] = minute_close.iloc[:, 120] / minute_pre_close.iloc[:, -1] - 1
    ret_half_day.loc[:, 'pm'] = minute_close.iloc[:, -1] / minute_close.iloc[:, 120] - 1
    return ret_half_day.stack()

def get_residual(df: pd.DataFrame):
    df_1 = copy.deepcopy(df)
    df_1.fillna(0, inplace=True)
    inform = stats.linregress(df_1.iloc[:, 0].values, df_1.iloc[:, 1].values)
    return df.iloc[:, 1] - (inform[0] * df.iloc[:, 0] + inform[1])


def func(x, A):
    return A * x


def pre_process(df: pd.Series):
    mean_value = df.mean()
    std_value = df.std()
    df[df > (mean_value + 3 * std_value)] = mean_value + 3 * std_value
    df[df < (mean_value - 3 * std_value)] = mean_value - 3 * std_value
    return (df - mean_value) / std_value


class NonFactorDailyMinAPMTemp(DailyMinFactorBase):
    def __init__(self, alpha_factor_root_path, stock_list, start_date_int, end_date_int, params):
        super().__init__(alpha_factor_root_path, stock_list, start_date_int, end_date_int)
        self.n = params['n']
        start_calculation = Dtk.get_n_days_off(start_date_int, -(self.n + 5))[0]
        self.__index_code = '000300.SH'
        index_minute_data = Dtk.get_single_stock_minute_data(self.__index_code, start_calculation, end_date_int,
                                                             fill_nan=True, append_pre_close=True, adj_type='NONE',
                                                             drop_nan=False, full_length_padding=True)

        self.index_ret_halfday = divide_to_half_ret(index_minute_data)


    def single_stock_factor_generator(self, code):
        ############################################
        # 以下是数据计算逻辑的部分，需要用户自定义 #
        # 计算数据时，最后应得到factor_data这个DataFrame，涵盖的时间段是self.start_date至self.end_date（前闭后闭）；
        # factor_data的因子值一列，应当以股票代码为列名；
        # 最后factor_data的索引，应当是8位整数的交易日；
        # 获取分钟数据应当用get_single_stock_minute_data2这个函数
        ############################################
        n = self.n
        start_calculation = Dtk.get_n_days_off(self.start_date, -(n + 5))[0]
        stock_minute_data = Dtk.get_single_stock_minute_data(code, start_calculation, self.end_date, fill_nan=True,
                                                             append_pre_close=True,
                                                             adj_type='NONE', drop_nan=False, full_length_padding=True)
        data_check_df = stock_minute_data.dropna()
        if stock_minute_data.columns.__len__() > 0 and data_check_df.__len__() > 0:
            ret_halfday = divide_to_half_ret(stock_minute_data)
            combine_df = pd.DataFrame([self.index_ret_halfday, ret_halfday]).T
            index_list = []
            stat = []
            number = 2 * n
            for index in combine_df.index[2 * n:]:
                number += 1
                if index[1] == 'am':
                    continue
                if number > len(combine_df.index):
                    break
                index_list.append(index[0])
                data = combine_df.iloc[number - 2 * n:number, :]
                residual: pd.Series = get_residual(data)
                residual = residual.unstack()
                residual = residual.iloc[:, 0] - residual.iloc[:, 1]
                try:
                    stat.append(10000 * residual.mean() / residual.std() / math.sqrt(n))
                except Exception as e:
                    # print(str(code)+' '+str(index[0])+' got 0 std')
                    stat.append(np.nan)

            stat = pd.Series(stat, index=index_list)
            stat = pre_process(stat)
            factor_data = pd.DataFrame(stat, index=stat.index, columns=[code])
        ########################################
        # 因子计算逻辑到此为止，以下勿随意变更 #
        ########################################
        else:  # 如取到的行情的DataFrame为空，则自造一个全为nan的DataFrame
            factor_data = self.generate_empty_df(code)
        return factor_data
