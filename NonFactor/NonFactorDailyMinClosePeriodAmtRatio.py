#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019/3/27 9:35
# @Author  : 011673
from Factor.DailyMinFactorBase import DailyMinFactorBase
import DataAPI.DataToolkit as Dtk
import numpy as np


class NonFactorDailyMinClosePeriodAmtRatio(DailyMinFactorBase):
    def __init__(self, alpha_factor_root_path, stock_list, start_date_int, end_date_int, params):
        super().__init__(alpha_factor_root_path, stock_list, start_date_int, end_date_int)

    def single_stock_factor_generator(self, code):
        ############################################
        # 以下是数据计算逻辑的部分，需要用户自定义 #
        # 计算数据时，最后应得到factor_data这个DataFrame，涵盖的时间段是self.start_date至self.end_date（前闭后闭）；
        # factor_data的因子值一列，应当以股票代码为列名；
        # 最后factor_data的索引，应当是8位整数的交易日；
        # 获取分钟数据应当用get_single_stock_minute_data2这个函数
        ############################################
        stock_minute_data = Dtk.get_single_stock_minute_data(code, self.start_date, self.end_date,
                                                             fill_nan=True, append_pre_close=False, adj_type='NONE',
                                                             drop_nan=False, full_length_padding=True)
        data_check_df = stock_minute_data.dropna()  # 用于检查行情的完整性
        if stock_minute_data.columns.__len__() > 0 and data_check_df.__len__() > 0:
            stock_minute_amt = stock_minute_data['amt'].unstack()
            stock_minute_amt_temp = stock_minute_amt.iloc[:, 1:-1]
            abn = stock_minute_amt_temp.mean(axis=1) + stock_minute_amt_temp.std(axis=1)
            close__amt = stock_minute_amt.loc[:, stock_minute_amt.columns[stock_minute_amt.columns > 1430]]
            factor_data = (close__amt.T - abn).T
            factor_data = factor_data > 0
            factor_data = factor_data.sum(axis=1)
            ava_data = stock_minute_amt.sum(axis=1)
            ava_data = np.isnan(ava_data)
            factor_data[ava_data] = np.nan
            factor_data = factor_data.to_frame(code)
            factor_data = factor_data.loc[self.start_date: self.end_date]
        ########################################
        # 因子计算逻辑到此为止，以下勿随意变更 #
        ########################################
        else:  # 如取到的行情的DataFrame为空，则自造一个全为nan的DataFrame
            factor_data = self.generate_empty_df(code)
        return factor_data
