#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019/2/27 9:20
# @Author  : 011673
from Factor.DailyMinFactorBase import DailyMinFactorBase
import DataAPI.DataToolkit as Dtk
import pandas as pd


class NonFactorDailyMinExceedIndexRet_ema(DailyMinFactorBase):
    def __init__(self, alpha_factor_root_path, stock_list, start_date_int, end_date_int, params):
        super().__init__(alpha_factor_root_path, stock_list, start_date_int, end_date_int)
        self.__data_volume = Dtk.get_panel_daily_pv_df(stock_list, start_date_int, end_date_int,
                                                       pv_type='volume')
        self.__n = params['n']
        self.__start_date_minus_n_2 = Dtk.get_n_days_off(start_date_int, -(self.__n + 2))[0]
        index_close = Dtk.get_single_stock_minute_data('000905.SH', self.__start_date_minus_n_2, end_date_int,
                                                       fill_nan=True,
                                                       append_pre_close=False, adj_type='NONE', drop_nan=False,
                                                       full_length_padding=True)
        index_close = index_close['close'].unstack()
        index_close = index_close.iloc[:, 1:]
        index_ret = 100 * index_close / index_close.shift(5, axis=1) - 100
        minite = list(index_ret.columns)
        self.__minite_index = []
        for i in minite:
            if i % 5 == 0:
                self.__minite_index.append(i)
        self.__index_ret = index_ret.loc[:, self.__minite_index]

    def single_stock_factor_generator(self, code):
        ############################################
        # 以下是数据计算逻辑的部分，需要用户自定义 #
        # 计算数据时，最后应得到factor_data这个DataFrame，涵盖的时间段是self.start_date至self.end_date（前闭后闭）；
        # factor_data的因子值一列，应当以股票代码为列名；
        # 最后factor_data的索引，应当是8位整数的交易日；
        # 获取分钟数据应当用get_single_stock_minute_data2这个函数
        ############################################
        stock_minute_data = Dtk.get_single_stock_minute_data2(code, self.start_date, self.end_date,
                                                              fill_nan=True, append_pre_close=False, adj_type='NONE',
                                                              drop_nan=False, full_length_padding=True)
        data_check_df = stock_minute_data.dropna()  # 用于检查行情的完整性
        if stock_minute_data.columns.__len__() > 0 and data_check_df.__len__() > 0:
            close_price = stock_minute_data['close'].unstack()
            close_price = close_price.iloc[:, 1:]
            close_ret = 100 * close_price / close_price.shift(5, axis=1) - 100
            close_ret = close_ret.loc[:, self.__minite_index]
            factor_data = close_ret * self.__index_ret
            factor_data[factor_data > 0] = 0
            factor_data = factor_data / self.__index_ret
            factor_data = factor_data.sum(axis=1)
            factor_data = factor_data.to_frame(code)
        ########################################
        # 因子计算逻辑到此为止，以下勿随意变更 #
        ########################################
        else:  # 如取到的行情的DataFrame为空，则自造一个全为nan的DataFrame
            factor_data = self.generate_empty_df(code)
        return factor_data
