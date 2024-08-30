#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019/2/26 16:32
# @Author  : 011673
from Factor.DailyMinFactorBase import DailyMinFactorBase
import DataAPI.DataToolkit as Dtk
import pandas as pd
import numpy as np


class NonFactorDailyMinSeperateMomentum(DailyMinFactorBase):
    def __init__(self, alpha_factor_root_path, stock_list, start_date_int, end_date_int, params):
        super().__init__(alpha_factor_root_path, stock_list, start_date_int, end_date_int)
        self.__data_volume = Dtk.get_panel_daily_pv_df(stock_list, start_date_int, end_date_int,
                                                       pv_type='volume')
        self.ratio=params['ratio']

    def single_stock_factor_generator(self, code):
        ############################################
        # 以下是数据计算逻辑的部分，需要用户自定义 #
        # 计算数据时，最后应得到factor_data这个DataFrame，涵盖的时间段是self.start_date至self.end_date（前闭后闭）；
        # factor_data的因子值一列，应当以股票代码为列名；
        # 最后factor_data的索引，应当是8位整数的交易日；
        # 获取分钟数据应当用get_single_stock_minute_data2这个函数
        ############################################
        stock_minute_data = Dtk.get_single_stock_minute_data(code, self.start_date, self.end_date, fill_nan=True, append_pre_close=True,
                                                             adj_type='None', drop_nan=False, full_length_padding=True)
        data_check_df = stock_minute_data.dropna()  # 用于检查行情的完整性
        if stock_minute_data.columns.__len__() > 0 and data_check_df.__len__() > 0:
            pre_close = stock_minute_data['pre_close'].unstack().iloc[:, 0]
            close = stock_minute_data['close'].unstack()
            ret0 = close.iloc[:, 0] / pre_close - 1
            ret1 = close.loc[:, 1029] / close.loc[:, 930] - 1
            ret2 = close.loc[:, 1129] / close.loc[:, 1029] - 1
            ret3 = close.loc[:, 1359] / close.loc[:, 1129] - 1
            ret4 = close.loc[:, 1500] / close.loc[:, 1359] - 1
            factor_data: pd.DataFrame = -self.ratio[0]*ret0 - self.ratio[1]*ret1 + self.ratio[2]*ret2 + self.ratio[3]*ret3 + self.ratio[4]*ret4
            factor_data: pd.DataFrame = pd.DataFrame(factor_data, index=list(factor_data.index), columns=[code])
        ########################################
        # 因子计算逻辑到此为止，以下勿随意变更 #
        ########################################
        else:  # 如取到的行情的DataFrame为空，则自造一个全为nan的DataFrame
            factor_data = self.generate_empty_df(code)
        return factor_data
