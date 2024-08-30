#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019/2/26 11:28
# @Author  : 011673
from Factor.DailyMinFactorBase import DailyMinFactorBase
import DataAPI.DataToolkit as Dtk
import pandas as pd


class NonFactorDailyMinTrendStrength(DailyMinFactorBase):
    def __init__(self, alpha_factor_root_path, stock_list, start_date_int, end_date_int, params):
        super().__init__(alpha_factor_root_path, stock_list, start_date_int, end_date_int)
        self.__data_volume = Dtk.get_panel_daily_pv_df(stock_list, start_date_int, end_date_int,
                                                       pv_type='volume')

    def single_stock_factor_generator(self, code):
        ############################################
        # 以下是数据计算逻辑的部分，需要用户自定义 #
        # 计算数据时，最后应得到factor_data这个DataFrame，涵盖的时间段是self.start_date至self.end_date（前闭后闭）；
        # factor_data的因子值一列，应当以股票代码为列名；
        # 最后factor_data的索引，应当是8位整数的交易日；
        # 获取分钟数据应当用get_single_stock_minute_data2这个函数
        ############################################
        stock_minute_data = Dtk.get_single_stock_minute_data(code, self.start_date, self.end_date, fill_nan=True, append_pre_close=False,
                                                             adj_type='None', drop_nan=False, full_length_padding=True)
        data_check_df = stock_minute_data.dropna()  # 用于检查行情的完整性
        if stock_minute_data.columns.__len__() > 0 and data_check_df.__len__() > 0:  # 如可正常取到行情DataFrame
            ############################################
            # 以下是数据计算逻辑的部分，需要用户自定义 #
            # 计算数据时，最后应得到factor_data这个对象，类型应当是DataFrame，涵盖的时间段是start至end（前闭后闭）；
            # factor_data的因子值一列，应当以股票代码为列名；
            # 最后factor_data的索引，应当从原始分钟数据中获得的dt，即start至end，内容的格式是20180904这种8位数字
            ############################################
            close = stock_minute_data['close'].unstack()
            ret: pd.DataFrame = close - close.shift(1, axis=1)
            ret = abs(ret)
            ret_sum = ret.sum(axis=1)
            pht = close.iloc[:, -1] - close.iloc[:, 0]
            factor_data = pht / ret_sum
            factor_data = pd.DataFrame(factor_data, index=list(close.index), columns=[code])
            factor_data: pd.DataFrame = factor_data.rolling(window=5, min_periods=1).mean()
        ########################################
        # 因子计算逻辑到此为止，以下勿随意变更 #
        ########################################
        else:  # 如取到的行情的DataFrame为空，则自造一个全为nan的DataFrame
            factor_data = self.generate_empty_df(code)
        return factor_data
