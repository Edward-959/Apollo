# -*- coding: utf-8 -*-
# @Time    : 2018/12/24 11:26
# @Author  : 011673
# @File    : FactorDailyMinSeperateCorr.py

from Factor.DailyMinFactorBase import DailyMinFactorBase
import DataAPI.DataToolkit as Dtk
import pandas as pd


class NonFactorDailyMinSeperateCorr(DailyMinFactorBase):
    def __init__(self, alpha_factor_root_path, stock_list, start_date_int, end_date_int, params):
        super().__init__(alpha_factor_root_path, stock_list, start_date_int, end_date_int)
        self.n=params['n']
        self.__start_date_minus_n_2 = Dtk.get_n_days_off(start_date_int, -(self.n + 2))[0]

    def single_stock_factor_generator(self, code):
        ############################################
        # 以下是数据计算逻辑的部分，需要用户自定义 #
        # 计算数据时，最后应得到factor_data这个DataFrame，涵盖的时间段是self.start_date至self.end_date（前闭后闭）；
        # factor_data的因子值一列，应当以股票代码为列名；
        # 最后factor_data的索引，应当是8位整数的交易日；
        # 获取分钟数据应当用get_single_stock_minute_data2这个函数
        ############################################
        stock_minute_data = Dtk.get_single_stock_minute_data(code, self.__start_date_minus_n_2, self.end_date, fill_nan=True,
                                                             append_pre_close=False, adj_type='NONE', drop_nan=False,
                                                             full_length_padding=True)
        if stock_minute_data.columns.__len__() > 0 and stock_minute_data.__len__() > 0:
            stock_minute_volume = stock_minute_data['volume'].unstack()
            stock_minute_close = stock_minute_data['close'].unstack()
            stock_minute_volume_1 = stock_minute_volume.loc[:, 1300:1400]
            stock_minute_close_1 = stock_minute_close.loc[:, 1300:1400]
            stock_corr: pd.Series = -stock_minute_close_1.corrwith(stock_minute_volume_1, axis=1)
            stock_minute_volume_2 = stock_minute_volume.loc[:, 1400:]
            stock_minute_close_2 = stock_minute_close.loc[:, 1400:]
            stock_corr_2: pd.Series = -stock_minute_close_2.corrwith(stock_minute_volume_2, axis=1)
            stock_minute_volume_3 = stock_minute_volume.loc[:, 930:1030]
            stock_minute_close_3 = stock_minute_close.loc[:, 930:1030]
            stock_corr_3: pd.Series = -stock_minute_close_3.corrwith(stock_minute_volume_3, axis=1)
            stock_corr = stock_corr + stock_corr_2 + stock_corr_3
            factor_data = stock_corr.to_frame(code)
        ########################################
        # 因子计算逻辑到此为止，以下勿随意变更 #
        ########################################
        else:  # 如取到的行情的DataFrame为空，则自造一个全为nan的DataFrame
            factor_data = self.generate_empty_df(code)
        return factor_data

