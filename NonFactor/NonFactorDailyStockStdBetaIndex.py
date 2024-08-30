# -*- coding: utf-8 -*-
"""

"""
from Factor.DailyFactorBase import DailyFactorBase
import DataAPI.DataToolkit as Dtk
import numpy as np
import os


class NonFactorDailyStockStdBetaIndex(DailyFactorBase):
    # 这个因子没有参数，但也须在初始化时预留一个"params"
    def __init__(self, alpha_factor_root_path, stock_list, start_date_int, end_date_int, params):
        super().__init__(alpha_factor_root_path)
        self.stock_list = stock_list
        self.start_date = start_date_int
        self.end_date = end_date_int
        self.index_code = params['index_code']
        self.n = params['n']

    def factor_calc(self):
        valid_start_date = Dtk.get_n_days_off(self.start_date, -self.n-30)[0]
        stock_close = Dtk.get_panel_daily_pv_df(self.stock_list, valid_start_date, self.end_date,
                                                pv_type='close', adj_type='FORWARD')
        index_close = Dtk.get_panel_daily_pv_df([self.index_code], valid_start_date, self.end_date, pv_type='close')

        stock_close_log = np.log(stock_close)
        index_close_log = np.log(index_close)
        stock_close_std = stock_close_log.rolling(self.n).std()
        index_close_std = index_close_log.rolling(self.n).std()
        trading_days = Dtk.get_trading_day(self.start_date, self.end_date)
        ans_df = stock_close.copy()
        ans_df[:] = np.nan
        for date in trading_days:
            i = stock_close_log.index.tolist().index(date)
            stock_close_std_i = stock_close_std.iloc[i - self.n + 1: i + 1]
            index_close_std_i = index_close_std.iloc[i - self.n + 1: i + 1]
            index_close_std_i = index_close_std_i[self.index_code]
            ff = np.vstack([np.array(index_close_std_i), np.ones(len(index_close_std_i))])
            reg = np.linalg.inv(ff.dot(ff.T)).dot(ff).dot(np.array(stock_close_std_i))
            ans_df.loc[date] = reg[0,:]
        # ----以下勿改动----
        ans_df = ans_df.loc[self.start_date: self.end_date]
        ans_df = Dtk.convert_df_index_type(ans_df, 'date_int', 'timestamp')
        return ans_df
