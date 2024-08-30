# -*- coding: utf-8 -*-
"""
@author: 006688
created on 2018/11/28
revised on 2019/02/22
将过去n日的换手率求和取对数，并在横截面上关于对数流通市值回归，取残差作为因子值
"""
from Factor.DailyFactorBase import DailyFactorBase
import DataAPI.DataToolkit as Dtk
import numpy as np
import pandas as pd


class FactorDailyTurnPure(DailyFactorBase):
    # 这个因子没有参数，但也须在初始化时预留一个"params"
    def __init__(self, alpha_factor_root_path, stock_list, start_date_int, end_date_int, params):
        super().__init__(alpha_factor_root_path)
        self.stock_list = stock_list
        self.start_date = start_date_int
        self.end_date = end_date_int
        self.n = params['n']

    def factor_calc(self):
        valid_start_date = Dtk.get_n_days_off(self.start_date, -self.n-2)[0]
        data_close = Dtk.get_panel_daily_pv_df(self.stock_list, valid_start_date, self.end_date,
                                               pv_type='close', adj_type='NONE')
        data_turn = Dtk.get_panel_daily_info(self.stock_list, valid_start_date, self.end_date, info_type='turn')
        data_free_float_shares = Dtk.get_panel_daily_info(self.stock_list, valid_start_date, self.end_date,
                                                          info_type='free_float_shares')
        stock_mv = np.log((data_free_float_shares * data_close * 10000).replace(0, np.nan))
        stock_liq_raw = np.log(data_turn.rolling(self.n).sum().replace(0, np.nan))
        trading_days = Dtk.get_trading_day(self.start_date, self.end_date)
        ans_df = data_close.copy()
        ans_df[:] = np.nan
        for date in trading_days:
            reg_data = pd.DataFrame([])
            reg_data['float_mv'] = stock_mv.loc[date, :]
            reg_data['liq_raw'] = stock_liq_raw.loc[date, :]
            reg_data.dropna(inplace=True)
            X = np.array(reg_data['float_mv'], ndmin=2)
            reg = np.linalg.inv(X.dot(X.T)).dot(X).dot(np.array(reg_data['liq_raw']))
            res = reg_data['liq_raw'] - X.T.dot(reg)
            ans_df.loc[date] = res
        # ----以下勿改动----
        ans_df = ans_df.loc[self.start_date: self.end_date]
        ans_df = Dtk.convert_df_index_type(ans_df, 'date_int', 'timestamp')
        return ans_df
