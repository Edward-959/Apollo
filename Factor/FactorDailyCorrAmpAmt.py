# -*- coding: utf-8 -*-
"""
@author: 006688
Created on 2019/01/22
revised on 2019/02/21
振幅与成交额的相关性
"""
from Factor.DailyFactorBase import DailyFactorBase
import DataAPI.DataToolkit as Dtk


class FactorDailyCorrAmpAmt(DailyFactorBase):
    # 这个因子没有参数，但也须在初始化时预留一个"params"
    def __init__(self, alpha_factor_root_path, stock_list, start_date_int, end_date_int, params):
        super().__init__(alpha_factor_root_path)
        self.stock_list = stock_list
        self.start_date = start_date_int
        self.end_date = end_date_int
        self.n = params['n']

    def factor_calc(self):
        valid_start_date = Dtk.get_n_days_off(self.start_date, -self.n-2)[0]
        data_amt = Dtk.get_panel_daily_pv_df(self.stock_list, valid_start_date, self.end_date, pv_type='amt')
        data_high = Dtk.get_panel_daily_pv_df(self.stock_list, valid_start_date, self.end_date, pv_type='high')
        data_low = Dtk.get_panel_daily_pv_df(self.stock_list, valid_start_date, self.end_date, pv_type='low')
        stock_amp = (data_high - data_low) / (data_high + data_low)
        ans_df = stock_amp.rolling(self.n, min_periods=round(self.n / 2)).corr(data_amt)
        # ----以下勿改动----
        ans_df = ans_df.loc[self.start_date: self.end_date]
        ans_df = Dtk.convert_df_index_type(ans_df, 'date_int', 'timestamp')
        return ans_df
