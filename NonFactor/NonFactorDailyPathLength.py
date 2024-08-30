# -*- coding: utf-8 -*-
"""
Created on 2019/3/25
@author: Xiu Zixing
来自民生证券研报《因子研究专题三——动量（反转）因子解析》
路径-长度因子
"""
from Factor.DailyFactorBase import DailyFactorBase
import DataAPI.DataToolkit as Dtk
import os
import numpy as np


class NonFactorDailyPathLength(DailyFactorBase):
    def __init__(self, alpha_factor_root_path, stock_list, start_date_int, end_date_int, params):
        super().__init__(alpha_factor_root_path)
        self.stock_list = stock_list
        self.start_date = start_date_int
        self.end_date = end_date_int
        self.rolling_window = params['n']


    def factor_calc(self):
        valid_start_date = Dtk.get_n_days_off(self.start_date, -(self.rolling_window + 2))[0]
        close_df = Dtk.get_panel_daily_pv_df(self.stock_list, valid_start_date, self.end_date, pv_type='close',
                                             adj_type="FORWARD")
        close_df_norm = (close_df - close_df.min())/(close_df.max() - close_df.min())
        close_df_last_day = close_df_norm.shift(1)
        close_diff_df = abs(close_df_norm - close_df_last_day)
        factor_original_df = close_diff_df.rolling(self.rolling_window).sum()/(self.rolling_window - 1)
        ans_df = factor_original_df.loc[self.start_date: self.end_date]
        ans_df = Dtk.convert_df_index_type(ans_df, 'date_int', 'timestamp')
        return ans_df
