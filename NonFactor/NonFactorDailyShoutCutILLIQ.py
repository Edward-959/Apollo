#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019/2/25 9:54
# @Author  : 011673
import DataAPI.DataToolkit as Dtk
from Factor.DailyFactorBase import DailyFactorBase
import pandas as pd
import numpy as np


class NonFactorDailyShoutCutILLIQ(DailyFactorBase):
    def __init__(self, alpha_factor_root_path, stock_list, start_date_int, end_date_int, params):
        super().__init__(alpha_factor_root_path)
        self.stock_list = stock_list
        self.start_date = start_date_int
        self.end_date = end_date_int
        self.n = params["n"]

    def factor_calc(self):
        start = Dtk.get_n_days_off(self.start_date, -22)[0]
        close: pd.DataFrame = Dtk.get_panel_daily_pv_df(self.stock_list, start, self.end_date,
                                                        pv_type='close', adj_type='FORWARD')
        amt: pd.DataFrame = Dtk.get_panel_daily_pv_df(self.stock_list, start, self.end_date,
                                                      pv_type='amt', adj_type='FORWARD')
        high: pd.DataFrame = Dtk.get_panel_daily_pv_df(self.stock_list, start, self.end_date,
                                                       pv_type='high', adj_type='FORWARD')
        low: pd.DataFrame = Dtk.get_panel_daily_pv_df(self.stock_list, start, self.end_date,
                                                      pv_type='low', adj_type='FORWARD')
        open_: pd.DataFrame = Dtk.get_panel_daily_pv_df(self.stock_list, start, self.end_date,
                                                        pv_type='open', adj_type='FORWARD')
        short_cut = 2 * (high - low) - abs(open_ - close)
        factor_data: pd.DataFrame = 100000000 * short_cut / amt
        factor_data.replace(np.inf, np.nan, inplace=True)
        factor_original_df = factor_data.loc[self.start_date: self.end_date].copy()
        ans_df = factor_original_df
        ans_df = ans_df.loc[self.start_date: self.end_date]
        ans_df = Dtk.convert_df_index_type(ans_df, 'date_int', 'timestamp')
        return ans_df
