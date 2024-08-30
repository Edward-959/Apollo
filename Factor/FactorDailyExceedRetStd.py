#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019/2/11 9:35
# @Author  : 011673
import DataAPI.DataToolkit as Dtk
from Factor.DailyFactorBase import DailyFactorBase
import pandas as pd
import numpy as np


class FactorDailyExceedRetStd(DailyFactorBase):
    def __init__(self, alpha_factor_root_path, stock_list, start_date_int, end_date_int, params):
        super().__init__(alpha_factor_root_path)
        self.stock_list = stock_list
        self.start_date = start_date_int
        self.end_date = end_date_int
        self.n = params["n"]

    def factor_calc(self):
        start_date_minus_lag_2 = Dtk.get_n_days_off(self.start_date, -(self.n + 2))[0]
        stock_close: pd.DataFrame = Dtk.get_panel_daily_pv_df(self.stock_list, start_date_minus_lag_2, self.end_date,
                                                              pv_type='close', adj_type='FORWARD')
        index_close: pd.DataFrame = Dtk.get_panel_daily_pv_df(['000905.SH'], start_date_minus_lag_2, self.end_date,
                                                              pv_type='close')
        stock_close: pd.DataFrame = (stock_close / stock_close.shift(1)) - 1
        index_close: pd.DataFrame = (index_close / index_close.shift(1)) - 1
        factor_data = (stock_close.T - index_close.iloc[:, 0]).T
        factor_data = factor_data.rolling(window=self.n).std()
        factor_original_df = factor_data.loc[self.start_date: self.end_date].copy()
        ans_df = factor_original_df
        ans_df = Dtk.convert_df_index_type(ans_df, 'date_int', 'timestamp')
        return ans_df
