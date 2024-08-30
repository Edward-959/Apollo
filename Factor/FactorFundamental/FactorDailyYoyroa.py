#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019/2/20 10:24
# @Author  : 011673
import DataAPI.DataToolkit as Dtk
from Factor.DailyFactorBase import DailyFactorBase
import numpy as np
import copy
import pandas as pd


class FactorDailyYoyroa(DailyFactorBase):
    def __init__(self, alpha_factor_root_path, stock_list, start_date_int, end_date_int, params):
        super().__init__(alpha_factor_root_path)
        self.stock_list = stock_list
        self.start_date = start_date_int
        self.end_date = end_date_int

    def factor_calc(self):
        valid_start = Dtk.get_n_days_off(self.start_date, -250)[0]
        factor_data = Dtk.get_panel_daily_info(self.stock_list, valid_start, self.end_date, "roa")
        factor_data_last_year = factor_data.shift(250)
        factor_data = factor_data.loc[self.start_date:self.end_date]
        factor_data_last_year = factor_data_last_year.loc[self.start_date:self.end_date]
        factor_data.replace(np.inf, np.nan, inplace=True)
        # factor_data: pd.DataFrame = factor_data - factor_data.shift(1)
        # factor_data.replace(0, np.nan, inplace=True)
        # factor_data.fillna(method='ffill', inplace=True)
        factor_data = (factor_data - factor_data_last_year)/abs(factor_data_last_year)
        factor_data = factor_data.loc[self.start_date: self.end_date].copy()
        ans_df = factor_data.loc[self.start_date: self.end_date]
        ans_df = Dtk.convert_df_index_type(ans_df, 'date_int', 'timestamp')
        return ans_df

