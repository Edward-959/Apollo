#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019/1/15 15:05
# @Author  : 011673
# @revised by 006566 on 2019/3/12 将因子名从错误的FactorDailyYoyeps改为FactorDailyQfaYoyeps
import DataAPI.DataToolkit as Dtk
from Factor.DailyFactorBase import DailyFactorBase
import numpy as np


class FactorDailyQfaYoyeps(DailyFactorBase):
    def __init__(self, alpha_factor_root_path, stock_list, start_date_int, end_date_int, params):
        super().__init__(alpha_factor_root_path)
        self.stock_list = stock_list
        self.start_date = start_date_int
        self.end_date = end_date_int

    def factor_calc(self):
        factor_data = Dtk.get_panel_daily_info(self.stock_list, self.start_date, self.end_date, "qfa_yoyeps")
        factor_data.replace(np.inf, np.nan, inplace=True)
        ans_df = factor_data.loc[self.start_date: self.end_date]
        ans_df = Dtk.convert_df_index_type(ans_df, 'date_int', 'timestamp')
        return ans_df
