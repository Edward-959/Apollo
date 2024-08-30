#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019/2/25 20:08
# @Author  : 011673
import DataAPI.DataToolkit as Dtk
from Factor.DailyFactorBase import DailyFactorBase
import pandas as pd
import math
import numpy as np


class NonFactorDailyOppsiteIndexMomentum(DailyFactorBase):
    def __init__(self, alpha_factor_root_path, stock_list, start_date_int, end_date_int, params):
        super().__init__(alpha_factor_root_path)
        self.stock_list = stock_list
        self.start_date = start_date_int
        self.end_date = end_date_int

    def factor_calc(self):
        start = Dtk.get_n_days_off(self.start_date, -22)[0]
        close: pd.DataFrame = Dtk.get_panel_daily_pv_df(self.stock_list, start, self.end_date,
                                                        pv_type='close', adj_type='FORWARD')
        close_500 = Dtk.get_panel_daily_pv_df(['000905.SH'], start, self.end_date, pv_type='close')
        ret: pd.DataFrame = (close / close.shift(5) - 1) * 100
        ret_500 = (close_500 / close_500.shift(5) - 1) * 100
        ret_500: pd.Series = ret_500['000905.SH']
        factor_data = ret.apply(lambda x: x * ret_500)
        factor_data[factor_data > 0] = np.nan
        factor_data: pd.DataFrame = factor_data.apply(lambda x: x / ret_500)
        ans_df = factor_data.loc[self.start_date: self.end_date]
        ans_df = Dtk.convert_df_index_type(ans_df, 'date_int', 'timestamp')
        return ans_df
