#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019/2/22 14:36
# @Author  : 011673
import DataAPI.DataToolkit as Dtk
from Factor.DailyFactorBase import DailyFactorBase
import pandas as pd
import math


class NonFactorDaily_Exp_Wgt_Return_nm(DailyFactorBase):
    def __init__(self, alpha_factor_root_path, stock_list, start_date_int, end_date_int, params):
        super().__init__(alpha_factor_root_path)
        self.stock_list = stock_list
        self.start_date = start_date_int
        self.end_date = end_date_int
        self.n = params["n"]

    def factor_calc(self):
        start = Dtk.get_n_days_off(self.start_date, -14)[0]
        close: pd.DataFrame = Dtk.get_panel_daily_pv_df(self.stock_list, start, self.end_date,
                                                        pv_type='close', adj_type='FORWARD')
        turn: pd.DataFrame = Dtk.get_panel_daily_info(self.stock_list, start, self.end_date, 'turn')
        close = close / close.shift(1) - 1

        def weight(data):
            if len(data) == 1:
                return data[0]
            else:
                return data[0] * math.exp(-(len(data) - 1) / 4 / self.n * 20) + weight(data[1:])

        turn_weight: pd.DataFrame = turn.rolling(window=self.n).apply(weight)
        multi: pd.DataFrame = (close * turn).rolling(window=self.n).apply(weight)
        factor_data: pd.DataFrame = multi / turn_weight
        factor_original_df = factor_data.loc[self.start_date: self.end_date].copy()
        ans_df = factor_original_df
        ans_df = ans_df.loc[self.start_date: self.end_date]
        ans_df = Dtk.convert_df_index_type(ans_df, 'date_int', 'timestamp')
        return ans_df
