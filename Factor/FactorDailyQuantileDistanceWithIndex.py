#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019/4/8 20:58
# @Author  : 011673

import DataAPI.DataToolkit as Dtk
from Factor.DailyFactorBase import DailyFactorBase
import pandas as pd
import numpy as np
import math


class FactorDailyQuantileDistanceWithIndex(DailyFactorBase):
    def __init__(self, alpha_factor_root_path, stock_list, start_date_int, end_date_int, params):
        super().__init__(alpha_factor_root_path)
        self.stock_list = stock_list
        self.start_date = start_date_int
        self.end_date = end_date_int
        self.n = params["n"]

    def factor_calc(self):
        def rank_df(data):
            return np.argsort(data)[-1]
        start = Dtk.get_n_days_off(self.start_date, -self.n)[0]
        close: pd.DataFrame = Dtk.get_panel_daily_pv_df(self.stock_list, start, self.end_date,
                                                        pv_type='close', adj_type='FORWARD')
        index_close:pd.DataFrame=Dtk.get_panel_daily_pv_df(['000905.SH'],start, self.end_date,
                                                        pv_type='close')
        index_close_rank=index_close.rolling(window=self.n).apply(rank_df)
        close_rank=close.rolling(window=self.n).apply(rank_df)
        factor_data=(close_rank.T-index_close_rank['000905.SH']).T
        factor_original_df = factor_data.loc[self.start_date: self.end_date].copy()
        ans_df = factor_original_df
        ans_df = ans_df.loc[self.start_date: self.end_date]
        ans_df = Dtk.convert_df_index_type(ans_df, 'date_int', 'timestamp')
        return ans_df