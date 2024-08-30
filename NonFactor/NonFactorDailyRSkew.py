#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019/2/25 9:38
# @Author  : 011673
import DataAPI.DataToolkit as Dtk
from Factor.DailyFactorBase import DailyFactorBase
import pandas as pd
import math
import numpy as np


class NonFactorDailyRSkew(DailyFactorBase):
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
        ret: pd.DataFrame = close / close.shift(1)
        ret_log = pd.DataFrame(np.log(ret.values), index=ret.index, columns=ret.columns)
        ret_log_square = ret_log * ret_log
        rvar = ret_log_square.rolling(window=self.n, min_periods=1).sum()
        rvar = rvar * rvar * rvar
        rvar = pd.DataFrame(np.sqrt(rvar.values), index=rvar.index, columns=rvar.columns)
        #
        temp: pd.DataFrame = ret_log * ret_log * ret_log
        temp = math.sqrt(self.n) * temp.rolling(window=self.n, min_periods=1).sum()
        factor_data: pd.DataFrame = temp / rvar
        factor_original_df = factor_data.loc[self.start_date: self.end_date].copy()
        ans_df = factor_original_df
        ans_df = ans_df.loc[self.start_date: self.end_date]
        ans_df = Dtk.convert_df_index_type(ans_df, 'date_int', 'timestamp')
        return ans_df
