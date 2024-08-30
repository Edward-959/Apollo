#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019/2/21 14:21
# @Author  : 011673
from Factor.DailyFactorBase import DailyFactorBase
import DataAPI.DataToolkit as Dtk
import pandas as pd
import numpy as np
import copy


class NonFactorDailyUpVolatilityRatio(DailyFactorBase):
    def __init__(self, alpha_factor_root_path, stock_list, start_date_int, end_date_int, params):
        super().__init__(alpha_factor_root_path)
        self.stock_list = stock_list
        self.start_date = start_date_int
        self.end_date = end_date_int
        self.n = params["n"]
        self.close_df = None

    def factor_calc(self):
        start = Dtk.get_n_days_off(self.start_date, -22)[0]
        close: pd.DataFrame = Dtk.get_panel_daily_pv_df(self.stock_list, start, self.end_date,
                                                        pv_type='close', adj_type='FORWARD')
        ret: pd.DataFrame = close / close.shift(1)
        ret_log = pd.DataFrame(np.log(ret.values), index=ret.index, columns=ret.columns)
        ret_log: pd.DataFrame = 1000 * ret_log * ret_log
        original_ret_log = copy.deepcopy(ret_log)
        condition = ret < 1
        ret_log[condition == True] = 0
        ret_log = ret_log.rolling(window=self.n, min_periods=1).sum()
        original_ret_log = original_ret_log.rolling(window=self.n, min_periods=1).sum()
        factor_data = ret_log / original_ret_log
        # ----以下勿改动----
        ans_df = factor_data.loc[self.start_date: self.end_date]
        ans_df = Dtk.convert_df_index_type(ans_df, 'date_int', 'timestamp')
        return ans_df
