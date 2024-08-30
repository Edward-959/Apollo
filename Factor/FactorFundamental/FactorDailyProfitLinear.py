#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019/1/14 10:14
# @Author  : 011673
import DataAPI.DataToolkit as Dtk
from Factor.DailyFactorBase import DailyFactorBase
import pandas as pd
import numpy as np


def get_predict(data):
    data = data[data != 0]
    data = data[~np.isnan(data)]
    if data.__len__()<4:
        return np.nan
    else:
        x=np.arange(0,len(data))
        coeffs=np.polyfit(x,data,1)
        return coeffs[0]*(len(data)+1)+coeffs[1]


class FactorDailyProfitLinear(DailyFactorBase):
    def __init__(self, alpha_factor_root_path, stock_list, start_date_int, end_date_int, params):
        super().__init__(alpha_factor_root_path)
        self.stock_list = stock_list
        self.start_date = start_date_int
        self.end_date = end_date_int

    def factor_calc(self):
        valid_start = Dtk.get_n_days_off(self.start_date, -510)[0]
        factor_data = Dtk.get_panel_daily_info(self.stock_list, valid_start, self.end_date, "profit_ttm2")
        data: pd.DataFrame = (factor_data - factor_data.shift(1)) / factor_data.shift(1)
        data = data.rolling(window=510, min_periods=250).apply(get_predict)
        ans_df = data.loc[self.start_date: self.end_date]
        ans_df = Dtk.convert_df_index_type(ans_df, 'date_int', 'timestamp')
        return ans_df