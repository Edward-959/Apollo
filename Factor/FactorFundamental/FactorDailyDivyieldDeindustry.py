#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019/1/11 14:51
# @Author  : 011673
import DataAPI.DataToolkit as Dtk
from Factor.DailyFactorBase import DailyFactorBase
import pandas as pd
import numpy as np
import copy


class FactorDailyDivyieldDeindustry(DailyFactorBase):
    def __init__(self, alpha_factor_root_path, stock_list, start_date_int, end_date_int, params):
        super().__init__(alpha_factor_root_path)
        self.stock_list = stock_list
        self.start_date = start_date_int
        self.end_date = end_date_int

    def factor_calc(self):
        valid_start = Dtk.get_n_days_off(self.start_date, 10)[0]
        factor_data = Dtk.get_panel_daily_info(self.stock_list, valid_start, self.end_date, "dividendyield2")
        industry = Dtk.get_panel_daily_info(self.stock_list, valid_start, self.end_date, info_type='industry3')
        factor_data.replace(np.inf, np.nan, inplace=True)
        result = None
        for i in range(1, 32):
            print('process {}/{}'.format(i, 32))
            temp_industry: pd.DataFrame = industry.clip_upper(0).copy()
            temp_industry[industry == i] = 1
            temp_industry.replace(0, np.nan, inplace=True)
            temp_factor = copy.deepcopy(factor_data)
            temp_factor: pd.DataFrame = temp_factor * temp_industry
            temp_factor_mean = temp_factor.mean(axis=1)
            temp_factor_std = temp_factor.std(axis=1)
            temp_factor = temp_factor.apply(lambda x: x - temp_factor_mean)
            temp_factor = temp_factor.apply(lambda x: x / temp_factor_std)
            temp_factor.replace(np.nan, 0, inplace=True)
            if result is None:
                result = temp_factor
            else:
                result = result + temp_factor
        factor_data = result * factor_data / factor_data
        temp = industry.clip_upper(0).copy()
        temp.replace(0, 1, inplace=True)
        ans_df = factor_data * temp
        ans_df = ans_df.loc[self.start_date: self.end_date]
        ans_df = Dtk.convert_df_index_type(ans_df, 'date_int', 'timestamp')
        return ans_df