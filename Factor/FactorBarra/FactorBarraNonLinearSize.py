# -*- coding: utf-8 -*-
"""
@author: 006688
Created on 2019/02/19
Style: NonLinearSize
Definition: 1.0 · NLSIZE
NLSIZE: Cube of Size
"""
from Factor.DailyFactorBase import DailyFactorBase
import DataAPI.DataToolkit as Dtk
import numpy as np
import pandas as pd


class FactorBarraNonLinearSize(DailyFactorBase):
    # 这个因子没有参数，但也须在初始化时预留一个"params"
    def __init__(self, alpha_factor_root_path, stock_list, start_date_int, end_date_int, params):
        super().__init__(alpha_factor_root_path)
        self.stock_list = stock_list
        self.start_date = start_date_int
        self.end_date = end_date_int

    def factor_calc(self):
        mkt_cap_df = Dtk.get_panel_daily_info(self.stock_list, self.start_date, self.end_date, 'mkt_cap_ard')
        size = np.log(mkt_cap_df)
        size_cube = size ** 3
        trading_days = Dtk.get_trading_day(self.start_date, self.end_date)
        non_linear_size = pd.DataFrame(index=trading_days, columns=self.stock_list)
        for date in trading_days:
            factor_df = pd.concat([size.loc[date], size_cube.loc[date]], axis=1)
            factor_df = factor_df.dropna()
            factor_df.columns = ['size', 'cube']
            X = np.vstack((np.ones(factor_df['size'].size), np.array(factor_df['size'])))
            reg = np.linalg.inv(X.dot(X.T)).dot(X).dot(np.array(factor_df['cube']))
            res = factor_df['cube'] - X.T.dot(reg)
            non_linear_size.loc[date] = res
        ans_df = non_linear_size.astype(float)
        # ----以下勿改动----
        ans_df = ans_df.loc[self.start_date: self.end_date]
        ans_df = Dtk.convert_df_index_type(ans_df, 'date_int', 'timestamp')
        return ans_df
