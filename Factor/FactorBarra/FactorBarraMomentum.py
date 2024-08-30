# -*- coding: utf-8 -*-
"""
@author: 006688
Created on 2019/02/18
Style: Momentum
Definition: 1.0 · RSTR
RSTR: Relative strength
"""
from Factor.DailyFactorBase import DailyFactorBase
import DataAPI.DataToolkit as Dtk
import numpy as np
import pandas as pd


class FactorBarraMomentum(DailyFactorBase):
    # 这个因子没有参数，但也须在初始化时预留一个"params"
    def __init__(self, alpha_factor_root_path, stock_list, start_date_int, end_date_int, params):
        super().__init__(alpha_factor_root_path)
        self.stock_list = stock_list
        self.start_date = start_date_int
        self.end_date = end_date_int
        self.trail = 504
        self.half_life = 126
        self.lag = 21

    def factor_calc(self):
        valid_start_date = Dtk.get_n_days_off(self.start_date, -self.trail-self.lag-2)[0]
        stock_close = Dtk.get_panel_daily_pv_df(self.stock_list, valid_start_date, self.end_date,
                                                pv_type='close', adj_type='FORWARD')
        stock_return = stock_close / stock_close.shift(1) - 1
        ln_return = np.log(1 + stock_return)
        alpha = 0.5 ** (1 / self.half_life)
        weighted_window = np.logspace(self.trail, 1, self.trail, base=alpha)
        trading_days = Dtk.get_trading_day(self.start_date, self.end_date)
        stock_momentum = pd.DataFrame(index=trading_days, columns=self.stock_list)
        for date in trading_days:
            i = stock_return.index.tolist().index(date)
            if i < self.half_life:
                continue
            trail = min(self.trail, i - self.lag)
            ln_return_i = ln_return.iloc[i - trail - self.lag + 1:i - self.lag + 1].mul(weighted_window[-trail:], axis=0)
            stock_momentum.loc[date] = ln_return_i.sum(axis=0, min_count=1).div(weighted_window[-trail:].sum())
        ans_df = stock_momentum.astype(float)
        # ----以下勿改动----
        ans_df = ans_df.loc[self.start_date: self.end_date]
        ans_df = Dtk.convert_df_index_type(ans_df, 'date_int', 'timestamp')
        return ans_df
