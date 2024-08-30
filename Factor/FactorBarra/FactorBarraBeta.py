# -*- coding: utf-8 -*-
"""
@author: 006688
Created on 2019/02/18
Style: Size
Definition: 1.0 · BETA
BETA: Beta
"""
from Factor.DailyFactorBase import DailyFactorBase
import DataAPI.DataToolkit as Dtk
import numpy as np
import pandas as pd


class FactorBarraBeta(DailyFactorBase):
    # 这个因子没有参数，但也须在初始化时预留一个"params"
    def __init__(self, alpha_factor_root_path, stock_list, start_date_int, end_date_int, params):
        super().__init__(alpha_factor_root_path)
        self.stock_list = stock_list
        self.start_date = start_date_int
        self.end_date = end_date_int
        self.trail = 252
        self.half_life = 63
        self.complete_stock_list = Dtk.get_complete_stock_list()

    def factor_calc(self):
        valid_start_date = Dtk.get_n_days_off(self.start_date, -self.trail-2)[0]
        estimation_universe = Dtk.get_panel_daily_info(self.complete_stock_list, valid_start_date, self.end_date,
                                                       'risk_universe')
        mkt_cap = Dtk.get_panel_daily_info(self.complete_stock_list, valid_start_date, self.end_date, 'mkt_cap_ard')
        stock_close = Dtk.get_panel_daily_pv_df(self.complete_stock_list, valid_start_date, self.end_date,
                                                pv_type='close', adj_type='FORWARD')
        stock_return = stock_close / stock_close.shift(1) - 1
        universe_cap_return = (stock_return[estimation_universe == 1] * mkt_cap[estimation_universe == 1]).sum(
            axis=1) / mkt_cap[estimation_universe == 1].sum(axis=1)
        # 由于2009年7月前universe数据缺失，不对股票池进行过滤
        universe_cap_return[universe_cap_return.isnull()] = stock_return.mul(mkt_cap).sum(axis=1) / mkt_cap.sum(axis=1)
        trading_days = Dtk.get_trading_day(self.start_date, self.end_date)
        alpha = 0.5 ** (1 / self.half_life)
        weighted_window = np.logspace(self.trail, 1, self.trail, base=alpha)
        stock_beta = pd.DataFrame(index=trading_days, columns=self.stock_list)
        for date in trading_days:
            i = stock_return.index.tolist().index(date)
            # 数据长度至少为half_life
            if i < self.half_life:
                continue
            trail = min(self.trail, i)
            stock_return_i = stock_return.iloc[i - trail + 1:i + 1]
            stock_return_i = stock_return_i.loc[:, self.stock_list]
            universe_return_i = universe_cap_return.iloc[i - trail + 1:i + 1]
            weight_mat = np.diag(weighted_window[-trail:])
            X = np.vstack((np.ones(universe_return_i.size), np.array(universe_return_i)))
            stock_reg = np.linalg.inv(X.dot(weight_mat).dot(X.T)).dot(X).dot(weight_mat).dot(np.array(stock_return_i))
            stock_beta.loc[date] = stock_reg[1, :]
        ans_df = stock_beta.astype(float)
        # ----以下勿改动----
        ans_df = ans_df.loc[self.start_date: self.end_date]
        ans_df = Dtk.convert_df_index_type(ans_df, 'date_int', 'timestamp')
        return ans_df
