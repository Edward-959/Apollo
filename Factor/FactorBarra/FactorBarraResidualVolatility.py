# -*- coding: utf-8 -*-
"""
@author: 006688
Created on 2019/02/18
Style: Momentum
Definition: 0.74 · DASTD + 0.16 · CMRA + 0.10 · HSIGMA
DASTD: Daily standard deviation
CMRA: Cumulative range
HSIGMA: Historical sigma
"""
from Factor.DailyFactorBase import DailyFactorBase
import DataAPI.DataToolkit as Dtk
import numpy as np
import pandas as pd
import os


class FactorBarraResidualVolatility(DailyFactorBase):
    # 这个因子没有参数，但也须在初始化时预留一个"params"
    def __init__(self, alpha_factor_root_path, stock_list, start_date_int, end_date_int, params):
        super().__init__(alpha_factor_root_path)
        self.stock_list = stock_list
        self.start_date = start_date_int
        self.end_date = end_date_int
        self.trail = 252
        self.half_life_dastd = 42
        self.half_life_hsigma = 63
        self.complete_stock_list = Dtk.get_complete_stock_list()

    def factor_calc(self):
        valid_start_date = Dtk.get_n_days_off(self.start_date, -self.trail-2)[0]
        stock_close = Dtk.get_panel_daily_pv_df(self.complete_stock_list, valid_start_date, self.end_date,
                                                pv_type='close', adj_type='FORWARD')
        stock_return = stock_close / stock_close.shift(1) - 1
        alpha_dastd = 0.5 ** (1 / self.half_life_dastd)
        alpha_hsigma = 0.5 ** (1 / self.half_life_hsigma)
        weighted_window_dastd = np.logspace(self.trail, 1, self.trail, base=alpha_dastd)
        weighted_window_hsigma = np.logspace(self.trail, 1, self.trail, base=alpha_hsigma)
        estimation_universe = Dtk.get_panel_daily_info(self.complete_stock_list, valid_start_date, self.end_date,
                                                       'risk_universe')
        mkt_cap = Dtk.get_panel_daily_info(self.complete_stock_list, valid_start_date, self.end_date, 'mkt_cap_ard')
        universe_cap_return = (stock_return[estimation_universe == 1] * mkt_cap[estimation_universe == 1]).sum(
            axis=1) / mkt_cap[estimation_universe == 1].sum(axis=1)
        # 由于2009年7月前universe数据缺失，不对股票池进行过滤
        universe_cap_return[universe_cap_return.isnull()] = stock_return.mul(mkt_cap).sum(axis=1) / mkt_cap.sum(axis=1)
        # Load Factor Size
        size_factor_path = os.path.join(self.alpha_factor_root_path, "BarraFactors", "F_B_Size.h5")
        size_df = self.get_non_factor_df(size_factor_path)
        size_df = Dtk.convert_df_index_type(size_df, 'timestamp', 'date_int')
        trading_days = Dtk.get_trading_day(self.start_date, self.end_date)
        residual_volatility = pd.DataFrame(index=trading_days, columns=self.stock_list)
        month_cut = np.linspace(0, 252, 13)
        for date in trading_days:
            i = stock_return.index.tolist().index(date)
            if i < self.half_life_hsigma:
                continue
            trail = min(self.trail, i)
            stock_close_i = stock_close.iloc[i-trail:i+1]
            stock_close_i = stock_close_i.loc[:, self.stock_list]
            stock_return_i = stock_return.iloc[i - trail + 1:i + 1]
            stock_return_i = stock_return_i.loc[:, self.stock_list]
            # compute DASTD
            weighted_window = weighted_window_dastd[-trail:]
            return_mean = stock_return_i.mul(weighted_window, axis=0).sum(axis=0) / weighted_window.sum()
            return_bias = np.square(stock_return_i.sub(return_mean, axis=1))
            return_var = return_bias.mul(weighted_window, axis=0).sum(axis=0) / weighted_window.sum()
            dastd = np.sqrt(return_var)
            # compute CMRA
            # 数量级比DASTD和HSIGMA大，暂时先不用
            # month_close = stock_close_i.iloc[month_cut]
            # month_return = month_close.diff() / month_close.shift(1)
            # month_return = np.log(1 + month_return.iloc[1:])
            # month_return_cum = month_return.sort_index(axis=0, ascending=False).cumsum()
            # cum_max = (1 + month_return_cum.max(axis=0)).clip_lower(0).replace(0, np.nan)
            # cum_min = (1 + month_return_cum.min(axis=0)).clip_lower(0).replace(0, np.nan)
            # cmra = np.log(cum_max) - np.log(cum_min)
            # compute HSIGMA
            weight_mat = np.diag(weighted_window_hsigma[-trail:])
            universe_return_i = universe_cap_return.iloc[i - trail + 1:i + 1]
            X = np.vstack((np.ones(universe_return_i.size), np.array(universe_return_i)))
            stock_reg = np.linalg.inv(X.dot(weight_mat).dot(X.T)).dot(X).dot(weight_mat).dot(np.array(stock_return_i))
            stock_beta = stock_reg[1, :]
            stock_res = stock_return_i - X.T.dot(stock_reg)
            hsigma = stock_res.std()
            # orthogonalize factor Residual Volatility with respect to factor Beta and Size
            factor_set = pd.DataFrame(stock_beta, index=self.stock_list, columns=['beta'])
            factor_set['size'] = size_df.loc[date, self.stock_list]
            factor_set['vola'] = 0.75 * dastd + 0.25 * hsigma  # 0.74 * dastd + 0.16 * cmra + 0.1 * hsigma
            factor_reg = factor_set.dropna(axis=0)
            X = np.array(factor_reg[['beta', 'size']])
            vola_reg = np.linalg.inv(X.T.dot(X)).dot(X.T).dot(np.array(factor_reg['vola']))
            vola_res = factor_reg['vola'] - X.dot(vola_reg)
            residual_volatility.loc[date] = vola_res
        ans_df = residual_volatility.astype(float)
        # ----以下勿改动----
        ans_df = ans_df.loc[self.start_date: self.end_date]
        ans_df = Dtk.convert_df_index_type(ans_df, 'date_int', 'timestamp')
        return ans_df
