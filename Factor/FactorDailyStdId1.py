# -*- coding: utf-8 -*-
"""
@author: 006688
revised on 2019/02/22
特质波动率1：个股最近N 日收益率序列对指数日收益率序列进行一元线性回归的残差的标准差
"""
from Factor.DailyFactorBase import DailyFactorBase
import DataAPI.DataToolkit as Dtk
import numpy as np


class FactorDailyStdId1(DailyFactorBase):
    # 这个因子没有参数，但也须在初始化时预留一个"params"
    def __init__(self, alpha_factor_root_path, stock_list, start_date_int, end_date_int, params):
        super().__init__(alpha_factor_root_path)
        self.stock_list = stock_list
        self.start_date = start_date_int
        self.end_date = end_date_int
        self.index_code = params['index_code']
        self.n = params['n']

    def factor_calc(self):
        valid_start_date = Dtk.get_n_days_off(self.start_date, -self.n-2)[0]
        stock_close = Dtk.get_panel_daily_pv_df(self.stock_list, valid_start_date, self.end_date,
                                                pv_type='close', adj_type='FORWARD')
        index_close = Dtk.get_panel_daily_pv_df([self.index_code], valid_start_date, self.end_date, pv_type='close')
        stock_pct_chg = stock_close / stock_close.shift(1) - 1
        index_pct_chg = index_close / index_close.shift(1) - 1
        trading_days = Dtk.get_trading_day(self.start_date, self.end_date)
        ans_df = stock_pct_chg.copy()
        ans_df[:] = np.nan
        for date in trading_days:
            i = stock_pct_chg.index.tolist().index(date)
            stock_pct_chg_i = stock_pct_chg.iloc[i - self.n + 1: i + 1]
            index_pct_chg_i = index_pct_chg.iloc[i - self.n + 1: i + 1]
            index_pct_chg_i = index_pct_chg_i[self.index_code]
            X = np.vstack((np.ones(index_pct_chg_i.size), np.array(index_pct_chg_i)))
            stock_reg = np.linalg.inv(X.dot(X.T)).dot(X).dot(np.array(stock_pct_chg_i))
            stock_res = stock_pct_chg_i - X.T.dot(stock_reg)
            ans_df.loc[date] = stock_res.std(axis=0)
        # ----以下勿改动----
        ans_df = ans_df.loc[self.start_date: self.end_date]
        ans_df = Dtk.convert_df_index_type(ans_df, 'date_int', 'timestamp')
        return ans_df
