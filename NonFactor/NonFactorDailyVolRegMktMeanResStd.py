# -*- coding: utf-8 -*-
"""
013542
"""
from Factor.DailyFactorBase import DailyFactorBase
import DataAPI.DataToolkit as Dtk
import numpy as np
import pandas as pd


class NonFactorDailyVolRegMktMeanResStd(DailyFactorBase):
    # 这个因子没有参数，但也须在初始化时预留一个"params"
    def __init__(self, alpha_factor_root_path, stock_list, start_date_int, end_date_int, params):
        super().__init__(alpha_factor_root_path)
        self.stock_list = stock_list
        self.start_date = start_date_int
        self.end_date = end_date_int
        self.n = params['n']

    def factor_calc(self):
        valid_start_date = Dtk.get_n_days_off(self.start_date, -self.n-2)[0]
        stock_vol = Dtk.get_panel_daily_pv_df(self.stock_list, valid_start_date, self.end_date,
                                                pv_type='volume')
        stock_mkt_cap = Dtk.get_panel_daily_info(self.stock_list, valid_start_date, self.end_date, 'mkt_cap_ard')
        stock_vol_log = np.log(stock_vol)
        stock_mkt_cap_log = np.log(stock_mkt_cap)
        stock_mkt_log_mean = stock_mkt_cap_log.mean(axis=1)
        trading_days = Dtk.get_trading_day(self.start_date, self.end_date)
        ans_df = stock_vol.copy()
        ans_df[:] = np.nan
        for date in trading_days:
            i = stock_vol_log.index.tolist().index(date)
            stock_vol_log_i = stock_vol_log.iloc[i - self.n + 1: i + 1]
            stock_mkt_cap_log_i = stock_mkt_log_mean.iloc[i - self.n + 1: i + 1]
            ff = np.vstack([np.array(stock_mkt_cap_log_i), np.ones(len(stock_mkt_cap_log_i))])
            reg = np.linalg.inv(ff.dot(ff.T)).dot(ff).dot(np.array(stock_vol_log_i))
            stock_res = stock_vol_log_i - ff.T.dot(reg)
            ans_df.loc[date] = stock_res.loc[date]
        # ----以下勿改动----
        ans_df = ans_df.loc[self.start_date: self.end_date]
        ans_df = Dtk.convert_df_index_type(ans_df, 'date_int', 'timestamp')
        return ans_df
