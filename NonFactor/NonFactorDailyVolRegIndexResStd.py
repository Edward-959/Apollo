# -*- coding: utf-8 -*-
"""

"""
from Factor.DailyFactorBase import DailyFactorBase
import DataAPI.DataToolkit as Dtk
import numpy as np
import pandas as pd


class NonFactorDailyVolRegIndexResStd(DailyFactorBase):
    # 这个因子没有参数，但也须在初始化时预留一个"params"
    def __init__(self, alpha_factor_root_path, stock_list, start_date_int, end_date_int, params):
        super().__init__(alpha_factor_root_path)
        self.stock_list = stock_list
        self.start_date = start_date_int
        self.end_date = end_date_int
        self.index_code = params['index_code']
        self.n = params['n']

    def factor_calc(self):
        valid_start_date = Dtk.get_n_days_off(self.start_date, -self.n-30)[0]
        stock_vol = Dtk.get_panel_daily_pv_df(self.stock_list, valid_start_date, self.end_date,
                                                pv_type='volume')
        index_vol = Dtk.get_panel_daily_pv_df([self.index_code], valid_start_date, self.end_date, pv_type='volume')

        stock_vol_adj = stock_vol
        temp_df = pd.concat([stock_vol_adj, np.log(index_vol)], axis=1)
        factor_mean = temp_df.mean(axis=1)
        factor_std = temp_df.std(axis=1)
        value_df = temp_df.sub(factor_mean, axis=0)
        temp_df_zscore = value_df.div(factor_std, axis=0)

        index_vol_log = temp_df_zscore.iloc[:,-1]
        stock_vol_zscore = temp_df_zscore.iloc[:,:-1]
        trading_days = Dtk.get_trading_day(self.start_date, self.end_date)
        ans_df = stock_vol.copy()
        ans_df[:] = np.nan
        for date in trading_days:
            i = stock_vol_zscore.index.tolist().index(date)
            stock_vol_log_i = stock_vol_zscore.iloc[i - self.n + 1: i + 1]
            index_vol_log_i = index_vol_log.iloc[i - self.n + 1: i + 1]
            ff = np.vstack([np.array(index_vol_log_i), np.ones(len(index_vol_log_i))])
            reg = np.linalg.inv(ff.dot(ff.T)).dot(ff).dot(np.array(stock_vol_log_i))
            stock_res = stock_vol_log_i - ff.T.dot(reg)
            ans_df.loc[date] = stock_res.loc[date]
        # ----以下勿改动----
        ans_df = ans_df.loc[self.start_date: self.end_date]
        ans_df = Dtk.convert_df_index_type(ans_df, 'date_int', 'timestamp')
        return ans_df
