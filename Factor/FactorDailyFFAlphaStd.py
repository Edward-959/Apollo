# -*- coding: utf-8 -*-
"""
@author: 013542
中国版三因子回归截距项（Alpha）,过去一段时间的标准差
"""
from Factor.DailyFactorBase import DailyFactorBase
import DataAPI.DataToolkit as Dtk
import numpy as np
import os


class FactorDailyFFAlphaStd(DailyFactorBase):
    # 这个因子没有参数，但也须在初始化时预留一个"params"
    def __init__(self, alpha_factor_root_path, stock_list, start_date_int, end_date_int, params):
        super().__init__(alpha_factor_root_path)
        self.stock_list = stock_list
        self.start_date = start_date_int
        self.end_date = end_date_int
        self.index_code = params['index_code']
        self.n = params['n']

    def factor_calc(self):
        valid_start_date = Dtk.get_n_days_off(self.start_date, -self.n - 30)[0]
        valid_start_date1 = Dtk.get_n_days_off(self.start_date, -self.n - 2)[0]
        stock_close = Dtk.get_panel_daily_pv_df(self.stock_list, valid_start_date, self.end_date,
                                                pv_type='close', adj_type='FORWARD')
        index_close = Dtk.get_panel_daily_pv_df([self.index_code], valid_start_date, self.end_date, pv_type='close')
        stock_pct_chg = stock_close / stock_close.shift(1) - 1
        index_pct_chg = index_close / index_close.shift(1) - 1
        # 读取SMB和HML因子值
        ff_factor_path = os.path.join(self.alpha_factor_root_path, "AlphaNonFactors", "NF_D_CHNFamaFrench.h5")
        ff_factor = self.get_non_factor_df(ff_factor_path)
        ff_factor = Dtk.convert_df_index_type(ff_factor, 'timestamp', 'date_int')
        trading_days = Dtk.get_trading_day(valid_start_date1, self.end_date)
        ans_df = stock_pct_chg.copy()
        ans_df[:] = np.nan
        for date in trading_days:
            i = stock_pct_chg.index.tolist().index(date)
            stock_pct_chg_i = stock_pct_chg.iloc[i - self.n + 1: i + 1]
            index_pct_chg_i = index_pct_chg.iloc[i - self.n + 1: i + 1]
            index_pct_chg_i = index_pct_chg_i[self.index_code]
            i_ff = ff_factor.index.tolist().index(date)
            SMB_i = ff_factor.iloc[i_ff - self.n + 1: i_ff + 1, 0]
            HML_i = ff_factor.iloc[i_ff - self.n + 1: i_ff + 1, 1]
            ff = np.vstack([np.array(index_pct_chg_i), np.array(SMB_i), np.array(HML_i), np.ones(len(index_pct_chg_i))])
            reg_result = np.linalg.inv(ff.dot(ff.T)).dot(ff).dot(np.array(stock_pct_chg_i))
            ans_df.loc[date] = reg_result[3, :]
        ans_df = ans_df.rolling(self.n).std()
        # ----以下勿改动----
        ans_df = ans_df.loc[self.start_date: self.end_date]
        ans_df = Dtk.convert_df_index_type(ans_df, 'date_int', 'timestamp')
        return ans_df
