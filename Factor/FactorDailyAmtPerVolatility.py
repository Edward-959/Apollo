# -*- coding: utf-8 -*-
"""
created on 2018/11/30
revised on 2019/02/21
@author: 006688
过去n日成交金额与过去n日股价波动率的比值，股价波动率采用高低价计算法
"""
from Factor.DailyFactorBase import DailyFactorBase
import DataAPI.DataToolkit as Dtk
import numpy as np


class FactorDailyAmtPerVolatility(DailyFactorBase):
    # 这个因子没有参数，但也须在初始化时预留一个"params"
    def __init__(self, alpha_factor_root_path, stock_list, start_date_int, end_date_int, params):
        super().__init__(alpha_factor_root_path)
        self.stock_list = stock_list
        self.start_date = start_date_int
        self.end_date = end_date_int
        self.n = params['n']

    def factor_calc(self):
        valid_start_date = Dtk.get_n_days_off(self.start_date, -self.n-2)[0]
        data_amt = Dtk.get_panel_daily_pv_df(self.stock_list, valid_start_date, self.end_date, pv_type='amt')
        data_close = Dtk.get_panel_daily_pv_df(self.stock_list, valid_start_date, self.end_date,
                                               pv_type='close', adj_type='FORWARD')
        data_high = Dtk.get_panel_daily_pv_df(self.stock_list, valid_start_date, self.end_date,
                                              pv_type='high', adj_type='FORWARD')
        data_low = Dtk.get_panel_daily_pv_df(self.stock_list, valid_start_date, self.end_date,
                                             pv_type='low', adj_type='FORWARD')
        data_volatility = ((data_high.rolling(self.n).max() - data_low.rolling(self.n).min()) /
                           data_close.shift(self.n)).replace(0, np.nan)
        ans_df = data_amt.rolling(self.n).sum() / np.log(data_volatility)
        # ----以下勿改动----
        ans_df = ans_df.loc[self.start_date: self.end_date]
        ans_df = Dtk.convert_df_index_type(ans_df, 'date_int', 'timestamp')
        return ans_df
