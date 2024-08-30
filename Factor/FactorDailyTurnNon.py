# -*- coding: utf-8 -*-
"""
@author: 006688
revised on 2019/02/22
非流动性因子：过去n日（每日涨跌幅绝对值/成交额）的均值取对数
"""
from Factor.DailyFactorBase import DailyFactorBase
import DataAPI.DataToolkit as Dtk
import numpy as np


class FactorDailyTurnNon(DailyFactorBase):
    # 这个因子没有参数，但也须在初始化时预留一个"params"
    def __init__(self, alpha_factor_root_path, stock_list, start_date_int, end_date_int, params):
        super().__init__(alpha_factor_root_path)
        self.stock_list = stock_list
        self.start_date = start_date_int
        self.end_date = end_date_int
        self.n = params['n']

    def factor_calc(self):
        valid_start_date = Dtk.get_n_days_off(self.start_date, -self.n-2)[0]
        data_close = Dtk.get_panel_daily_pv_df(self.stock_list, valid_start_date, self.end_date,
                                               pv_type='close', adj_type='FORWARD')
        data_amt = Dtk.get_panel_daily_pv_df(self.stock_list, valid_start_date, self.end_date, pv_type='amt')
        data_return = data_close / data_close.shift(1) - 1
        non_liquid = abs(data_return) / data_amt
        ans_df = np.log(non_liquid.rolling(self.n).mean())
        # ----以下勿改动----
        ans_df = ans_df.loc[self.start_date: self.end_date]
        ans_df = Dtk.convert_df_index_type(ans_df, 'date_int', 'timestamp')
        return ans_df
