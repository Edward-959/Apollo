# -*- coding: utf-8 -*-
"""
@author: 006688
Created on 2018/11/22
Revised on 2019/01/23
将过去n日的隔夜收益率（今开 /昨收 -1）小于-0.01的值求加权平均
"""
from Factor.DailyFactorBase import DailyFactorBase
import DataAPI.DataToolkit as Dtk


class FactorDailyOvernightNegReturn(DailyFactorBase):
    # 这个因子没有参数，但也须在初始化时预留一个"params"
    def __init__(self, alpha_factor_root_path, stock_list, start_date_int, end_date_int, params):
        super().__init__(alpha_factor_root_path)
        self.stock_list = stock_list
        self.start_date = start_date_int
        self.end_date = end_date_int
        self.n = params['n']

    def factor_calc(self):
        valid_start_date = Dtk.get_n_days_off(self.start_date, -self.n-2)[0]
        data_pre_close = Dtk.get_panel_daily_pv_df(self.stock_list, valid_start_date, self.end_date,
                                                   pv_type='pre_close', adj_type='NONE')
        data_open = Dtk.get_panel_daily_pv_df(self.stock_list, valid_start_date, self.end_date,
                                              pv_type='open', adj_type='NONE')
        overnight_return = data_open / data_pre_close - 1
        overnight_return[overnight_return > -0.01] = 0
        ans_df = overnight_return.ewm(com=self.n).mean()
        # ----以下勿改动----
        ans_df = ans_df.loc[self.start_date: self.end_date]
        ans_df = Dtk.convert_df_index_type(ans_df, 'date_int', 'timestamp')
        return ans_df
