# -*- coding: utf-8 -*-
"""
@author: 006688
revised on 2019/02/22
用个股每日最高价除以前一日收盘价计算日内最大涨幅，此处有复权处理，再计算最大涨幅序列的标准差
"""
from Factor.DailyFactorBase import DailyFactorBase
import DataAPI.DataToolkit as Dtk


class FactorDailyStdHigh(DailyFactorBase):
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
        data_high = Dtk.get_panel_daily_pv_df(self.stock_list, valid_start_date, self.end_date,
                                              pv_type='high', adj_type='FORWARD')
        data_return_high = data_high / data_close.shift(1) - 1
        ans_df = data_return_high.rolling(self.n).std()
        # ----以下勿改动----
        ans_df = ans_df.loc[self.start_date: self.end_date]
        ans_df = Dtk.convert_df_index_type(ans_df, 'date_int', 'timestamp')
        return ans_df
