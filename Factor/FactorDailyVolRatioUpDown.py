# -*- coding: utf-8 -*-
"""
@author: 006688
created on 2018/12/06
revised on 2019/02/22
近n日上涨日成交量之和与下跌日成交量之和的比率
"""
from Factor.DailyFactorBase import DailyFactorBase
import DataAPI.DataToolkit as Dtk


class FactorDailyVolRatioUpDown(DailyFactorBase):
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
        data_volume = Dtk.get_panel_daily_pv_df(self.stock_list, valid_start_date, self.end_date, pv_type='volume')
        data_return = data_close / data_close.shift(1) - 1
        volume_up = data_volume[data_return > 0].fillna(0).rolling(self.n).sum()
        volume_down = data_volume[data_return < 0].fillna(0).rolling(self.n).sum()
        volume_mid = data_volume[data_return == 0].fillna(0).rolling(self.n).sum()
        ans_df = (volume_up * 2 + volume_mid) / (volume_down * 2 + volume_mid) * 100
        # ----以下勿改动----
        ans_df = ans_df.loc[self.start_date: self.end_date]
        ans_df = Dtk.convert_df_index_type(ans_df, 'date_int', 'timestamp')
        return ans_df
