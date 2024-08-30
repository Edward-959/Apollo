"""
Created by 006566 on 2019/3/7
"""

import DataAPI.DataToolkit as Dtk
from Factor.DailyFactorBase import DailyFactorBase


class NonFactorDailyCloseVSVwap(DailyFactorBase):
    def __init__(self, alpha_factor_root_path, stock_list, start_date_int, end_date_int, params):
        super().__init__(alpha_factor_root_path)
        self.stock_list = stock_list
        self.start_date = start_date_int
        self.end_date = end_date_int

    def factor_calc(self):
        close_df = Dtk.get_panel_daily_pv_df(self.stock_list, self.start_date, self.end_date, 'close')
        amt_df = Dtk.get_panel_daily_pv_df(self.stock_list, self.start_date, self.end_date, 'amt')
        volume_df = Dtk.get_panel_daily_pv_df(self.stock_list, self.start_date, self.end_date, 'volume')
        vwap_df = amt_df / volume_df
        return_df = close_df / vwap_df - 1
        # ----以下勿改动----
        ans_df = return_df.loc[self.start_date: self.end_date]
        ans_df = Dtk.convert_df_index_type(ans_df, 'date_int', 'timestamp')
        return ans_df
