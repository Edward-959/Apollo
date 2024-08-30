# -*- coding: utf-8 -*-
"""
@author: 006566, 2019/4/8
从Wind落地库的利润表获取经营活动产生的现金流量净额，再除以总市值
"""

import DataAPI.DataToolkit as Dtk
from Factor.DailyFactorBase import DailyFactorBase


class FactorDailyOCFP(DailyFactorBase):
    # 这个因子没有参数，但也须在初始化时预留一个"params"
    def __init__(self, alpha_factor_root_path, stock_list, start_date_int, end_date_int, params):
        super().__init__(alpha_factor_root_path)
        self.stock_list = stock_list
        self.start_date = start_date_int
        self.end_date = end_date_int

    def factor_calc(self):
        ocf = Dtk.get_daily_wind_quarterly_data(self.stock_list, 'AShareCashFlow',
                                                'NET_CASH_FLOWS_OPER_ACT',
                                                self.start_date, self.end_date, 'ttm')
        mkt_cap = Dtk.get_panel_daily_info(self.stock_list, self.start_date, self.end_date, 'mkt_cap_ard')
        ans_df = ocf / mkt_cap
        # ----以下勿改动----
        ans_df = ans_df.loc[self.start_date: self.end_date]
        ans_df = Dtk.convert_df_index_type(ans_df, 'date_int', 'timestamp')
        return ans_df
