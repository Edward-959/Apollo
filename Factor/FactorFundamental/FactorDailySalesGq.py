# -*- coding: utf-8 -*-
"""
@author: 006566, 2018/11/22
revised on 2019/2/27
SalesGq, 营业总收入（当季）同比增长率，'qfa_yoysales'，这个数据在Wind终端原是季频的，通过backfill方法变为日频的
"""

import DataAPI.DataToolkit as Dtk
from Factor.DailyFactorBase import DailyFactorBase


class FactorDailySalesGq(DailyFactorBase):
    # 这个因子没有参数，但也须在初始化时预留一个"params"
    def __init__(self, alpha_factor_root_path, stock_list, start_date_int, end_date_int, params):
        super().__init__(alpha_factor_root_path)
        self.stock_list = stock_list
        self.start_date = start_date_int
        self.end_date = end_date_int

    def factor_calc(self):
        ans_df = Dtk.get_panel_daily_info(self.stock_list, self.start_date, self.end_date, "qfa_yoysales")
        # ----以下勿改动----
        ans_df = ans_df.loc[self.start_date: self.end_date]
        ans_df = Dtk.convert_df_index_type(ans_df, 'date_int', 'timestamp')
        return ans_df
