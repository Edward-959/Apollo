# -*- coding: utf-8 -*-
"""
@author: 006566
Created on 2019/03/05
Style: Value
Definition: 1.0 · BTOP
BTOP: Book-to-price ratio
Revised on 2019/3/6: replace all the 'inf' values by 'nan'
"""
from Factor.DailyFactorBase import DailyFactorBase
import DataAPI.DataToolkit as Dtk
import numpy as np


class FactorBarraValue(DailyFactorBase):
    # 这个因子没有参数，但也须在初始化时预留一个"params"
    def __init__(self, alpha_factor_root_path, stock_list, start_date_int, end_date_int, params):
        super().__init__(alpha_factor_root_path)
        self.stock_list = stock_list
        self.start_date = start_date_int
        self.end_date = end_date_int

    def factor_calc(self):
        bp = Dtk.get_panel_daily_info(self.stock_list, self.start_date, self.end_date, 'pb_lf')
        ans_df = np.reciprocal(bp)
        ans_df = ans_df.replace(np.inf, np.nan)
        # ----以下勿改动----
        ans_df = ans_df.loc[self.start_date: self.end_date]
        ans_df = Dtk.convert_df_index_type(ans_df, 'date_int', 'timestamp')
        return ans_df
