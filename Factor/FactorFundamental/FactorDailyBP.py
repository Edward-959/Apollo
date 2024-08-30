# -*- coding: utf-8 -*-
"""
@author: 006566, 2018/11/22
revised on 2019/2/27
个股的pb_lf的倒数
注意有另一种算法，是total_equity / market_cap, 这里未采纳；目前用的数据来源（真实来源）是wind-api
如后续改为用落地数据库的话，pb_lf则没有负数
"""

import DataAPI.DataToolkit as Dtk
from Factor.DailyFactorBase import DailyFactorBase
import numpy as np


class FactorDailyBP(DailyFactorBase):
    # 这个因子没有参数，但也须在初始化时预留一个"params"
    def __init__(self, alpha_factor_root_path, stock_list, start_date_int, end_date_int, params):
        super().__init__(alpha_factor_root_path)
        self.stock_list = stock_list
        self.start_date = start_date_int
        self.end_date = end_date_int

    def factor_calc(self):
        bp = Dtk.get_panel_daily_info(self.stock_list, self.start_date, self.end_date, 'pb_lf')
        ans_df = np.reciprocal(bp)
        # ----以下勿改动----
        ans_df = ans_df.loc[self.start_date: self.end_date]
        ans_df = Dtk.convert_df_index_type(ans_df, 'date_int', 'timestamp')
        return ans_df
