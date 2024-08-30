# -*- coding: utf-8 -*-
"""
@author: 006566, 2018/11/23
revised on 2019/2/27
财务质量因子：当季ROE；数据的最终来源是Wind终端的qfa_roe
这个指标原本是季频的，被backfill成日频的；计算原理是【净利润*2／(期初净资产+期末净资产)】
经仔细比对，净利润是“利润表”中“归属母公司股东的净利润”（W30028333）；
净资产是“资产负债表”中“归属母公司股东的权益（W34656681）”；“期初”指上季末，“期末”指本季末
"""

import DataAPI.DataToolkit as Dtk
from Factor.DailyFactorBase import DailyFactorBase


class FactorDailyQfaROE(DailyFactorBase):
    # 这个因子没有参数，但也须在初始化时预留一个"params"
    def __init__(self, alpha_factor_root_path, stock_list, start_date_int, end_date_int, params):
        super().__init__(alpha_factor_root_path)
        self.stock_list = stock_list
        self.start_date = start_date_int
        self.end_date = end_date_int

    def factor_calc(self):
        ans_df = Dtk.get_panel_daily_info(self.stock_list, self.start_date, self.end_date, "qfa_roe")
        # ----以下勿改动----
        ans_df = ans_df.loc[self.start_date: self.end_date]
        ans_df = Dtk.convert_df_index_type(ans_df, 'date_int', 'timestamp')
        return ans_df
