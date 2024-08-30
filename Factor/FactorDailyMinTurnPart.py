# -*- coding: utf-8 -*-
"""
@author: 006688
Created on 2018/11/29
revised on 2019/02/25
局部流动性因子：i取值0，1，2，3，4，分别表示隔夜、日内第1~4个小时的换手率，
将过去n日对应时段换手率求和取对数(该日内因子的计算到此为止，剩下部分在FactorDailyTurnPartPure中计算)，
并参考日级别因子TurnPure的计算方法，在横截面上关于对数流通市值回归取残差，再将残差关于TurnPure回归取残差作为因子值
"""
from Factor.DailyFactorBase import DailyFactorBase
import DataAPI.DataToolkit as Dtk
import os
import numpy as np


class FactorDailyMinTurnPart(DailyFactorBase):
    def __init__(self, alpha_factor_root_path, stock_list, start_date_int, end_date_int, params):
        super().__init__(alpha_factor_root_path)
        self.stock_list = stock_list
        self.start_date = start_date_int
        self.end_date = end_date_int
        self.period = params["period"]
        self.n = params["rolling"]

    def factor_calc(self):
        # ---- 1. 请设定对应的因子原始值文件-------
        corresponding_non_factor_file = "NF_D_MinTurnPart_" + str(self.period) + ".h5"
        # ---- 第1部分到此为止，其他部分勿改动 ----
        corresponding_non_factor_path = os.path.join(self.alpha_factor_root_path, "AlphaNonFactors",
                                                     corresponding_non_factor_file)
        factor_original_df = self.get_non_factor_df(corresponding_non_factor_path)
        factor_original_df = Dtk.convert_df_index_type(factor_original_df, 'timestamp', 'date_int')
        # ---- 2. 对因子原始值求ema或rolling------------
        ans_df = np.log(factor_original_df.rolling(self.n).sum().clip_lower(0).replace(0, np.nan))
        # ---- 第2部分到此为止，以下勿改动----------
        ans_df = ans_df.loc[self.start_date: self.end_date]
        ans_df = Dtk.convert_df_index_type(ans_df, 'date_int', 'timestamp')
        return ans_df
