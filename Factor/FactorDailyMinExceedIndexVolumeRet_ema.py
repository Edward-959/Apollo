#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019/1/30 10:38
# @Author  : 011673
from Factor.DailyFactorBase import DailyFactorBase
import DataAPI.DataToolkit as Dtk
import os


class FactorDailyMinExceedIndexVolumeRet_ema(DailyFactorBase):
    def __init__(self, alpha_factor_root_path, stock_list, start_date_int, end_date_int, params):
        super().__init__(alpha_factor_root_path)
        self.stock_list = stock_list
        self.start_date = start_date_int
        self.end_date = end_date_int

    def factor_calc(self):
        # ---- 1. 请设定对应的因子原始值文件-------
        corresponding_non_factor_file = "NF_D_MinExceedIndexVolumeRet_ema15.h5"
        # ---- 第1部分到此为止，其他部分勿改动 ----
        corresponding_non_factor_path = os.path.join(self.alpha_factor_root_path, "AlphaNonFactors",
                                                     corresponding_non_factor_file)
        factor_original_df = self.get_non_factor_df(corresponding_non_factor_path)
        factor_original_df = Dtk.convert_df_index_type(factor_original_df, 'timestamp', 'date_int')
        # ---- 2. 对因子原始值求ewm或rolling--------
        ans_df = factor_original_df.ewm(com=15).mean()
        # ---- 第2部分到此为止，以下勿改动----------
        ans_df = ans_df.loc[self.start_date: self.end_date]
        ans_df = Dtk.convert_df_index_type(ans_df, 'date_int', 'timestamp')
        return ans_df
