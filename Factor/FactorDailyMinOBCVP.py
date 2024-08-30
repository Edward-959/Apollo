# -*- coding: utf-8 -*-
"""
@author: 006566
revised on 2019/02/22
OCVP stands for opening call auction volume percent
"""

from Factor.DailyFactorBase import DailyFactorBase
import DataAPI.DataToolkit as Dtk
import os


class FactorDailyMinOBCVP(DailyFactorBase):
    def __init__(self, alpha_factor_root_path, stock_list, start_date_int, end_date_int, params):
        super().__init__(alpha_factor_root_path)
        self.stock_list = stock_list
        self.start_date = start_date_int
        self.end_date = end_date_int
        self.ema_span = params["ema_span"]

    def factor_calc(self):
        corresponding_non_factor_file1 = "NF_D_MinOCVP.h5"
        corresponding_non_factor_path1 = os.path.join(self.alpha_factor_root_path, "AlphaNonFactors",
                                                      corresponding_non_factor_file1)
        factor_original_df1 = self.get_non_factor_df(corresponding_non_factor_path1)
        factor_original_df1 = Dtk.convert_df_index_type(factor_original_df1, 'timestamp', 'date_int')

        corresponding_non_factor_file2 = "NF_D_MinBCVP.h5"
        corresponding_non_factor_path2 = os.path.join(self.alpha_factor_root_path, "AlphaNonFactors",
                                                      corresponding_non_factor_file2)
        factor_original_df2 = self.get_non_factor_df(corresponding_non_factor_path2)
        factor_original_df2 = Dtk.convert_df_index_type(factor_original_df2, 'timestamp', 'date_int')
        raw = factor_original_df1.mul(0.5) + factor_original_df2.mul(0.5)
        ans_df = raw.ewm(span=self.ema_span).mean()
        # ---- 第2部分到此为止，以下勿改动----------
        ans_df = ans_df.loc[self.start_date: self.end_date]
        ans_df = Dtk.convert_df_index_type(ans_df, 'date_int', 'timestamp')
        return ans_df
