"""
Created by 006566 on 2019/3/7
"""

import DataAPI.DataToolkit as Dtk
from Factor.DailyFactorBase import DailyFactorBase
import os


class FactorDailyCloseVSVwap(DailyFactorBase):
    def __init__(self, alpha_factor_root_path, stock_list, start_date_int, end_date_int, params):
        super().__init__(alpha_factor_root_path)
        self.stock_list = stock_list
        self.start_date = start_date_int
        self.end_date = end_date_int
        self.ewm_span = params["ewm_span"]

    def factor_calc(self):
        # ---- 1. 请设定对应的因子原始值文件-------
        corresponding_non_factor_file = "NF_D_CloseVSVwap.h5"
        # ---- 第1部分到此为止，其他部分勿改动 ----
        corresponding_non_factor_path = os.path.join(self.alpha_factor_root_path, "AlphaNonFactors",
                                                     corresponding_non_factor_file)
        factor_original_df = self.get_non_factor_df(corresponding_non_factor_path)
        factor_original_df = Dtk.convert_df_index_type(factor_original_df, 'timestamp', 'date_int')
        # ---- 2. 对因子原始值求ema ----------------
        ans_df = factor_original_df.rolling(self.ewm_span).mean()
        # ---- 第2部分到此为止，以下勿改动----------
        ans_df = ans_df.loc[self.start_date: self.end_date]
        ans_df = Dtk.convert_df_index_type(ans_df, 'date_int', 'timestamp')
        return ans_df
