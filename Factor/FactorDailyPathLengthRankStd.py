# -*- coding: utf-8 -*-
"""
Created on 2019/3/25
@author: Xiu Zixing
来自民生证券研报《因子研究专题三——动量（反转）因子解析》
路径-长度因子，并在横截面上求排名，时间序列上求标准差
"""
import DataAPI.DataToolkit as Dtk
from Factor.DailyFactorBase import DailyFactorBase
import os


class FactorDailyPathLengthRankStd(DailyFactorBase):
    def __init__(self, alpha_factor_root_path, stock_list, start_date_int, end_date_int, params):
        super().__init__(alpha_factor_root_path)
        self.stock_list = stock_list
        self.start_date = start_date_int
        self.end_date = end_date_int
        self.n = params["n"]

    def factor_calc(self):
        # ---- 1. 请设定对应的因子原始值文件-------
        corresponding_non_factor_file = "NF_D_PathLength" + str(self.n) + ".h5"
        corresponding_non_factor_path = os.path.join(self.alpha_factor_root_path, "AlphaNonFactors",
                                                     corresponding_non_factor_file)
        factor_original_df = self.get_non_factor_df(corresponding_non_factor_path)
        factor_original_df = Dtk.convert_df_index_type(factor_original_df, 'timestamp', 'date_int')
        # ---- 2. 对因子原始值求Rank ----------------
        factor_rank = factor_original_df.rank(axis=1)
        ans_df = factor_rank.rolling(self.n).std()
        ans_df = ans_df.loc[self.start_date: self.end_date]
        ans_df = Dtk.convert_df_index_type(ans_df, 'date_int', 'timestamp')
        return ans_df
