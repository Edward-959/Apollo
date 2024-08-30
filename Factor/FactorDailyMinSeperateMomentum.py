# -*- coding: utf-8 -*-
# @Time    : 2018/12/4 11:36
# @Author  : 011673
# @File    : FactorDailyMinSeperateMomentum.py
# 收益率拆分：
# 隔夜时段收益：R0 = P 今开/P 昨收 - 1
# 第 1 小时收益: R1 = P10:30/P09:30 - 1
# 第 2 小时收益：R2 = P11:30/P10:30 - 1
# 第 3 小时收益：R3 = P14:00/P13:00 - 1
# 第 4 小时收益：R4 = P15:00/P14:00 - 1
# 非等权系数选取:[-0.5,-0.5,0.75,1.5,1]
import DataAPI.DataToolkit as Dtk
from Factor.DailyFactorBase import DailyFactorBase
import os


class FactorDailyMinSeperateMomentum(DailyFactorBase):
    def __init__(self, alpha_factor_root_path, stock_list, start_date_int, end_date_int, params):
        super().__init__(alpha_factor_root_path)
        self.stock_list = stock_list
        self.start_date = start_date_int
        self.end_date = end_date_int
        self.eq=params['eq']

    def factor_calc(self):
        # ---- 1. 请设定对应的因子原始值文件-------
        corresponding_non_factor_file = "NF_D_MinSeperateMomentum_ema{}.h5".format(self.eq)
        # ---- 第1部分到此为止，其他部分勿改动 ----
        corresponding_non_factor_path = os.path.join(self.alpha_factor_root_path, "AlphaNonFactors",
                                                     corresponding_non_factor_file)
        factor_original_df = self.get_non_factor_df(corresponding_non_factor_path)
        factor_original_df = Dtk.convert_df_index_type(factor_original_df, 'timestamp', 'date_int')
        # ---- 2. 对因子原始值求ema ----------------
        ans_df =  factor_original_df.ewm(com=10).mean()
        # ---- 第2部分到此为止，以下勿改动----------
        ans_df = ans_df.loc[self.start_date: self.end_date]
        ans_df = Dtk.convert_df_index_type(ans_df, 'date_int', 'timestamp')
        return ans_df