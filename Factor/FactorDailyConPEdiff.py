# -*- coding: utf-8 -*-
"""
created on 2018/12/6
revised on 2019/2/27
@author: 006566
对一致预期PE取n天前的差分；这是对海通证券研报中con_PE_rel的改进，原因子是con_PE的环比增长率，但我认为环比增长率若有穿
越0的问题时、会导致结果的不可比，不如直接做差比较
"""

import DataAPI.DataToolkit as Dtk
from Factor.DailyFactorBase import DailyFactorBase
import numpy as np
import pandas as pd


class FactorDailyConPEdiff(DailyFactorBase):
    # 这个因子没有参数，但也须在初始化时预留一个"params"
    def __init__(self, alpha_factor_root_path, stock_list, start_date_int, end_date_int, params):
        super().__init__(alpha_factor_root_path)
        self.stock_list = stock_list
        self.start_date = start_date_int
        self.end_date = end_date_int
        self.t_days = params["t_days"]
        self.max_contype = params["max_contype"]

    def factor_calc(self):
        valid_start_date = Dtk.get_n_days_off(self.start_date, -(self.t_days + 2))[0]
        con_pe_df: pd.DataFrame = Dtk.read_h5_gogoal_data("FCD_CHINA_STOCK_DAILY_SUNTIME", "C4",
                                                          valid_start_date, self.end_date,
                                                          self.stock_list, self.max_contype)
        con_pe_df = con_pe_df.replace(0, np.nan)
        con_pe_rel = con_pe_df - con_pe_df.shift(self.t_days)
        ans_df = con_pe_rel
        # ----以下勿改动----
        ans_df = ans_df.loc[self.start_date: self.end_date]
        ans_df = Dtk.convert_df_index_type(ans_df, 'date_int', 'timestamp')
        return ans_df
