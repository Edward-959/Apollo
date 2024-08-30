# -*- coding: utf-8 -*-
"""
created on 2018/12/6
revised on 2019/2/27
@author: 006566
ConEPChange, EP.diff(t)
"""

import DataAPI.DataToolkit as Dtk
from Factor.DailyFactorBase import DailyFactorBase
import numpy as np
import pandas as pd


class FactorDailyConEPChg(DailyFactorBase):
    # 这个因子没有参数，但也须在初始化时预留一个"params"
    def __init__(self, alpha_factor_root_path, stock_list, start_date_int, end_date_int, params):
        super().__init__(alpha_factor_root_path)
        self.stock_list = stock_list
        self.start_date = start_date_int
        self.end_date = end_date_int
        self.t_days = params["t_days"]
        self.max_contype = params["max_contype"]

    def factor_calc(self):
        valid_start_date = Dtk.get_n_days_off(self.start_date, -1 * (self.t_days + 2))[0]
        con_pe_df: pd.DataFrame = Dtk.read_h5_gogoal_data("FCD_CHINA_STOCK_DAILY_SUNTIME", "C4",
                                                          valid_start_date, self.end_date,
                                                          self.stock_list, self.max_contype)
        con_ep_df = np.reciprocal(con_pe_df)
        con_ep_df = con_ep_df.replace(np.inf, np.nan)
        con_ep_df_diff = con_ep_df.diff(self.t_days)
        ans_df = con_ep_df_diff
        # ----以下勿改动----
        ans_df = ans_df.loc[self.start_date: self.end_date]
        ans_df = Dtk.convert_df_index_type(ans_df, 'date_int', 'timestamp')
        return ans_df