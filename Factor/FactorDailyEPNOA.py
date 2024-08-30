# -*- coding: utf-8 -*-
"""
Created on 2019/4/4
@author: Xiu Zixing
根据国盛证券研报《对价值因子的思考和改进》
将EP的分子替换为营业收入-生产成本，分母替换为NOA Market Value
"""
import DataAPI.DataToolkit as Dtk
from Factor.DailyFactorBase import DailyFactorBase
import os
import pandas as pd
import numpy as np


class FactorDailyEPAdjusted(DailyFactorBase):
    # 这个因子没有参数，但也须在初始化时预留一个"params"
    def __init__(self, alpha_factor_root_path, stock_list, start_date_int, end_date_int, params):
        super().__init__(alpha_factor_root_path)
        self.stock_list = stock_list
        self.start_date = start_date_int
        self.end_date = end_date_int

    def factor_calc(self):
        corresponding_non_factor_file = "NF_D_MarketValueAdjusted2.h5"
        corresponding_non_factor_path = os.path.join(self.alpha_factor_root_path, "AlphaNonFactors",
                                                     corresponding_non_factor_file)
        market_value_adjusted = self.get_non_factor_df(corresponding_non_factor_path)
        market_value_adjusted = Dtk.convert_df_index_type(market_value_adjusted, 'timestamp', 'date_int')
        alt2 = 'AShareIncome'
        # 以下计算ttm的收入数据
        oper_rev = Dtk.get_daily_wind_quarterly_data(self.stock_list, alt2, 'oper_rev', self.start_date,
                                                     self.end_date, data_type='ttm')
        less_oper_cost = Dtk.get_daily_wind_quarterly_data(self.stock_list, alt2, 'less_oper_cost', self.start_date,
                                                           self.end_date, data_type='ttm')
        ans_df = (oper_rev - less_oper_cost)/(market_value_adjusted*10000)
        ans_df = ans_df.loc[self.start_date: self.end_date]
        ans_df = Dtk.convert_df_index_type(ans_df, 'date_int', 'timestamp')
        ans_df = pd.DataFrame(ans_df, dtype=np.float)
        return ans_df
