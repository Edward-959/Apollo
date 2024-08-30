# -*- coding: utf-8 -*-
"""
Created on 2019/3/21
@author: Xiu Zixing
来自民生证券研报《因子研究专题三——动量（反转）因子解析》
路径-趋势因子
"""
from Factor.DailyFactorBase import DailyFactorBase
import DataAPI.DataToolkit as Dtk
import math
import pandas as pd
import numpy as np


class NonFactorDailyPathTrend(DailyFactorBase):
    def __init__(self, alpha_factor_root_path, stock_list, start_date_int, end_date_int, params):
        super().__init__(alpha_factor_root_path)
        self.stock_list = stock_list
        self.start_date = start_date_int
        self.end_date = end_date_int
        self.rolling_window = params['n']


    def factor_calc(self):
        valid_start_date = Dtk.get_n_days_off(self.start_date, -(self.rolling_window + 2))[0]
        close_df = Dtk.get_panel_daily_pv_df(self.stock_list, valid_start_date, self.end_date, pv_type='close',
                                             adj_type="FORWARD")
        close_df_norm = (close_df - close_df.min())/(close_df.max() - close_df.min())
        regress_x = np.array(list(range(1, self.rolling_window + 1)))
        factor_dict = {}
        for column in list(close_df_norm.columns):
            close_column = list(close_df_norm[column])
            factor_dict.update({column: []})
            for i in range(len(close_column)):
                if i < self.rolling_window:
                    factor_dict[column].append(float('nan'))
                else:
                    regress_y = np.array(close_column[i - self.rolling_window + 1:i + 1])
                    factor_dict[column].append(float(np.cov(regress_y, regress_x)[0, 1]/np.var(regress_x)))
        factor_df = pd.DataFrame(factor_dict, index=close_df_norm.index, columns=close_df_norm.columns)
        ans_df = factor_df
        ans_df = ans_df.loc[self.start_date: self.end_date]
        ans_df = Dtk.convert_df_index_type(ans_df, 'date_int', 'timestamp')
        return ans_df
