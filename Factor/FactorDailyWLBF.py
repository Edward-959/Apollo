# -*- coding: utf-8 -*-
"""
@author: 006566
revised on 2019/02/22
Daily Winner Factor + Daily Loser Factor  = Winners Losers Both Factor
list_range 涨幅榜单范围
half_life 半衰期
[decay_alpha = 1 - exp(ln(0.5) / 10); rolling_window 回望时长 = half_life * 2]

"""
from Factor.DailyFactorBase import DailyFactorBase
import DataAPI.DataToolkit as Dtk
import math
import pandas as pd
import numpy as np


class FactorDailyWLBF(DailyFactorBase):
    # 这个因子没有参数，但也须在初始化时预留一个"params"
    def __init__(self, alpha_factor_root_path, stock_list, start_date_int, end_date_int, params):
        super().__init__(alpha_factor_root_path)
        self.stock_list = stock_list
        self.start_date = start_date_int
        self.end_date = end_date_int
        self.half_life = params['half_life']
        self.rolling_window_x = params['rolling_window_x']
        self.list_range = params['list_range']

    def factor_calc(self):
        rolling_window = self.half_life * self.rolling_window_x
        decay_alpha = 1 - math.exp(math.log(0.5) / self.half_life)
        valid_start_date = Dtk.get_n_days_off(self.start_date, -(rolling_window + 2))[0]
        close_df = Dtk.get_panel_daily_pv_df(self.stock_list, valid_start_date, self.end_date, pv_type='close')
        pre_close_df = Dtk.get_panel_daily_pv_df(self.stock_list, valid_start_date, self.end_date, pv_type='pre_close')
        pct_chg_df = close_df / pre_close_df - 1
        pct_chg_df_rank = pct_chg_df.rank(axis=1, ascending=False)
        winner_list_df: pd.DataFrame = pct_chg_df_rank <= self.list_range
        winner_list_df2 = winner_list_df.replace({True: 1.0, False: 0.0})
        loser_list_df: pd.DataFrame = pct_chg_df_rank <= self.list_range
        loser_list_df2 = loser_list_df.replace({True: 1.0, False: 0.0})
        decay_param_df_list = []
        for i in range(rolling_window):
            temp_decay_df = pd.DataFrame(np.tile(np.power((1 - decay_alpha), i), pct_chg_df.shape),
                                         index=pct_chg_df.index, columns=pct_chg_df.columns)
            decay_param_df_list.append(temp_decay_df)
        ans_df = pd.DataFrame()
        for i in range(rolling_window):
            if i > 0:
                ans_df = ans_df + decay_param_df_list[i] * winner_list_df2.shift(i) + decay_param_df_list[
                                                                                          i] * loser_list_df2.shift(i)
            else:
                ans_df = decay_param_df_list[i] * winner_list_df2 + decay_param_df_list[i] * loser_list_df2.shift(i)
        # 因数据严重右偏，故取根号
        ans_df = np.power(ans_df, 0.5)
        # ----以下勿改动----
        ans_df = ans_df.loc[self.start_date: self.end_date]
        ans_df = Dtk.convert_df_index_type(ans_df, 'date_int', 'timestamp')
        return ans_df
