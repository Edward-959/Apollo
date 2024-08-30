# -*- coding: utf-8 -*-
"""
@author: 006688
Created on 2019/02/19
Style: Liquidity
Definition: 0.35 · STOM + 0.35 · STOQ + 0.30 · STOA
STOM: Share turnover, one month
STOQ: Average share turnover, trailing 3 months
STOA: Average share turnover, trailing 12 months
"""
from Factor.DailyFactorBase import DailyFactorBase
import DataAPI.DataToolkit as Dtk
import numpy as np


class FactorBarraLiquidity(DailyFactorBase):
    # 这个因子没有参数，但也须在初始化时预留一个"params"
    def __init__(self, alpha_factor_root_path, stock_list, start_date_int, end_date_int, params):
        super().__init__(alpha_factor_root_path)
        self.stock_list = stock_list
        self.start_date = start_date_int
        self.end_date = end_date_int
        self.trail = 252

    def factor_calc(self):
        valid_start_date = Dtk.get_n_days_off(self.start_date, -self.trail-2)[0]
        stock_turn = Dtk.get_panel_daily_info(self.stock_list, valid_start_date, self.end_date, 'turn')
        stock_turn_m = stock_turn.rolling(21).sum()
        stock_turn_q = stock_turn.rolling(21 * 3).sum()
        stock_turn_a = stock_turn.rolling(21 * 12, min_periods=126).sum()
        stom = stock_turn_m.clip_lower(0).replace(0, np.nan)
        stoq = (stock_turn_q / 3).clip_lower(0).replace(0, np.nan)
        stoa = (stock_turn_a / 12).clip_lower(0).replace(0, np.nan)
        ans_df = 0.35 * np.log(stom) + 0.35 * np.log(stoq) + 0.3 * np.log(stoa)
        # ----以下勿改动----
        ans_df = ans_df.loc[self.start_date: self.end_date]
        ans_df = Dtk.convert_df_index_type(ans_df, 'date_int', 'timestamp')
        return ans_df
