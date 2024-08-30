# -*- coding: utf-8 -*-
# @Time    : 2018/12/10 9:59
# @Author  : 011673
# @File    : FactorDailyBeforehandRet.py
import DataAPI.DataToolkit as Dtk
from Factor.DailyFactorBase import DailyFactorBase
import pandas as pd


class FactorDailyBeforehandRet(DailyFactorBase):
    def __init__(self, alpha_factor_root_path, stock_list, start_date_int, end_date_int, params):
        super().__init__(alpha_factor_root_path)
        self.stock_list = stock_list
        self.start_date = start_date_int
        self.end_date = end_date_int
        self.n = params["n"]

    def factor_calc(self):
        start_date_minus_lag_2 = Dtk.get_n_days_off(self.start_date, -(self.n + 2))[0]
        close: pd.DataFrame = Dtk.get_panel_daily_pv_df(self.stock_list, start_date_minus_lag_2, self.end_date,
                                                        pv_type='close', adj_type='FORWARD')
        turn: pd.DataFrame = Dtk.get_panel_daily_info(self.stock_list, start_date_minus_lag_2, self.end_date, info_type='turn')
        ret: pd.DataFrame = (close - close.shift(1)) / close.shift(1)
        last_turn = turn.shift(1)
        last_turn = last_turn.fillna(0)
        ret = ret.fillna(0)
        factor_data = ret.rolling(self.n).corr(last_turn)
        factor_data = factor_data * turn / turn
        factor_original_df = factor_data.loc[self.start_date: self.end_date].copy()
        ans_df = factor_original_df
        ans_df = ans_df.loc[self.start_date: self.end_date]
        ans_df = Dtk.convert_df_index_type(ans_df, 'date_int', 'timestamp')
        return ans_df
