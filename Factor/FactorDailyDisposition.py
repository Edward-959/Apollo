# -*- coding: utf-8 -*-
"""
@author: 006688
created on 2018/12/06
revised on 2019/02/21
用处理后的换手率对过去n日收盘价加权平均，求当日收盘价与其差距
"""
from Factor.DailyFactorBase import DailyFactorBase
import DataAPI.DataToolkit as Dtk
import numpy as np


class FactorDailyDisposition(DailyFactorBase):
    # 这个因子没有参数，但也须在初始化时预留一个"params"
    def __init__(self, alpha_factor_root_path, stock_list, start_date_int, end_date_int, params):
        super().__init__(alpha_factor_root_path)
        self.stock_list = stock_list
        self.start_date = start_date_int
        self.end_date = end_date_int
        self.n = params['n']

    def factor_calc(self):
        valid_start_date = Dtk.get_n_days_off(self.start_date, -self.n-2)[0]
        data_close = Dtk.get_panel_daily_pv_df(self.stock_list, valid_start_date, self.end_date,
                                               pv_type='close', adj_type='FORWARD')
        data_turn = Dtk.get_panel_daily_info(self.stock_list, valid_start_date, self.end_date, info_type='turn')
        data_turn = data_turn.div(100)
        trading_days = Dtk.get_trading_day(self.start_date, self.end_date)
        rp = data_close.copy()
        rp[:] = np.nan
        for date in trading_days:
            i = data_close.index.tolist().index(date)
            data_close_i = data_close.iloc[i - self.n + 1: i + 1]
            data_turn_i = data_turn.iloc[i - self.n + 1: i + 1]
            temp = (1 - data_turn_i).sort_index(ascending=False).cumprod().shift(1).fillna(1)
            turn_weight_i = temp * data_turn_i
            rp.loc[date] = (data_close_i * turn_weight_i).sum() / turn_weight_i.sum()
        rp = rp * data_turn / data_turn
        ans_df = data_close / rp - 1
        # ----以下勿改动----
        ans_df = ans_df.loc[self.start_date: self.end_date]
        ans_df = Dtk.convert_df_index_type(ans_df, 'date_int', 'timestamp')
        return ans_df
