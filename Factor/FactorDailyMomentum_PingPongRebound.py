# -*- coding: utf-8 -*-
# @Time    : 2018/11/19 14:32
# @Author  : 011673
# @File    : FactorDailyMomentum_20Ret.py
import DataAPI.DataToolkit as Dtk
from Factor.DailyFactorBase import DailyFactorBase
import pandas as pd


class FactorDailyMomentum_PingPongRebound(DailyFactorBase):
    def __init__(self, alpha_factor_root_path, stock_list, start_date_int, end_date_int, params):
        super().__init__(alpha_factor_root_path)
        self.stock_list = stock_list
        self.start_date = start_date_int
        self.end_date = end_date_int
        self.n = params["n"]

    def factor_calc(self):
        # ---- 1. 请设定对应的因子原始值文件-------
        # 获取close的后复权数据，是DataFrame，每一列的列名是股票代码，每一行的标签则是日期（例如20180829，是8位数的int）
        start_date_minus_k_2 = Dtk.get_n_days_off(self.start_date, -(self.n + 2))[0]
        # 获取close的后复权数据，是DataFrame，每一列的列名是股票代码，每一行的标签则是日期（例如20180829，是8位数的int）
        close: pd.DataFrame = Dtk.get_panel_daily_pv_df(self.stock_list, start_date_minus_k_2, self.end_date,
                                                        pv_type='close', adj_type='FORWARD')

        factor_data = close / close.rolling(window=self.n).mean() - 1

        # 保留start_date至end_date（前闭后闭）期间的数据
        factor_original_df = factor_data.loc[self.start_date: self.end_date].copy()
        # ---- 2. 对因子原始值求ema ----------------
        ans_df = factor_original_df
        # ---- 第2部分到此为止，以下勿改动----------
        ans_df = ans_df.loc[self.start_date: self.end_date]
        ans_df = Dtk.convert_df_index_type(ans_df, 'date_int', 'timestamp')
        return ans_df