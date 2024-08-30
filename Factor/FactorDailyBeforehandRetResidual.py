# -*- coding: utf-8 -*-
# @Time    : 2018/12/10 9:59
# @Author  : 011673
# @File    : FactorDailyBeforehandRet.py
import DataAPI.DataToolkit as Dtk
from Factor.DailyFactorBase import DailyFactorBase
import pandas as pd
import numpy as np


def get_residual(df: pd.DataFrame):
    result = df.rolling(window=len(df.index) - 1, min_periods=20).apply(get_residual_number)
    return result


def get_residual_number(data_1):
    data = data_1[~np.isnan(data_1)]
    data = data[~np.isinf(data)]
    data = data[data != 0]
    if len(data) < 10:
        return np.nan
    else:
        x = data[1:]
        y = data[:-1]
        linear = np.polyfit(x, y, 1)
        return y[-1] - (linear[0] * x[-1] + linear[1])


class FactorDailyBeforehandRetResidual(DailyFactorBase):
    def __init__(self, alpha_factor_root_path, stock_list, start_date_int, end_date_int, params):
        super().__init__(alpha_factor_root_path)
        self.stock_list = stock_list
        self.start_date = start_date_int
        self.end_date = end_date_int
        self.n = params["n"]

    def factor_calc(self):
        # ---- 1. 请设定对应的因子原始值文件-------
        # 获取close的后复权数据，是DataFrame，每一列的列名是股票代码，每一行的标签则是日期（例如20180829，是8位数的int）
        # 获取close的后复权数据，是DataFrame，每一列的列名是股票代码，每一行的标签则是日期（例如20180829，是8位数的int）
        # 获取close的后复权数据，是DataFrame，每一列的列名是股票代码，每一行的标签则是日期（例如20180829，是8位数的int）
        start_date_minus_lag_2 = Dtk.get_n_days_off(self.start_date, -(self.n + 20))[0]
        # 获取close的后复权数据，是DataFrame，每一列的列名是股票代码，每一行的标签则是日期（例如20180829，是8位数的int）
        close: pd.DataFrame = Dtk.get_panel_daily_pv_df(self.stock_list, start_date_minus_lag_2, self.end_date,
                                                        pv_type='close', adj_type='FORWARD')
        turn: pd.DataFrame = Dtk.get_panel_daily_info(self.stock_list, start_date_minus_lag_2, self.end_date, info_type='turn')
        # 计算因子值
        ret: pd.DataFrame = (close - close.shift(1)) / close.shift(1)
        turn_residual = get_residual(turn)
        ret = ret.fillna(0)
        turn_residual.fillna(0)
        factor_data = ret.rolling(self.n).corr(turn_residual)
        factor_data = factor_data * turn / turn
        # 保留start_date至end_date（前闭后闭）期间的数据
        factor_original_df = factor_data.loc[self.start_date: self.end_date].copy()
        # ---- 2. 对因子原始值求ema ----------------
        ans_df = factor_original_df
        # ---- 第2部分到此为止，以下勿改动----------
        ans_df = ans_df.loc[self.start_date: self.end_date]
        ans_df = Dtk.convert_df_index_type(ans_df, 'date_int', 'timestamp')
        return ans_df
