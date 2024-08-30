#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019/2/11 14:49
# @Author  : 011673
import DataAPI.DataToolkit as Dtk
from Factor.DailyFactorBase import DailyFactorBase
import pandas as pd
import numpy as np
import copy


def get_residual(data: pd.DataFrame, mtk: pd.DataFrame):
    result = pd.DataFrame(np.nan, index=data.index, columns=data.columns)
    data = data.dropna(axis=0, how='all')
    for date in data.index:
        result.loc[date] = get_residual_number(mtk.loc[date], data.loc[date])
    result.replace([np.inf, -np.inf], np.nan)
    return result


def get_residual_number(x: pd.Series, y: pd.Series):
    data_1 = copy.deepcopy(x)
    data_2 = copy.deepcopy(y)
    data_1 = data_1.values
    data_2 = data_2.values
    valid_data_position = (~np.isnan(data_1)) & (~np.isinf(data_1)) & (~np.isnan(data_2)) & (~np.isinf(data_2))
    data_1 = data_1[valid_data_position]
    data_2 = data_2[valid_data_position]
    linear = np.polyfit(data_1, data_2, 1)
    del data_1, data_2
    return y - (linear[0] * x + linear[1])



class FactorDailyExceedSwingCorAmt(DailyFactorBase):
    def __init__(self, alpha_factor_root_path, stock_list, start_date_int, end_date_int, params):
        super().__init__(alpha_factor_root_path)
        self.stock_list = stock_list
        self.start_date = start_date_int
        self.end_date = end_date_int
        self.n = params["n"]

    def factor_calc(self):
        start_date_minus_lag_2 = Dtk.get_n_days_off(self.start_date, -(self.n + 2))[0]
        stock_high: pd.DataFrame = Dtk.get_panel_daily_pv_df(self.stock_list, start_date_minus_lag_2, self.end_date,
                                                             pv_type='high', adj_type='FORWARD')
        stock_low: pd.DataFrame = Dtk.get_panel_daily_pv_df(self.stock_list, start_date_minus_lag_2, self.end_date,
                                                            pv_type='low', adj_type='FORWARD')
        index_high: pd.DataFrame = Dtk.get_panel_daily_pv_df(['000905.SH'], start_date_minus_lag_2, self.end_date,
                                                             pv_type='high')
        index_low: pd.DataFrame = Dtk.get_panel_daily_pv_df(['000905.SH'], start_date_minus_lag_2, self.end_date,
                                                            pv_type='low')
        stock_amt: pd.DataFrame = Dtk.get_panel_daily_pv_df(self.stock_list, start_date_minus_lag_2, self.end_date,
                                                            pv_type='amt', adj_type='FORWARD')
        mkt_cap_ard_df = Dtk.get_panel_daily_info(self.stock_list, start_date_minus_lag_2, self.end_date, 'mkt_cap_ard')
        mkt_cap_ard_df = np.log(mkt_cap_ard_df)
        stock_swing: pd.DataFrame = 2 * (stock_high - stock_low) / (stock_high + stock_low)
        index_swing: pd.DataFrame = 2 * (index_high - index_low) / (index_high + index_low)
        exceed_swing: pd.DataFrame = (stock_swing.T - index_swing.iloc[:, 0]).T
        factor_data = exceed_swing.rolling(window=self.n).corr(stock_amt)
        factor_data = get_residual(factor_data, mkt_cap_ard_df)
        factor_original_df = factor_data.loc[self.start_date: self.end_date].copy()
        ans_df = factor_original_df
        ans_df = Dtk.convert_df_index_type(ans_df, 'date_int', 'timestamp')
        return ans_df
