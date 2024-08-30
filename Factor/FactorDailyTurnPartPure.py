# -*- coding: utf-8 -*-
"""
@author: 006688
created on 2018/11/29
revised on 2019/02/25
局部流动性因子：i取值0，1，2，3，4，分别表示隔夜、日内第1~4个小时的换手率，将过去n日对应时段换手率求和取对数，
(以上部分已在FactorDailyMinTurnPart中计算，故请先计算FactorDailyMinTurnPart和FactorDailyTurnPure，再计算该因子，且保持参数相同)
并参考日级别因子TurnPure的计算方法，在横截面上关于对数流通市值回归取残差，再将残差关于TurnPure回归取残差作为因子值
"""
from Factor.DailyFactorBase import DailyFactorBase
import DataAPI.DataToolkit as Dtk
import os
import numpy as np
import pandas as pd


class FactorDailyTurnPartPure(DailyFactorBase):
    def __init__(self, alpha_factor_root_path, stock_list, start_date_int, end_date_int, params):
        super().__init__(alpha_factor_root_path)
        self.stock_list = stock_list
        self.start_date = start_date_int
        self.end_date = end_date_int
        self.period = params["period"]
        self.n = params["rolling"]

    def factor_calc(self):
        stock_close = Dtk.get_panel_daily_pv_df(self.stock_list, self.start_date, self.end_date,
                                                pv_type='close', adj_type='NONE')
        stock_free_float_shares = Dtk.get_panel_daily_info(self.stock_list, self.start_date, self.end_date,
                                                           info_type='free_float_shares')
        # 读取已计算好的因子值
        liq_part_file = "F_D_MinTurnPart_" + str(self.period) + "_" + str(self.n) + ".h5"
        corresponding_non_factor_path = os.path.join(self.alpha_factor_root_path, "AlphaFactors",
                                                     liq_part_file)
        liquidity_part = self.get_non_factor_df(corresponding_non_factor_path)
        liquidity_part = Dtk.convert_df_index_type(liquidity_part, 'timestamp', 'date_int')
        liq_day_file = "F_D_TurnPure_" + str(self.n) + ".h5"
        corresponding_non_factor_path = os.path.join(self.alpha_factor_root_path, "AlphaFactors",
                                                     liq_day_file)
        liquidity_day = self.get_non_factor_df(corresponding_non_factor_path)
        liquidity_day = Dtk.convert_df_index_type(liquidity_day, 'timestamp', 'date_int')
        # 回归
        stock_free_float_mv = np.log((stock_free_float_shares * stock_close * 10000).clip_lower(0).replace(0, np.nan))
        trading_days = Dtk.get_trading_day(self.start_date, self.end_date)
        ans_df = stock_close.copy()
        ans_df[:] = np.nan
        for date in trading_days:
            reg_data = pd.DataFrame([])
            reg_data['float_mv'] = stock_free_float_mv.loc[date, :]
            reg_data['liq_part'] = liquidity_part.loc[date, :]
            reg_data['liq_day'] = liquidity_day.loc[date, :]
            reg_data.dropna(inplace=True)
            X1 = np.array(reg_data['float_mv'], ndmin=2)
            reg1 = np.linalg.inv(X1.dot(X1.T)).dot(X1).dot(np.array(reg_data['liq_part']))
            liquidity_part_res = reg_data['liq_part'] - X1.T.dot(reg1)
            X2 = np.array(reg_data['liq_day'], ndmin=2)
            reg2 = np.linalg.inv(X2.dot(X2.T)).dot(X2).dot(np.array(liquidity_part_res))
            ans_df.loc[date, :] = liquidity_part_res - X2.T.dot(reg2)
        # ---- 以下勿改动----------
        ans_df = ans_df.loc[self.start_date: self.end_date]
        ans_df = Dtk.convert_df_index_type(ans_df, 'date_int', 'timestamp')
        return ans_df
