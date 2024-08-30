# -*- coding: utf-8 -*-
"""
@author: 006566
Created on 2019/03/05
Style: EarningsYield
Definition: 0.5 * CETOP + 0.5 * ETOP
CETOP: Cash earnings-to-price ratio, 1 / PCF_ttm
ETOP: Trailing earnings-to-price ratio, 1 / PE_ttm
Revised on 2019/3/6: replace all the 'inf' values by 'nan'
Revised on 2019/4/12: rewrote the factors
"""
from Factor.DailyFactorBase import DailyFactorBase
import DataAPI.DataToolkit as Dtk
import numpy as np


class FactorBarraEarningsYield(DailyFactorBase):
    # 这个因子没有参数，但也须在初始化时预留一个"params"
    def __init__(self, alpha_factor_root_path, stock_list, start_date_int, end_date_int, params):
        super().__init__(alpha_factor_root_path)
        self.stock_list = stock_list
        self.start_date = start_date_int
        self.end_date = end_date_int

    def factor_calc(self):
        profit = Dtk.get_daily_wind_quarterly_data(self.stock_list, 'AShareIncome', 'NET_PROFIT_AFTER_DED_NR_LP',
                                                   self.start_date, self.end_date, 'ttm')
        ocf = Dtk.get_daily_wind_quarterly_data(self.stock_list, 'AShareCashFlow', 'NET_CASH_FLOWS_OPER_ACT',
                                                self.start_date, self.end_date, 'ttm')
        mkt_cap = Dtk.get_panel_daily_info(self.stock_list, self.start_date, self.end_date, 'mkt_cap_ard')
        ep = profit / mkt_cap
        ocfp = ocf / mkt_cap
        ans_df = 0.5 * ep + 0.5 * ocfp
        ans_df = ans_df.replace(np.inf, np.nan)
        # ----以下勿改动----
        ans_df = ans_df.loc[self.start_date: self.end_date]
        ans_df = Dtk.convert_df_index_type(ans_df, 'date_int', 'timestamp')
        return ans_df
