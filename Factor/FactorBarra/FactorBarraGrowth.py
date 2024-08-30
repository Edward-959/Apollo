# -*- coding: utf-8 -*-
"""
@author: 006566
Created on 2019/03/05
Style: Growth
Definition: 0.5 * ProfitGq + 0.5 * SalesGq
ProfitGq: 单季净利润同比增长率
SalesGq: 单季营收同比增长率

Revised on 2019/4/12: xquant提供的h5因子，qfa_yoysales和qfa_yoyprofit出现了修改，原来应该是nan的改为了0；
           因此将ans_df * mkt / mkt，以便将未上市公司的0设为nan
"""
from Factor.DailyFactorBase import DailyFactorBase
import DataAPI.DataToolkit as Dtk


class FactorBarraGrowth(DailyFactorBase):
    # 这个因子没有参数，但也须在初始化时预留一个"params"
    def __init__(self, alpha_factor_root_path, stock_list, start_date_int, end_date_int, params):
        super().__init__(alpha_factor_root_path)
        self.stock_list = stock_list
        self.start_date = start_date_int
        self.end_date = end_date_int

    def factor_calc(self):
        sales_g_q_df = Dtk.get_panel_daily_info(self.stock_list, self.start_date, self.end_date, "qfa_yoysales")
        profit_g_q_df = Dtk.get_panel_daily_info(self.stock_list, self.start_date, self.end_date, "qfa_yoyprofit")
        ans_df = sales_g_q_df.mul(0.5) + profit_g_q_df.mul(0.5)
        mkt_cap_df = Dtk.get_panel_daily_info(self.stock_list, self.start_date, self.end_date, 'mkt_cap_ard')
        ans_df = ans_df * mkt_cap_df / mkt_cap_df  # 将未上市公司的0过滤为nan
        # ----以下勿改动----
        ans_df = ans_df.loc[self.start_date: self.end_date]
        ans_df = Dtk.convert_df_index_type(ans_df, 'date_int', 'timestamp')
        return ans_df
