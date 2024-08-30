# -*- coding: utf-8 -*-
"""
@author: 006688
created on 2019/02/22
Fama-French回归中的SMB因子
"""
from Factor.DailyFactorBase import DailyFactorBase
import DataAPI.DataToolkit as Dtk
import numpy as np
import pandas as pd


class NonFactorDailyFamaFrench(DailyFactorBase):
    # 这个因子没有参数，但也须在初始化时预留一个"params"
    def __init__(self, alpha_factor_root_path, stock_list, start_date_int, end_date_int, params):
        super().__init__(alpha_factor_root_path)
        self.stock_list = Dtk.get_complete_stock_list()
        self.start_date = start_date_int
        self.end_date = end_date_int

    def factor_calc(self):
        valid_start_date = Dtk.get_n_days_off(self.start_date, -2)[0]
        stock_close = Dtk.get_panel_daily_pv_df(self.stock_list, valid_start_date, self.end_date,
                                                pv_type='close', adj_type='FORWARD')
        stock_amt = Dtk.get_panel_daily_pv_df(self.stock_list, valid_start_date, self.end_date, pv_type='amt')
        stock_mkt_cap = Dtk.get_panel_daily_info(self.stock_list, valid_start_date, self.end_date, 'mkt_cap_ard')
        stock_pb = Dtk.get_panel_daily_info(self.stock_list, valid_start_date, self.end_date, 'pb_lf')
        stock_pct_chg = stock_close / stock_close.shift(1) - 1
        stock_mkt_cap_rank = stock_mkt_cap.shift(1)[stock_amt > 0].rank(axis=1)
        stock_num = (stock_amt > 0).sum(axis=1)
        stock_pb[stock_pb <= 0] = np.nan
        stock_pb_rank = stock_pb.shift(1)[stock_amt > 0].rank(axis=1)
        stock_num_pb = (stock_pb > 0)[stock_amt > 0].sum(axis=1)
        ans_df = pd.DataFrame()
        ans_df['SMB'] = stock_pct_chg[stock_mkt_cap_rank.apply(lambda x: x <= round(stock_num * 0.3))].mean(axis=1) - \
                 stock_pct_chg[stock_mkt_cap_rank.apply(lambda x: x > round(stock_num * 0.7))].mean(axis=1)
        ans_df['HML'] = stock_pct_chg[stock_pb_rank.apply(lambda x: x > round(stock_num_pb * 0.7))].mean(axis=1) - \
                 stock_pct_chg[stock_pb_rank.apply(lambda x: x <= round(stock_num_pb * 0.3))].mean(axis=1)
        # ----以下勿改动----
        ans_df = ans_df.loc[self.start_date: self.end_date]
        ans_df = Dtk.convert_df_index_type(ans_df, 'date_int', 'timestamp')
        return ans_df
