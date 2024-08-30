# -*- coding: utf-8 -*-
"""
@author: 013542
created on 2019/03/14
中国版Fama-French回归中相关收益序列
"""
from Factor.DailyFactorBase import DailyFactorBase
import DataAPI.DataToolkit as Dtk
import numpy as np
import pandas as pd


class NonFactorDailyCHNFamaFrench(DailyFactorBase):
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
        stock_pe = Dtk.get_panel_daily_info(self.stock_list, valid_start_date, self.end_date, 'pe_ttm')
        stock_pct_chg = stock_close / stock_close.shift(1) - 1
        stock_mkt_cap_rank = stock_mkt_cap.shift(1)[stock_amt > 0].rank(axis=1)
        stock_num = (stock_amt > 0).sum(axis=1)
        stock_mkt_sign_S = stock_mkt_cap_rank.apply(lambda x: (x >= round(stock_num * 0.3)) * (x <round(stock_num * 0.65)))
        stock_mkt_sign_B = stock_mkt_cap_rank.apply(lambda x: x >= round(stock_num * 0.65))

        stock_EP = 1/stock_pe
        stock_EP[stock_EP <= 0] = np.nan
        stock_EP_rank = stock_EP.shift(1)[stock_amt > 0].rank(axis=1)
        stock_num_EP = (stock_EP > 0)[stock_amt > 0].sum(axis=1)
        stock_EP_Value = stock_EP_rank.apply(lambda x: x > round(stock_num_EP * 0.7))
        stock_EP_Middle = stock_EP_rank.apply(lambda x: (x > round(stock_num_EP * 0.3)) * (x <= round(stock_num_EP * 0.7)))
        stock_EP_Growth = stock_EP_rank.apply(lambda x: x < round(stock_num_EP * 0.3))

        ans_df = pd.DataFrame()
        ret_SV = (stock_pct_chg * stock_mkt_sign_S * stock_EP_Value).sum(axis=1)
        ret_SM = (stock_pct_chg * stock_mkt_sign_S * stock_EP_Middle).sum(axis=1)
        ret_SG = (stock_pct_chg * stock_mkt_sign_S * stock_EP_Growth).sum(axis=1)
        ret_BV = (stock_pct_chg * stock_mkt_sign_B * stock_EP_Value).sum(axis=1)
        ret_BM = (stock_pct_chg * stock_mkt_sign_B * stock_EP_Middle).sum(axis=1)
        ret_BG = (stock_pct_chg * stock_mkt_sign_B * stock_EP_Growth).sum(axis=1)
        ans_df['SMB'] = (ret_SV + ret_SM + ret_SG - ret_BV - ret_BM - ret_BG)/3
        ans_df['VMG'] = (ret_SV + ret_BV - ret_SG - ret_BG)/2
        # ----以下勿改动----
        ans_df = ans_df.loc[self.start_date: self.end_date]
        ans_df = Dtk.convert_df_index_type(ans_df, 'date_int', 'timestamp')
        return ans_df
