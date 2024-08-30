# -*- coding: utf-8 -*-
"""
@author: 013542
from 研报《民生证券因子研究专题三：动量（反转）因子解析》
"""
from Factor.DailyFactorBase import DailyFactorBase
import DataAPI.DataToolkit as Dtk

class FactorDailyRetRankStd(DailyFactorBase):
    # 这个因子没有参数，但也须在初始化时预留一个"params"
    def __init__(self, alpha_factor_root_path, stock_list, start_date_int, end_date_int, params):
        super().__init__(alpha_factor_root_path)
        self.stock_list = stock_list
        self.start_date = start_date_int
        self.end_date = end_date_int
        self.n = params['n']

    def factor_calc(self):
        valid_start_date = Dtk.get_n_days_off(self.start_date, -self.n-2)[0]
        stock_close = Dtk.get_panel_daily_pv_df(self.stock_list, valid_start_date, self.end_date,
                                                pv_type='close', adj_type='FORWARD')
        stock_amt = Dtk.get_panel_daily_pv_df(self.stock_list, valid_start_date, self.end_date,
                                              pv_type='amt')
        stock_ret = stock_close / stock_close.shift(1) - 1
        stock_ret = stock_ret * stock_amt/stock_amt
        stock_ret_rank = stock_ret.rank(axis=1)
        stock_ret_rank_std = stock_ret_rank.rolling(self.n).std()
        ans_df = stock_ret_rank_std
        ans_df = ans_df.loc[self.start_date: self.end_date]
        ans_df = Dtk.convert_df_index_type(ans_df, 'date_int', 'timestamp')
        return ans_df