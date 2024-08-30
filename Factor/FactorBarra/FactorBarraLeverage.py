# -*- coding: utf-8 -*-
"""
@author: 006566
Created on 2019/03/05
Style: Leverage
Definition: 1/3 * MLEV + 1/3 * DTOA + 1/3 BLEV
MLEV: Market leverage, (总市值+非流动负债)/总市值
DTOA: Debt-to-assets, 总负债/总资产
BLEV: Book leverage, (净资产+非流动负债)/净资产
最后把所有金融业（21: 银行, 29: 证券Ⅱ, 30: 保险Ⅱ, 31: 信托及其他）的Leverage设为nan
"""
from Factor.DailyFactorBase import DailyFactorBase
import DataAPI.DataToolkit as Dtk


class FactorBarraLeverage(DailyFactorBase):
    # 这个因子没有参数，但也须在初始化时预留一个"params"
    def __init__(self, alpha_factor_root_path, stock_list, start_date_int, end_date_int, params):
        super().__init__(alpha_factor_root_path)
        self.stock_list = stock_list
        self.start_date = start_date_int
        self.end_date = end_date_int

    def factor_calc(self):
        mkt_cap_df = Dtk.get_panel_daily_info(self.stock_list, self.start_date, self.end_date, 'mkt_cap_ard')
        tot_non_cur_liab_df = Dtk.get_panel_daily_info(self.stock_list, self.start_date, self.end_date,
                                                       'tot_non_cur_liab')
        # 因为非流动负债这个字段很多公司为0，而Wind数据库的bug使得0的值为nan，nan的出现会导致计算后这个股票的因子值变为
        # nan，而这是错的
        tot_non_cur_liab_df = tot_non_cur_liab_df.fillna(0)
        dtoa = Dtk.get_panel_daily_info(self.stock_list, self.start_date, self.end_date, 'debttoassets')
        dtoa = dtoa.fillna(0)
        be_df = Dtk.get_panel_daily_info(self.stock_list, self.start_date, self.end_date, 'tot_equity')
        mlev = (mkt_cap_df + tot_non_cur_liab_df) / mkt_cap_df
        blev = (be_df + tot_non_cur_liab_df) / be_df
        ans_df = mlev.mul(1/3) + dtoa.mul(1/3) + blev.mul(1/3)
        ans_df = ans_df * mkt_cap_df / mkt_cap_df
        industry_df = Dtk.get_panel_daily_info(self.stock_list, self.start_date, self.end_date, 'industry3')
        industry_df = industry_df.replace([21.0, 29.0, 30.0, 31.0, 21, 29, 30, 31], None)
        ans_df = ans_df * industry_df / industry_df  # 将4个金融业的杠杆率设为nan
        # ----以下勿改动----
        ans_df = ans_df.loc[self.start_date: self.end_date]
        ans_df = Dtk.convert_df_index_type(ans_df, 'date_int', 'timestamp')
        return ans_df
