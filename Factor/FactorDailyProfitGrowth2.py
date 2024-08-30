# -*- coding: utf-8 -*-
"""
@author: 011672, 2019/4/10
依据当年因子值与过去一年平均因子值之比的方式求取因子值
该因子使用扣除非经常性损益后净利润指标
"""

import DataAPI.DataToolkit as Dtk
from Factor.DailyFactorBase import DailyFactorBase
import numpy as np
from xquant.multifactor.IO.IO import read_data
from copy import deepcopy
import pandas as pd
import DataAPI



class FactorDailyProfitGrowth2(DailyFactorBase):
    # 这个因子没有参数，但也须在初始化时预留一个"params"
    def __init__(self, alpha_factor_root_path, stock_list, start_date_int, end_date_int, params):
        super().__init__(alpha_factor_root_path)
        self.stock_list = stock_list
        self.start_date = start_date_int
        self.end_date = end_date_int

    def factor_calc(self):
        data_type = 'ttm'
        earnings_ttm = Dtk.get_daily_wind_quarterly_data(self.stock_list, 'AShareIncome', "net_profit_after_ded_nr_lp".upper(), self.start_date, self.end_date, data_type='ttm')
        if data_type == 'original':  # original则向前回溯2个季度
            last_report_date0 = Dtk.start_date_backfill(self.start_date, back_years=0)
        else:  # ttm则向前回溯1年+2个季度
            last_report_date0 = Dtk.start_date_backfill(self.start_date, back_years=2)

        report_dates_list = DataAPI.GetTradingDay.get_quarterly_report_dates_list(last_report_date0, self.end_date)

        income_df = Dtk.return_statement_type_filtered_df("AShareIncome", "net_profit_after_ded_nr_lp".upper(),
                                                          last_report_date0, self.end_date)
        ann_df = income_df['ANN_DT']
        ann_df = Dtk.df_unstack_and_filter(ann_df, self.stock_list, report_dates_list)
        earnings = income_df['net_profit_after_ded_nr_lp'.upper()]
        earnings = Dtk.df_unstack_and_filter(earnings, self.stock_list, report_dates_list)

        # 计算 earnings_ttm
        data_df = earnings.copy()
        data_result = pd.DataFrame(index=report_dates_list, columns=self.stock_list)
        for i_report_date in report_dates_list[4:]:
            if str(i_report_date)[4:8] == '1231':
                data_result.loc[i_report_date] = data_df.loc[i_report_date]
            else:
                data_result.loc[i_report_date] = data_df.loc[i_report_date] + data_df.loc[
                    int(str(i_report_date - 10000)[0:4] + '1231')] - data_df.loc[i_report_date - 10000]
        earnings_ttm1 = data_result
        earnings_ttm_average = (earnings_ttm1.shift(1) + earnings_ttm1.shift(2) +earnings_ttm1.shift(3) + earnings_ttm1.shift(4))/4
        ann_df = ann_df.fillna(0)
        trading_days = Dtk.get_trading_day(last_report_date0, self.end_date)
        data_raw = pd.DataFrame(index=trading_days , columns=ann_df.columns)     
        earnings_ttm_average = Dtk.back_fill(data_raw, earnings_ttm_average, ann_df) 
        ans_df = earnings_ttm/earnings_ttm_average                                                   
        # ----以下勿改动----
        ans_df = ans_df.loc[self.start_date: self.end_date]
        ans_df = Dtk.convert_df_index_type(ans_df, 'date_int', 'timestamp')
        return ans_df
