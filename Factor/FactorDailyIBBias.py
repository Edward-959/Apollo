# -*- coding: utf-8 -*-
"""
@author: 011672, 2019/4/17
依据两张不同财务报表回归之后求截距来计算因子
parameter n: 回归回溯时间序列
parameter x: 自变量
parameter y: 因变量
"""

import DataAPI.DataToolkit as Dtk
from Factor.DailyFactorBase import DailyFactorBase
import numpy as np
from xquant.multifactor.IO.IO import read_data
from copy import deepcopy
import pandas as pd
import DataAPI
import os


class FactorDailyIBBias(DailyFactorBase):
    # 这个因子没有参数，但也须在初始化时预留一个"params"
    def __init__(self, alpha_factor_root_path, stock_list, start_date_int, end_date_int, params):
        super().__init__(alpha_factor_root_path)
        self.stock_list = stock_list
        self.start_date = start_date_int
        self.end_date = end_date_int
        self.rolling_window = params['n']
        self.x = params['x']
        self.y = params['y']

    def factor_calc(self):
        data_type = 'ttm'
        if data_type == 'original':  # original则向前回溯2个季度
            last_report_date0 = Dtk.start_date_backfill(self.start_date, back_years=0)
        else:  # ttm则向前回溯1年+2个季度
            last_report_date0 = Dtk.start_date_backfill(self.start_date, back_years=4)

        report_dates_list = DataAPI.GetTradingDay.get_quarterly_report_dates_list(last_report_date0, self.end_date)

        income_df = Dtk.return_statement_type_filtered_df("AShareIncome", self.y,
                                                          last_report_date0, self.end_date)
        ann_df = income_df['ANN_DT']
        ann_df = Dtk.df_unstack_and_filter(ann_df, self.stock_list, report_dates_list)
        y = income_df[self.y]
        y = Dtk.df_unstack_and_filter(y, self.stock_list, report_dates_list)

        # 计算 earnings_ttm
        data_df = y.copy()
        data_result = pd.DataFrame(index=report_dates_list, columns=self.stock_list)
        for i_report_date in report_dates_list[4:]:
            if str(i_report_date)[4:8] == '1231':
                data_result.loc[i_report_date] = data_df.loc[i_report_date]
            else:
                data_result.loc[i_report_date] = data_df.loc[i_report_date] + data_df.loc[
                    int(str(i_report_date - 10000)[0:4] + '1231')] - data_df.loc[i_report_date - 10000]
        y_ttm = data_result

        balance_df = Dtk.return_statement_type_filtered_df("AShareBalanceSheet", self.x,
                                                           last_report_date0, self.end_date)
        x = balance_df[self.x]
        x = Dtk.df_unstack_and_filter(x, self.stock_list, report_dates_list)

        x = (x - x.mean()) / x.std()
        y_ttm = (y_ttm - y_ttm.mean()) / y_ttm.std()
        factor_dict = {}
        # 对全部股票做遍历
        for column in list(x.columns):
            x_column = list(x[column])
            y_column = list(y_ttm[column])
            factor_dict.update({column: []})
            # 对每支股票的时间序列做遍历
            for i in range(len(x_column)):
                if i < self.rolling_window:
                    factor_dict[column].append(float('nan'))
                else:
                    regress_y = np.array(y_column[i - self.rolling_window + 1:i + 1])
                    regress_x = np.array(x_column[i - self.rolling_window + 1:i + 1])
                    k = float(np.cov(regress_y, regress_x)[0, 1] / np.var(regress_x))
                    b = regress_y.mean() - k * regress_x.mean()
                    factor_dict[column].append(b)

        factor_dict = pd.DataFrame(factor_dict, index=x.index)
        factor_dict = factor_dict.reindex(columns=x.columns)
        trading_days = Dtk.get_trading_day(last_report_date0, self.end_date)
        data_raw = pd.DataFrame(index=trading_days, columns=ann_df.columns)
        ann_df = ann_df.fillna(0)
        ans_df = Dtk.back_fill(data_raw, factor_dict, ann_df, fill_na=False)
        ans_df = pd.DataFrame(ans_df, dtype=np.float)
        # ----以下勿改动----
        ans_df = ans_df.loc[self.start_date: self.end_date]
        ans_df = Dtk.convert_df_index_type(ans_df, 'date_int', 'timestamp')
        return ans_df
