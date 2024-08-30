# -*- coding: utf-8 -*-
"""
created on 2019/03/20
013542
根据该文章构造的《美股上一个跨越时间尺度的趋势因子》  https://zhuanlan.zhihu.com/p/51043407
"""
from Factor.DailyFactorBase import DailyFactorBase
import DataAPI.DataToolkit as Dtk
import pandas as pd
import numpy as np

class FactorDailyResMaRegressionStd(DailyFactorBase):
    # 这个因子没有参数，但也须在初始化时预留一个"params"
    def __init__(self, alpha_factor_root_path, stock_list, start_date_int, end_date_int, params):
        super().__init__(alpha_factor_root_path)
        self.stock_list = stock_list
        self.start_date = start_date_int
        self.end_date = end_date_int
        self.Lag_list = params['Lag_list']
        self.n = params['n']

    def factor_calc(self):
        valid_start_date = Dtk.get_n_days_off(self.start_date, -250)[0]
        temp_start_date = Dtk.get_n_days_off(self.start_date, -20)[0]
        stock_close = Dtk.get_panel_daily_pv_df(self.stock_list, valid_start_date, self.end_date,
                                                pv_type='close', adj_type='FORWARD')
        stock_amt = Dtk.get_panel_daily_pv_df(self.stock_list, valid_start_date, self.end_date,
                                                pv_type='amt')
        stock_ret = stock_close/stock_close.shift(1) - 1
        stock_ret = stock_ret * stock_amt/stock_amt
        trading_days = Dtk.get_trading_day(temp_start_date, self.end_date)
        MaRatio_list = []
        for lag in self.Lag_list:
            MaRatio_list.append((stock_close.rolling(lag).mean()) / stock_close)
        ans_df = stock_close.copy()
        ans_df[:] = np.nan
        for date in trading_days:
            reg_data = pd.DataFrame([])
            reg_data['ret'] = stock_ret.loc[date,:]
            for i in range(MaRatio_list.__len__()):
                reg_data['MaRatioIndex'+str(i)] = MaRatio_list[i].shift(2).loc[date,:]
            reg_data.dropna(inplace=True)
            temp = (reg_data.iloc[:,1:]).T
            X = np.vstack([np.array(temp), np.ones(temp.shape[1])])
            reg = np.linalg.inv(X.dot(X.T)).dot(X).dot(np.array(reg_data['ret']))
            res = reg_data['ret'] - X.T.dot(reg)
            ans_df.loc[date] = res
        ans_df = ans_df.rolling(self.n).std()
        # ----以下勿改动----
        ans_df = ans_df.loc[self.start_date: self.end_date]
        ans_df = Dtk.convert_df_index_type(ans_df, 'date_int', 'timestamp')
        return ans_df
