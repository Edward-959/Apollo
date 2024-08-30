# -*- coding: utf-8 -*-
# @Time    : 2018/12/10 9:59
# @Author  : 011673
# @File    : FactorDailyBeforehandRet.py
import DataAPI.DataToolkit as Dtk
from Factor.DailyFactorBase import DailyFactorBase
import pandas as pd
import numpy as np


def get_cut_corr(ret: pd.DataFrame, last_turn: pd.DataFrame):
    result = []
    for i in last_turn.columns:
        temp_ret: pd.Series = ret.loc[:, i].dropna()
        temp_ret = temp_ret[temp_ret < temp_ret.quantile(0.9)]
        temp_last_turn: pd.Series = last_turn.loc[temp_ret.index, i]
        temp_ret = temp_ret * temp_last_turn / temp_last_turn
        temp_last_turn.dropna(inplace=True)
        temp_ret.dropna(inplace=True)
        if len(temp_last_turn.index) >= 10:
            result.append(temp_last_turn.corr(temp_ret))
        else:
            result.append(np.nan)
    return result


class FactorDailyBeforehandRetCut(DailyFactorBase):
    def __init__(self, alpha_factor_root_path, stock_list, start_date_int, end_date_int, params):
        super().__init__(alpha_factor_root_path)
        self.stock_list = stock_list
        self.start_date = start_date_int
        self.end_date = end_date_int
        self.n = params["n"]

    def factor_calc(self):
        start_date_minus_lag_2 = Dtk.get_n_days_off(self.start_date, -(self.n + 2))[0]
        close: pd.DataFrame = Dtk.get_panel_daily_pv_df(self.stock_list, start_date_minus_lag_2, self.end_date,
                                                        pv_type='close', adj_type='FORWARD')
        turn: pd.DataFrame = Dtk.get_panel_daily_info(self.stock_list, start_date_minus_lag_2, self.end_date, info_type='turn')
        ret: pd.DataFrame = (close - close.shift(1)) / close.shift(1)
        last_turn = turn.shift(1)
        print('prcocess begin')
        factor_list = []
        index_list = []
        number = self.n
        for index in ret.index[self.n:]:
            number = number + 1  # iloc取值左闭右开，当天的因子值需要包括当日行情，所以在开始用number+=1，使取到的行情可以包括今日
            print('{}/{}'.format(number, len(ret.index)))
            index_list.append(index)
            temp_ret = ret.iloc[number - self.n:number]
            temp_last_turn = last_turn.iloc[number - self.n:number]
            temp_reult = get_cut_corr(temp_ret, temp_last_turn)
            factor_list.append(temp_reult)
        factor_data = pd.DataFrame(factor_list, index=index_list, columns=last_turn.columns)
        factor_original_df = factor_data.loc[self.start_date: self.end_date].copy()
        ans_df = factor_original_df
        ans_df = ans_df.loc[self.start_date: self.end_date]
        ans_df = Dtk.convert_df_index_type(ans_df, 'date_int', 'timestamp')
        return ans_df
