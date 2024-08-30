# -*- coding: utf-8 -*-
# @Time    : 2018/12/10 10:20
# @Author  : 011673
# @File    : FactorDailySeperateBeforehandRet.py
import DataAPI.DataToolkit as Dtk
from Factor.DailyFactorBase import DailyFactorBase
import pandas as pd
import copy
import numpy as np


def get_SBR(ret: pd.DataFrame, last_turn: pd.DataFrame):
    # 上行
    ret_up = ret[ret > ret.median()]
    ret_up = ret_up * last_turn / last_turn
    codition = pd.isna(ret_up)
    last_turn_up = copy.deepcopy(last_turn)
    last_turn_up[codition == True] = np.nan
    # 下行
    ret_down = ret[ret <= ret.median()]
    ret_down = ret_down * last_turn / last_turn
    codition = pd.isna(ret_down)
    last_turn_down = copy.deepcopy(last_turn)
    last_turn_down[codition == True] = np.nan

    result = []
    for i in last_turn_up.columns:
        data_1: pd.Series = ret_up.loc[:, i].dropna()
        data_2: pd.Series = last_turn_up.loc[:, i].dropna()
        data_3: pd.Series = ret_down.loc[:, i].dropna()
        data_4: pd.Series = last_turn_down.loc[:, i].dropna()
        if data_1.__len__() < 10 or data_2.__len__() < 10 or data_3.__len__() < 10 or data_4.__len__() < 10:
            temp = np.nan
        else:
            temp = data_1.corr(data_2) - data_3.corr(data_4)
        result.append(temp)
    return result


def pre_process(data: pd.Series):
    if data.std() == 0:
        return data
    max_value = data.mean() + 3 * data.std()
    min_value = data.mean() - 3 * data.std()
    data[data > max_value] = max_value
    data[data < min_value] = min_value
    data = (data - data.mean()) / data.std()
    return data


class FactorDailySeperateBeforehandRet(DailyFactorBase):
    def __init__(self, alpha_factor_root_path, stock_list, start_date_int, end_date_int, params):
        super().__init__(alpha_factor_root_path)
        self.stock_list = stock_list
        self.start_date = start_date_int
        self.end_date = end_date_int
        self.n = params["n"]

    def factor_calc(self):
        start_date_minus_lag_2 = Dtk.get_n_days_off(self.start_date, -(2 * self.n + 2))[0]
        close: pd.DataFrame = Dtk.get_panel_daily_pv_df(self.stock_list, start_date_minus_lag_2, self.end_date,
                                                        pv_type='close', adj_type='FORWARD')
        turn: pd.DataFrame = Dtk.get_panel_daily_info(self.stock_list, start_date_minus_lag_2, self.end_date, info_type='turn')
        ret: pd.DataFrame = (close - close.shift(1)) / close.shift(1)
        last_turn = turn.shift(1)
        number = 2 * self.n
        factor_list = []
        index_list = []
        print('prcocess begin')
        for index in ret.index[2 * self.n:]:
            number = number + 1  # iloc取值左闭右开，当天的因子值需要包括当日行情，所以在开始用number+=1，使取到的行情可以包括今日
            print('{}/{}'.format(number, len(ret.index)))
            index_list.append(index)
            temp_ret = ret.iloc[number - 2 * self.n:number]
            temp_last_turn = last_turn.iloc[number - 2 * self.n:number]
            temp_reult = get_SBR(temp_ret, temp_last_turn)
            factor_list.append(temp_reult)
        factor_data = pd.DataFrame(factor_list, index=index_list, columns=last_turn.columns)
        factor_original_df = factor_data.loc[self.start_date: self.end_date].copy()
        ans_df = factor_original_df
        ans_df = ans_df.loc[self.start_date: self.end_date]
        ans_df = Dtk.convert_df_index_type(ans_df, 'date_int', 'timestamp')
        return ans_df
