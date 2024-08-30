#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019/1/18 13:50
# @Author  : 011673
import DataAPI.DataToolkit as Dtk
from Factor.DailyFactorBase import DailyFactorBase
import pandas as pd
import numpy as np

def datetime2int(date_time):
    temp = str(date_time)
    date_int = int(temp[0:4]) * 10000 + int(temp[5:7]) * 100 + int(temp[8:10])
    return date_int


def wind_get_daily_data(stock_list, sheet_name, list_i, start_date_int, end_date_int, back_fill=1, consoli_state=None):
    address = '/app/data/wdb_h5/WIND/' + sheet_name + '/' + sheet_name + '.h5'
    store = pd.HDFStore(address, mode='r')
    all_pandas = store.select("/" + sheet_name)
    store.close()
    if consoli_state is not None:
        all_pandas = all_pandas[all_pandas['STATEMENT_TYPE'] == consoli_state]  # 取合并报表的值
    LIST_I = list_i.upper()  # col名为字段名称大写
    if LIST_I in all_pandas:
        pd_field = all_pandas[LIST_I]  # pd_field为一个timestamp+股票名的双索引，需要unstack后方便处理
        if not pd_field.index.is_unique:  # 部分字段在退市或未上市新股里存在出现两个相同index的情况，此时无法直接unstack，因为对输出值没有影响，保留其中第一项即可
            pd_field = pd_field[~pd_field.index.duplicated(keep='first')]
        pd_field_unstack = pd_field.unstack().reset_index()
        pd_field_unstack['dt'] = pd_field_unstack['dt'].apply(lambda x: datetime2int(x))  # dt字段格式转为int型日期，与目前数据统一
        pd_field_unstack.index = pd_field_unstack['dt']
        pd_field_unstack.sort_index(inplace=True)  # 原表内数据日期存在错乱，需要先排序
        pd_field_unstack = pd_field_unstack.drop('dt', axis=1)
        trading_day = Dtk.get_trading_day(pd_field_unstack.index.tolist()[0], end_date_int)
        pd_field_unstack = pd_field_unstack.reindex(columns=stock_list, index=trading_day)
        if back_fill != 1:
            pd_field_unstack.fillna(method='ffill', inplace=True)
        pd_field_unstack = pd_field_unstack.loc[start_date_int:]
        return pd_field_unstack


class FactorDailyHolderNumber(DailyFactorBase):
    # 这个因子没有参数，但也须在初始化时预留一个"params"
    def __init__(self, alpha_factor_root_path, stock_list, start_date_int, end_date_int, params):
        super().__init__(alpha_factor_root_path)
        self.stock_list = stock_list
        self.start_date = start_date_int
        self.end_date = end_date_int

    def factor_calc(self):
        valid_start = Dtk.get_n_days_off(self.start_date, -250)[0]
        cap = Dtk.get_panel_daily_info(self.stock_list, valid_start, self.end_date, "mkt_cap_ard")
        factor_data: pd.DataFrame = wind_get_daily_data(self.stock_list, "AShareHolderNumber", 's_holder_num',
                                                        valid_start, self.end_date, back_fill=2)
        factor_data.ffill(inplace=True)
        factor_data = factor_data.loc[cap.index, cap.columns]
        factor_data.replace(np.inf, np.nan, inplace=True)
        ans_df=factor_data
        # ----以下勿改动----
        ans_df = ans_df.loc[self.start_date: self.end_date]
        ans_df = Dtk.convert_df_index_type(ans_df, 'date_int', 'timestamp')
        return ans_df
