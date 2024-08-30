#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019/1/18 14:00
# @Author  : 011673
import DataAPI.DataToolkit as Dtk
from Factor.DailyFactorBase import DailyFactorBase
import numpy as np
import pandas as pd
from xquant.multifactor.IO.IO import read_data


def get_wind_quarter_data(complete_stock_list, alt, column, start_date_int, end_date_int):
    last_report_date = Dtk.start_date_backfill(start_date_int)
    df = read_data([last_report_date, end_date_int], alt=alt)
    ann_df = df["ANN_DT"]
    column_up = column.upper()
    data_df_info = df[column_up]
    if alt == 'AShareBalanceSheet':
        statement_type = df["STATEMENT_TYPE"]
        if not ann_df.index.is_unique:  # 同一财务指标对应多个值，保留更正前报表的财政数据。如果未变更过则保留原数据
            condition1 = np.array(statement_type == '408001000') + np.array(statement_type == '408005000')
            statement_type = statement_type[condition1]
            ann_df = ann_df[condition1]
            data_df_info = data_df_info[condition1]
            ann_df = ann_df[~statement_type.index.duplicated(False) + np.array(statement_type == '408005000')]
            data_df_info = data_df_info[
                ~statement_type.index.duplicated(False) + np.array(statement_type == '408005000')]
    ann_df = ann_df.unstack()
    ann_df = ann_df.fillna(0)
    data_df_info = data_df_info.unstack()
    stock_list = data_df_info.columns.tolist()
    if type(stock_list[0]) == tuple:
        stock_list = [x[1] for x in stock_list]
        data_df_info.columns = stock_list
        ann_df.columns = stock_list
    if type(data_df_info.index.tolist()[1]) != int:
        data_df_info = Dtk.convert_df_index_type(data_df_info, 'timestamp2', 'date_int')
    if type(ann_df.index.tolist()[1]) != int:
        ann_df = Dtk.convert_df_index_type(ann_df, 'timestamp2', 'date_int')
    last_report_date = Dtk.start_date_backfill(start_date_int)
    trading_day_1 = Dtk.get_trading_day(last_report_date, end_date_int)
    data_df_raw = pd.DataFrame(index=trading_day_1, columns=stock_list)
    data_df = Dtk.back_fill(data_df_raw, data_df_info, ann_df)
    data_df = data_df.loc[start_date_int:]
    data_df = data_df.reindex(columns=complete_stock_list)
    return data_df


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


class FactorDailyEV2DevideEBITDA(DailyFactorBase):
    # 这个因子没有参数，但也须在初始化时预留一个"params"
    def __init__(self, alpha_factor_root_path, stock_list, start_date_int, end_date_int, params):
        super().__init__(alpha_factor_root_path)
        self.stock_list = stock_list
        self.start_date = start_date_int
        self.end_date = end_date_int

    def factor_calc(self):
        cap = Dtk.get_panel_daily_info(self.stock_list, self.start_date, self.end_date, "mkt_cap_ard")
        s_val_mv = wind_get_daily_data(self.stock_list, 'AShareEODDerivativeIndicator', 's_val_mv', self.start_date,
                                       self.end_date)
        s_val_mv = s_val_mv.loc[cap.index, cap.columns]
        s_fa_interestdebt = get_wind_quarter_data(self.stock_list, 'AShareFinancialIndicator', 's_fa_interestdebt',
                                                self.start_date, self.end_date)
        s_fa_interestdebt = s_fa_interestdebt.loc[cap.index, cap.columns]
        monetary_cap = get_wind_quarter_data(self.stock_list, 'AShareBalanceSheet', 'monetary_cap',
                                                            self.start_date, self.end_date)
        monetary_cap = monetary_cap.loc[cap.index, cap.columns]
        s_fa_ebitda = get_wind_quarter_data(self.stock_list, 'AShareFinancialIndicator', 's_fa_ebitda', self.start_date,
                                          self.end_date)
        factor_data = (s_val_mv + s_fa_interestdebt - monetary_cap) / s_fa_ebitda
        factor_data = factor_data.loc[cap.index, cap.columns]
        factor_data.replace(np.inf, np.nan, inplace=True)
        ans_df=factor_data
        # ----以下勿改动----
        ans_df = ans_df.loc[self.start_date: self.end_date]
        ans_df = Dtk.convert_df_index_type(ans_df, 'date_int', 'timestamp')
        return ans_df