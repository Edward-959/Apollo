#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019/1/18 14:14
# @Author  : 011673
import DataAPI.DataToolkit as Dtk
from Factor.DailyFactorBase import DailyFactorBase
import numpy as np
from xquant.multifactor.IO.IO import read_data
import pandas as pd


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


class FactorDailyCashRatio(DailyFactorBase):
    # 这个因子没有参数，但也须在初始化时预留一个"params"
    def __init__(self, alpha_factor_root_path, stock_list, start_date_int, end_date_int, params):
        super().__init__(alpha_factor_root_path)
        self.stock_list = stock_list
        self.start_date = start_date_int
        self.end_date = end_date_int

    def factor_calc(self):
        industry=Dtk.get_panel_daily_info(self.stock_list, self.start_date, self.end_date, "industry3")
        sepcial_position = industry.isin([21.0, 29.0, 30.0, 31.0])
        cap = Dtk.get_panel_daily_info(self.stock_list, self.start_date, self.end_date, "mkt_cap_ard")
        monetary_cap = get_wind_quarter_data(self.stock_list, 'AShareBalanceSheet', 'monetary_cap',
                                             self.start_date, self.end_date)
        monetary_cap = monetary_cap.loc[cap.index, cap.columns]
        notes_rcv = get_wind_quarter_data(self.stock_list, 'AShareBalanceSheet', 'notes_rcv', self.start_date,
                                          self.end_date)
        notes_rcv:pd.DataFrame= notes_rcv.loc[cap.index, cap.columns]
        notes_rcv.fillna(0,inplace=True)
        notes_rcv[sepcial_position]=np.nan
        tradable_fin_assets = get_wind_quarter_data(self.stock_list, 'AShareBalanceSheet',
                                                                   'tradable_fin_assets', self.start_date, self.end_date)
        tradable_fin_assets = tradable_fin_assets.loc[cap.index, cap.columns]
        tradable_fin_assets.fillna(0,inplace=True)
        tradable_fin_assets[sepcial_position]=np.nan
        tot_cur_liab = get_wind_quarter_data(self.stock_list, 'AShareBalanceSheet', 'tot_cur_liab',#
                                             self.start_date, self.end_date)
        tot_cur_liab = tot_cur_liab.loc[cap.index, cap.columns]
        factor_data = (monetary_cap + notes_rcv + tradable_fin_assets) / tot_cur_liab
        factor_data = factor_data.loc[cap.index, cap.columns]
        factor_data.replace(np.inf, np.nan, inplace=True)
        ans_df=factor_data
        # ----以下勿改动----
        ans_df = ans_df.loc[self.start_date: self.end_date]
        ans_df = Dtk.convert_df_index_type(ans_df, 'date_int', 'timestamp')
        return ans_df