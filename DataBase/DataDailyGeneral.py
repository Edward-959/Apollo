# -*- coding: utf-8 -*-
"""
created on 2019/3/2
@author: 006566
下载个股和指数日级别数据通用 8个字段——Close, High, Low, Open, PreClose, Amt, Volume, PctChg
"""
from Factor.DailyFactorBase import DailyFactorBase
import DataAPI.DataToolkit as Dtk
import xquant.multifactor.IO.IO as xIO
import pandas as pd


class DataDailyGeneral(DailyFactorBase):
    # 这个因子没有参数，但也须在初始化时预留一个"params"
    def __init__(self, alpha_factor_root_path, start_date_int, end_date_int, params):
        super().__init__(alpha_factor_root_path)
        self.start_date = start_date_int
        self.end_date = end_date_int
        self.item = params["item"]

    def factor_calc(self):
        # 添加股票和指数
        alt = "AShareEODPrices"
        df1 = xIO.read_data([self.start_date, self.end_date], alt=alt)[self.item]
        df1 = df1.unstack()
        alt = "AIndexEODPrices"
        df2 = xIO.read_data([self.start_date, self.end_date], alt=alt)[self.item]
        df2 = df2.unstack()
        imp_index_list = ["000001.SH", "000016.SH", "000300.SH", "000905.SH", "000906.SH", "399001.SZ",
                          "399006.SZ"]
        df2 = df2.reindex(columns=imp_index_list)
        factor_df = pd.concat([df1, df2], axis=1)
        factor_df = Dtk.convert_df_index_type(factor_df, 'timestamp2', 'date_int')
        valid_trading_day_list = Dtk.get_trading_day(self.start_date, self.end_date)
        ans_df = factor_df.reindex(valid_trading_day_list)
        ans_df.columns.name = ''
        # ----以下勿改动----
        ans_df = ans_df.loc[self.start_date: self.end_date]
        return ans_df
