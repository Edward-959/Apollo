# -*- coding: utf-8 -*-
"""
Created on 2019/2/2
@author: Xiu Zixing
依据下列四个条件筛选股票池
1. 上市满一年
2. 非 STPT
3. 过去半年至少一的正常交易
4. 剔除市值最小的5%的股票
"""
from Factor.DailyFactorBase import DailyFactorBase
import DataAPI.DataToolkit as Dtk
import pandas as pd
from xquant.multifactor.IO.IO import *
import numpy as np
import datetime as dt


def datetime2int(date_time):
    temp = str(date_time)
    date_int = int(temp[0:4]) * 10000 + int(temp[5:7]) * 100 + int(temp[8:10])
    return date_int


def int2datetime(date_int):
    temp = str(date_int)
    date_time = dt.datetime(int(temp[0:4]), int(temp[4:6]), int(temp[6:8]))
    return date_time


def risk_universe(date):
    alt1 = "MD_CHINA_STOCK_DAILY_WIND"
    alt2 = "UNIV_CHINA_STOCK_DAILY_OPTM"
    alt3 = "universe_complete"
    # 读第一张表
    df1 = read_data([date, date], alt=alt1)
    market_cap = df1['mkt_cap_ard']
    market_cap_list = market_cap.tolist()
    stock_list = [x[1] for x in market_cap.index.tolist()]
    # 建立四个字段，符合每个字段下每日符合条件则记为1，最后将4个字段的值相加，值为4的即为当日股票池成员
    columns = ['listing_1yr', 'NoSTPT', 'HalfYearInTrade', '95%MarketValue']
    risk_universe = pd.DataFrame(index=stock_list, columns=columns, data=0)
    stock_num = len(stock_list)
    percentile95 = round(stock_num * 0.95)
    market_cap_list = np.array(market_cap_list)
    nan_market_cap = []
    for i in range(0, len(stock_list)):
        if np.isnan(market_cap_list[i]) == 1:
            nan_market_cap.append(i)
    market_cap_sort = np.argsort(-market_cap_list)
    stock_list = np.array(stock_list)
    stock_list = stock_list[market_cap_sort]
    stock_list = stock_list[:percentile95]
    for stock in stock_list:
        risk_universe.at[stock, '95%MarketValue'] = 1

    # 读第二张表
    df2 = read_data([date, date], alt=alt2)
    half_year_in_trade = df2['over_half_for_half_year']
    list_date = df2['Listing_date']
    stock_list = [x[1] for x in half_year_in_trade.index.tolist()]
    half_year_in_trade_bool = half_year_in_trade.tolist()
    for stock in risk_universe.index.tolist():
        stock_index = stock_list.index(stock)
        if half_year_in_trade_bool[stock_index] + 0 == 1:
            risk_universe.at[stock, 'HalfYearInTrade'] = 1
        else:
            risk_universe.at[stock, 'HalfYearInTrade'] = 0
        listingDate = list_date[stock_index]
        listingDatedt = int2datetime(listingDate)
        datedt = int2datetime(date)
        daydiff = datedt - listingDatedt
        if daydiff.days > 365:
            risk_universe.at[stock, 'listing_1yr'] = 1
        else:
            risk_universe.at[stock, 'listing_1yr'] = 0
    # 读第三张表
    df3 = read_data([date, date], alt=alt3)
    stock_list = [x[1] for x in df3.index.tolist()]
    noSuspend = df3['SUSPEND']
    noSTPT = df3['STPT']
    noSuspend_bool = noSuspend.tolist()
    noSTPT_bool = noSTPT.tolist()
    for stock in risk_universe.index.tolist():
        stock_index = stock_list.index(stock)
        if noSTPT_bool[stock_index] == 1.0:
            risk_universe.at[stock, 'NoSTPT'] = 1
        else:
            risk_universe.at[stock, 'NoSTPT'] = 0
    risk_universe_1 = []
    for stock in risk_universe.index.tolist():
        if sum(risk_universe.loc[stock]) == 4:
            risk_universe_1.append(stock)
    return risk_universe_1


class DataDailyRiskUniverse(DailyFactorBase):
    # 这个因子没有参数，但也须在初始化时预留一个"params"
    def __init__(self, alpha_factor_root_path,  start_date_int, end_date_int, params):
        super().__init__(alpha_factor_root_path)
        self.start_date = start_date_int
        self.end_date = end_date_int

    def factor_calc(self):
        stock_list = Dtk.get_complete_stock_list()
        tradingDay = Dtk.get_trading_day(self.start_date, self.end_date)
        risk_universe_df = pd.DataFrame(columns=stock_list, index=tradingDay, data=0)
        for day in tradingDay:
            risk_universe_list = risk_universe(day)
            for stock in risk_universe_list:
                risk_universe_df.loc[day][stock] = 1
        ans_df = risk_universe_df
        # ----以下勿改动----
        ans_df = ans_df.loc[self.start_date: self.end_date]
        return ans_df