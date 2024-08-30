#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019/3/14 10:26
# @Author  : 011673
from Factor.DailyMinFactorBase import DailyMinFactorBase
import DataAPI.DataToolkit as Dtk
import pandas as pd
import statsmodels.tsa.stattools as ts
import numpy as np
import DataUpdater.IndustryIndexUpdate as industry_update


def load_industry_index():
    save_path = industry_update.get_path()
    result = {}
    store = pd.HDFStore(save_path + 'Industry.h5')
    for i in range(1, 32):
        result['industry_' + str(i)] = store.select('industry_' + str(i))
    return result


class NonFactorDailyMinIndustryRetMatching(DailyMinFactorBase):
    def __init__(self, alpha_factor_root_path, stock_list, start_date_int, end_date_int, params):
        super().__init__(alpha_factor_root_path, stock_list, start_date_int, end_date_int)
        self.__n = params['n']
        self.__start_date_minus_n_2 = Dtk.get_n_days_off(start_date_int, -(self.__n + 2))[0]
        self.__industry_ret = load_industry_index()
        self.__industry = Dtk.get_panel_daily_info(self.stock_list, self.__start_date_minus_n_2, self.end_date,
                                                   'industry3')
        self.__time_columns = list(self.__industry_ret['industry_1'].columns)
        print('init')

    def get_industry_data_in_code(self, code, date_list):
        result = {}
        for date in date_list:
            industry_number = self.__industry.loc[date, code]
            try:
                industry_number = int(industry_number)
            except:
                result[date] = pd.Series(1.0, index=self.__time_columns)
                continue
            if industry_number not in range(1, 32):
                result[date] = pd.Series(1.0, index=self.__time_columns)
            else:
                result[date] = self.__industry_ret['industry_' + str(industry_number)].loc[date]
        return pd.DataFrame(result).T

    def single_stock_factor_generator(self, code):
        ############################################
        # 以下是数据计算逻辑的部分，需要用户自定义 #
        # 计算数据时，最后应得到factor_data这个DataFrame，涵盖的时间段是self.start_date至self.end_date（前闭后闭）；
        # factor_data的因子值一列，应当以股票代码为列名；
        # 最后factor_data的索引，应当是8位整数的交易日；
        # 获取分钟数据应当用get_single_stock_minute_data2这个函数
        ############################################
        stock_minute_data = Dtk.get_single_stock_minute_data(code, self.__start_date_minus_n_2, self.end_date,
                                                             fill_nan=True,
                                                             append_pre_close=True, adj_type='NONE', drop_nan=False,
                                                             full_length_padding=True)
        factor_data = []
        data_check_df = stock_minute_data.dropna()  # 用于检查行情的完整性
        if stock_minute_data.columns.__len__() > 0 and data_check_df.__len__() > 0:
            close_price = (stock_minute_data['close'] / stock_minute_data['pre_close']).unstack()
            date_list = list(close_price.index)
            index_close = self.get_industry_data_in_code(code, date_list)
            for date in list(close_price.index):
                if not np.isnan(close_price.loc[date].values.sum()):
                    # linear=np.polyfit(close_price.loc[date].values,self.__index_close.loc[date].values,1)
                    # residual=self.__index_close.loc[date].values - (linear[0] * close_price.loc[date].values + linear[1])
                    close_ = close_price.loc[date]
                    index_close_ = index_close.loc[date]
                    residual = close_ - index_close_
                    adf = ts.adfuller(residual, 1)
                    result = adf[1] * 100
                    if residual.iloc[-1] < 0:
                        result = -result
                    factor_data.append(result)
                else:
                    factor_data.append(np.nan)
            #
            factor_data = pd.Series(factor_data, index=close_price.index)
            factor_data = factor_data.to_frame(code)
            factor_data = factor_data.loc[self.start_date: self.end_date]
        ########################################
        # 因子计算逻辑到此为止，以下勿随意变更 #
        ########################################
        else:  # 如取到的行情的DataFrame为空，则自造一个全为nan的DataFrame
            factor_data = self.generate_empty_df(code)
        return factor_data
