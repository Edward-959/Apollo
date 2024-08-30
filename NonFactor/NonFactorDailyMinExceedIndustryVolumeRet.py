#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019/3/12 10:00
# @Author  : 011673
from Factor.DailyMinFactorBase import DailyMinFactorBase
import DataAPI.DataToolkit as Dtk
import pandas as pd
import DataUpdater.IndustryIndexUpdate as industry_update


def shift_amt_data(data: pd.DataFrame, minite_index):
    data_sum = data.sum(axis=1)
    data = data.rolling(window=5, min_periods=5, axis=1).sum()
    data = data.loc[:, minite_index]
    data = (data.T / data_sum).T
    return data


def load_industry_index():
    save_path = industry_update.get_path()
    result = {}
    store = pd.HDFStore(save_path + 'Industry.h5')
    for i in range(1, 32):
        result['industry_' + str(i)] = store.select('industry_' + str(i))
    return result


def shift_data(data: pd.DataFrame, minite_index):
    data = data.iloc[:, 1:]
    ret = 100 * data / data.shift(5, axis=1) - 100
    return ret.loc[:, minite_index]


class NonFactorDailyMinExceedIndustryVolumeRet(DailyMinFactorBase):
    def __init__(self, alpha_factor_root_path, stock_list, start_date_int, end_date_int, params):
        super().__init__(alpha_factor_root_path, stock_list, start_date_int, end_date_int)
        self.__start_date_minus_n_2 = Dtk.get_n_days_off(self.start_date, -15)[0]
        industry_ret = load_industry_index()
        minite = list(industry_ret['industry_1'].columns)
        self.__minite_index = []
        for i in minite:
            if i % 10 == 0:
                self.__minite_index.append(i)
        self.__minite_index = self.__minite_index[1:]
        for industry_number in industry_ret.keys():
            industry_ret[industry_number] = shift_data(industry_ret[industry_number], self.__minite_index)
        self.__industry_ret = industry_ret
        self.__industry = Dtk.get_panel_daily_info(self.stock_list, self.__start_date_minus_n_2, self.end_date,
                                                   'industry3')

    def get_industry_data_in_code(self, code, date_list):
        result = {}
        for date in date_list:
            industry_number = self.__industry.loc[date, code]
            try:
                industry_number = int(industry_number)
            except:
                result[date] = pd.Series(0.0, index=self.__minite_index)
                continue
            if industry_number not in range(1, 32):
                result[date] = pd.Series(0.0, index=self.__minite_index)
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
        stock_minute_data = Dtk.get_single_stock_minute_data2(code, self.__start_date_minus_n_2, self.end_date,
                                                              fill_nan=True, append_pre_close=True, adj_type='NONE',
                                                              drop_nan=False, full_length_padding=True)
        data_check_df = stock_minute_data.dropna()  # 用于检查行情的完整性
        if stock_minute_data.columns.__len__() > 0 and data_check_df.__len__() > 0:
            amt = stock_minute_data['amt'].unstack()
            amt = shift_amt_data(amt, self.__minite_index)
            close_price = (stock_minute_data['close'] / stock_minute_data['pre_close']).unstack()
            date_list = list(close_price.index)
            industry_data = self.get_industry_data_in_code(code, date_list)
            close_ret = shift_data(close_price, self.__minite_index)
            factor_data = close_ret * industry_data
            factor_data[factor_data > 0] = 0
            factor_data = factor_data / industry_data
            factor_data = factor_data * amt
            factor_data = factor_data.sum(axis=1)
            factor_data = factor_data.to_frame(code)
            factor_data = factor_data.loc[self.start_date: self.end_date]
        ########################################
        # 因子计算逻辑到此为止，以下勿随意变更 #
        ########################################
        else:  # 如取到的行情的DataFrame为空，则自造一个全为nan的DataFrame
            factor_data = self.generate_empty_df(code)
        return factor_data
