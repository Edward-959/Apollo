# -*- coding: utf-8 -*-
# @Time    : 2018/8/30 15:41
# @Author  : 011673
# @File    : FactorTimeSeries.pymport DataAPI.DataToolkit as Dtk
# import pandas as pd
import DataAPI.DataToolkit as Dtk
import xarray
import numpy as np


class TimeSeriesBase:
    def __init__(self, stock_list, start_date, end_date, save_path, para):
        self.panel_data = {}
        self.save_path = save_path
        self.stock_list = stock_list
        self.time_series_data = {}
        self.start_data = start_date
        self.end_data = end_date
        self.para = para
        self.data = {}
        item_list = ['amt', 'close', 'high', 'low', 'open', 'pre_close', 'volume']
        cal_start_data = Dtk.get_n_days_off(start_date, -self.para['data_review_period']-1)[0]
        num_value = None
        date_list = None
        columns_list = None
        for item in item_list:
            self.data[item] = Dtk.get_panel_daily_pv_df(stock_list, cal_start_data, end_date, item, 'BACKWARD2')
            self.data[item] = self.data[item].loc[:, stock_list]
            if num_value is None:
                num_value = [self.data[item].values]
                date_list = list(self.data[item].index)
                columns_list = list(self.data[item].columns)
            else:
                num_value.append(self.data[item].values)
        num_value = np.array(num_value)
        self.data = xarray.DataArray(num_value, dims=['items', 'date', 'symbol'],
                                     coords=[item_list, date_list, columns_list])

    def get_data(self, name, data_type):
        if data_type == 'code':
            return self.data.sel(symbol=name).to_pandas().T
        elif data_type == 'time':
            return self.data.sel(date=name).to_pandas()
        elif data_type == 'item':
            return self.data.sel(item=name).to_pandas()
        else:
            raise Exception('name_type_error')

    def set_factor(self, factor_list):
        for factor_name in factor_list:
            self.panel_data[self.para['factor_name'] + '_' + factor_name] = {}

    @staticmethod
    def ema(para_lag, data_now, ema):
        alpha = 2 / (para_lag + 1)
        if len(ema) == 0:
            ema_pre = data_now
        else:
            ema_pre = ema[-1]
        ema_new = data_now * alpha + ema_pre * (1 - alpha)
        return ema_new

    def return_cal_result(self, dic):
        result = {}
        for key in dic:
            result[self.para['factor_name'] + '_' + key] = dic[key]
        return result

    def get_factor(self):
        result = {}
        factor_data_dic = self.panel_data
        for key in factor_data_dic:
            factor_file_name = key
            factor_data = factor_data_dic[key]
            # # 行情中获取原始的索引是20180829这种整形，保存因子文件时我们要转成timestamp；reset_index后，索引会变成普通的列'index'
            factor_data = factor_data.reset_index()
            date_list = Dtk.convert_date_or_time_int_to_datetime(factor_data['date'].tolist())
            timestamp_list = [i_date.timestamp() for i_date in date_list]
            factor_data['timestamp'] = timestamp_list
            # 将timestamp设为索引
            factor_data = factor_data.set_index(['timestamp'])
            # factor_data仅保留股票列表的列，删除其他无关的列
            factor_data = factor_data[self.stock_list].copy()
            result[factor_file_name] = factor_data
        return result
