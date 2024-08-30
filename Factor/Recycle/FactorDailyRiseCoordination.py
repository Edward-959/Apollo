# -*- coding: utf-8 -*-
# @Time    : 2018/9/3 17:17
# @Author  : 011673
# @File    : FactorDailyRiseCoordination.py
import DataAPI.DataToolkit as Dtk
import pandas as pd
import platform
from DataUpdater.DailyFactorUpdater import DailyFactorUpdater
from Factor.TimeSeriesBase import TimeSeriesBase
import copy
import numpy as np


def rc(start_date, end_date, factor_file_dir_path, para, mode):
    stock_list = Dtk.get_complete_stock_list()
    DailyFactorUpdater(mode, stock_list, start_date, end_date, factor_file_dir_path, para, FactorCal)


class FactorCal(TimeSeriesBase):
    """
    在cal函数中设置计算方式，其他都已经设置好了不需要再修改
    """

    def __init__(self, stock_list, start_date, end_date, save_path, para):
        TimeSeriesBase.__init__(self, stock_list, start_date, end_date, save_path, para)
        self.set_factor(self.para['factor_list'])
        for code in stock_list:
            temp = self.cal(self.get_data(code, 'code'))
            for key in temp:
                self.panel_data[key][code] = temp[key]
        for key in self.panel_data.keys():
            data = pd.DataFrame(self.panel_data[key])
            self.panel_data[key] = data[(data.index <= end_date) & (data.index >= start_date)]

    def cal(self, original_data):
        """
        :param original_data: 单个股票的K线数据，类型为DataFrame，列包括open, close, high, low, pre_close, amt, volume
        :return: self.return_cal_result（dic），其中dict是字典，key是需要计算的因子，值为时序的因子值(series)
        设置的参数可以在self.para里面寻找，预先在基类里写了一些方法（比如eam这种），需要详细计算的话可以去看基类
        """
        # =======================
        def cor_func(serie):
            sorted_value=copy.deepcopy(serie)
            sorted_value.sort()
            return np.corrcoef(serie,sorted_value)[0][1]

        N = self.para['N']
        stock_minute_close = original_data['close']
        close_list = stock_minute_close.rolling(window=N, min_periods=10)
        cor_value=close_list.apply(func=cor_func)
        cor_value=cor_value.fillna(0)
        temp = stock_minute_close.shift(1)
        temp.iloc[0] = stock_minute_close.iloc[0]
        last_close = (stock_minute_close - temp) / temp
        last_close.fillna(0)
        rcmu=cor_value*last_close
        return self.return_cal_result({
            'rc': cor_value,
            'rcmu':rcmu
        })


def main():
    ####################################################
    # 设置首次计算的参数
    if platform.system() == 'Windows':
        save_dir = "S:\Apollo\Factors"  # 保存的地址
    else:
        save_dir = "/app/data/006566/Apollo/Factors"  # 保存于XQuant的地址
    istart_date_int = 20180530
    iend_date_int = 20180730
    para_list = [{
        'factor_name': 'rise_coordination_20',
        'func_name': 'rc',
        'module_name': 'FactorDailyRiseCoordination',
        'data_review_period': 20,
        'N': 20,
        'factor_list': ['rc','rcmu']
    },
        {
            'factor_name': 'rise_coordination_30',
            'func_name': 'rc',
            'module_name': 'FactorDailyRiseCoordination',
            'data_review_period': 30,
            'N': 30,
            'factor_list': ['rc','rcmu']
        },
        {
            'factor_name': 'rise_coordination_40',
            'func_name': 'rc',
            'module_name': 'FactorDailyRiseCoordination',
            'data_review_period': 40,
            'N': 40,
            'factor_list': ['rc','rcmu']
        },
    ]
    ####################################################
    for i_para in para_list:
        # function_name = i_para['func_name']
        # 如果写成工厂模式的话可以在这里载入预先在别的文件里写好的函数
        eval(i_para['func_name'])(istart_date_int, iend_date_int, save_dir, i_para, 0)  # 动态函数调用

if __name__=='__main__':
    main()