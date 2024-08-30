# -*- coding: utf-8 -*-
# @Time    : 2018/8/31 14:14
# @Author  : 011673
# @File    : FactorDailyCoppockCurve.py
import DataAPI.DataToolkit as Dtk
import pandas as pd
import platform
from DataUpdater.DailyFactorUpdater import DailyFactorUpdater
from Factor.TimeSeriesBase import TimeSeriesBase


def fcc(start_date, end_date, factor_file_dir_path, para, mode):
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
        fast_lag = self.para['fast_lag']
        slow_lag = self.para['slow_lag']
        n = self.para['N']
        stock_minute_close = original_data['close']
        ##########################################
        temp = stock_minute_close.shift(fast_lag)
        temp.iloc[0] = stock_minute_close.iloc[0]
        temp = temp.fillna(method='ffill')
        fast_lag_close = (stock_minute_close - temp) / temp
        fast_lag_close.fillna(0)
        ########################################
        temp = stock_minute_close.shift(slow_lag)
        temp.iloc[0] = stock_minute_close.iloc[0]
        temp = temp.fillna(method='ffill')
        slow_lag_close = (stock_minute_close - temp) / temp
        slow_lag_close.fillna(0)
        ###########################################
        sum_value = slow_lag_close + fast_lag_close
        result = sum_value.rolling(window=n, min_periods=1).mean()
        result = result.fillna(method='ffill')
        return self.return_cal_result({
            'fcc': result,
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
        'factor_name': 'fcc_9_12_26',
        'func_name': 'fcc',
        'module_name': 'FactorDailyCoppockCurve',
        'data_review_period': 26,
        'N': 9,
        'fast_lag': 12,
        'slow_lag': 26,
        'factor_list': ['fcc']
    },
        {
            'factor_name': 'fcc_9_6_13',
            'func_name': 'fcc',
            'module_name': 'FactorDailyCoppockCurve',
            'data_review_period': 13,
            'N': 9,
            'fast_lag': 6,
            'slow_lag': 13,
            'factor_list': ['fcc']
        }
    ]
    ####################################################
    for i_para in para_list:
        # function_name = i_para['func_name']
        # 如果写成工厂模式的话可以在这里载入预先在别的文件里写好的函数
        eval(i_para['func_name'])(istart_date_int, iend_date_int, save_dir, i_para, 0)  # 动态函数调用


if __name__ == '__main__':
    main()
