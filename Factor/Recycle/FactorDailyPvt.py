# -*- coding: utf-8 -*-
# @Time    : 2018/8/31 13:19
# @Author  : 011673
# @File    : FactorDailyPvt.py
import DataAPI.DataToolkit as Dtk
import pandas as pd
import platform
from DataUpdater.DailyFactorUpdater import DailyFactorUpdater
from Factor.TimeSeriesBase import TimeSeriesBase


def pvt(start_date, end_date, factor_file_dir_path, para, mode):
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
        N = self.para['N']
        stock_minute_close = original_data['close']
        stock_minute_volume = original_data['amt']
        ave_volume = stock_minute_volume.rolling(window=20, min_periods=1).mean()
        stock_minute_volume = stock_minute_volume / ave_volume
        temp = stock_minute_close.shift(N)
        temp.iloc[0] = stock_minute_close.iloc[0]
        temp = temp.fillna(method='ffill')
        stock_minute_close = (stock_minute_close - temp) / temp
        stock_minute_volume = stock_minute_volume.fillna(0)
        result = stock_minute_close * stock_minute_volume
        return self.return_cal_result({
            'pvt': result,
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
        'factor_name': 'pvt_10',
        'func_name': 'pvt',
        'module_name': 'FactorDailyPvt',
        'data_review_period': 10,
        'N': 10,
        'factor_list': ['pvt']
        },
        {
            'factor_name': 'pvt_15',
            'func_name': 'pvt',
            'module_name': 'FactorDailyPvt',
            'data_review_period': 15,
            'N': 15,
            'factor_list': ['pvt']
        },
        {
            'factor_name': 'pvt_20',
            'func_name': 'pvt',
            'module_name': 'FactorDailyPvt',
            'data_review_period': 20,
            'N': 20,
            'factor_list': ['pvt']
        }
    ]
    ####################################################
    for i_para in para_list:
        # function_name = i_para['func_name']
        # 如果写成工厂模式的话可以在这里载入预先在别的文件里写好的函数
        eval(i_para['func_name'])(istart_date_int, iend_date_int, save_dir, i_para, 0)  # 动态函数调用

if __name__ == '__main__':
    main()
