# -*- coding: utf-8 -*-
# @Time    : 2018/8/29 16:08
# @Author  : 011673
# @File    : macd.py
import DataAPI.DataToolkit as Dtk
import pandas as pd
import platform
from DataUpdater.DailyFactorUpdater import DailyFactorUpdater
from Factor.TimeSeriesBase import TimeSeriesBase


def macd(start_date, end_date, factor_file_dir_path, para):
    stock_list = Dtk.get_complete_stock_list()[0:10]
    if __name__ == '__main__':
        mode = 0
    else:
        mode = 1
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
        n = self.para['N']
        fast_lag = self.para['fast_lag']
        slow_lag = self.para['slow_lag']
        fast_ema_close = []
        slow_ema_close = []
        diff_list = []
        for date in original_data.index:
            data = original_data[original_data.index <= date]
            temp = min(n, data.index.__len__())
            ma = data['close'].iloc[-temp:].mean()
            close = data['close'].iloc[-1]
            fast_ema_close.append(self.ema(fast_lag, close, fast_ema_close))
            slow_ema_close.append(self.ema(slow_lag, close, slow_ema_close))
            diff = fast_ema_close[-1] - slow_ema_close[-1]
            dea = self.ema(n, diff, diff_list)
            diff_list.append(diff)
            macd_value = (diff - dea) * 2
            original_data.loc[date, 'MA'] = ma
            original_data.loc[date, 'DIFF'] = diff
            original_data.loc[date, 'DEA'] = dea
            original_data.loc[date, 'MACD'] = macd_value
        return self.return_cal_result({
            'MA': original_data['MA'],
            'MACD': original_data['MACD'],
            'DEA': original_data['DEA'],
            'DIFF': original_data['DIFF']
        })


if __name__ == '__main__':
    ####################################################
    # 设置首次计算的参数
    if platform.system() == 'Windows':
        save_dir = "S:\Apollo\Factors"  # 保存的地址
    else:
        save_dir = "/app/data/006566/Apollo/Factors"  # 保存于XQuant的地址
    istart_date_int = 20180601
    iend_date_int = 20180803
    para_list = [{
        'factor_name': 'macd_9_12_26',
        'func_name': 'macd',
        'module_name': 'FactorDailymacd',
        'N': 9,
        'fast_lag': 12,
        'slow_lag': 26,
        'factor_list': ['MA', 'DIFF', 'DEA', 'MACD']
    }]
    ####################################################
    for i_para in para_list:
        function_name = i_para['func_name']
        # 如果写成工厂模式的话可以在这里载入预先在别的文件里写好的函数
        eval(i_para['func_name'])(istart_date_int, iend_date_int, save_dir, i_para)  # 动态函数调用
