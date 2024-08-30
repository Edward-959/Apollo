# -*- coding: utf-8 -*-
"""
Created on 2019/2/13 10:39

@author: 006547
"""
import pandas as pd
import platform
from os import environ
import os
import numpy as np
import json


class SimulationAnalysis:
    def __init__(self, signal_file_name, simulation_name):
        if platform.system() == "Windows":  # 云桌面环境运行是Windows
            simulation_path = "simulation/" + signal_file_name + '/'
        elif os.system("nvidia-smi") == 0:
            simulation_path = "/app/data/" + 'user' + "/Apollo/simulation/" + signal_file_name + '/'
        else:
            user_id = environ['USER_ID']
            simulation_path = "/app/data/" + user_id + "/Apollo/simulation/" + signal_file_name + '/'
        self.simulation_path = simulation_path
        self.simulation_name = simulation_name

        self.original_df = pd.read_csv(simulation_path + simulation_name + '.csv')

        self.max_drop = None
        self.max_drop_date_start = None
        self.max_drop_date_end = None
        self.sharp_ratio = None
        self.annual_return = None
        self.annual_volatility = None
        self.day_win_rate = None
        self.day_turnover = None
        self.sep_annual_return = None
        self.sep_annual_max_dd = None

        self.result = {}

    def analysis(self):
        # 计算最大回撤
        max_dd, date_start, date_end = self.cal_max_dd(self.original_df['net_value'].values[2:],
                                                       self.original_df['date'].values[2:])
        self.max_drop = max_dd
        self.max_drop_date_start = date_start
        self.max_drop_date_end = date_end

        self.sep_annual_max_dd = self.cal_sep_annual_max_dd(self.original_df['net_value'].values[2:],
                                                            self.original_df['date'].values[2:])
        # 计算夏普比率
        self.sharp_ratio = self.cal_sharp_ratio(self.original_df['pnl_rate'].values / 100)
        # 计算年化收益率
        self.annual_return = self.cal_annual_return(self.original_df['pnl_rate'].values / 100)
        # 计算年化波动率
        self.annual_volatility = self.cal_annual_volatility(self.original_df['pnl_rate'].values / 100)
        # 计算日胜率
        self.day_win_rate = self.cal_day_win_rate(self.original_df['pnl_rate'].values / 100)
        # 计算日均换手率
        self.day_turnover = self.cal_day_turnover(self.original_df['stock_sell_amount'].values[3:],
                                                  self.original_df['stock_mv'].values[3:])
        # 计算每年的年化收益率
        self.sep_annual_return = self.cal_sep_annual_return(self.original_df['pnl_rate'].values[2:] / 100,
                                                            self.original_df['date'].values[2:])

        self.result.update({'annual_return': [self.annual_return],
                            'annual_volatility': [self.annual_volatility],
                            'sharp_ratio': [self.sharp_ratio],
                            'max_drop': [self.max_drop],
                            'max_drop_date_start': [self.max_drop_date_start],
                            'max_drop_date_end': [self.max_drop_date_end],
                            'day_win_rate': [self.day_win_rate],
                            'day_turnover': [self.day_turnover],
                            })
        self.result.update(self.sep_annual_return)
        self.result.update(self.sep_annual_max_dd)
        for key in self.result.keys():
            print(str(key) + ': ', end='')
            print(self.result[key])

        with open(self.simulation_path + "simulation_analysis_" + self.simulation_name + ".json", "w") as f:
            json.dump(self.result, f)

    @staticmethod
    def cal_max_dd(x, date):
        max_i = 0
        min_i = 0

        max_unit_value = x[max_i]
        max_dd = 0

        for i in range(1, len(x)):
            if x[i] > max_unit_value:
                max_unit_value = x[i]
            dd = x[i] / max_unit_value - 1
            if dd < max_dd:
                max_dd = dd
                max_i = np.argmax(x[:i + 1])
                min_i = i
        date_start = date[max_i]
        date_end = date[min_i]
        return max_dd, date_start, date_end

    @staticmethod
    def cal_sep_annual_max_dd(x, date):
        result = {}
        i_start = 0
        i_end = 0
        for i in range(1, len(x)):
            if str(date[i])[:4] != str(date[i - 1])[:4]:
                # ret = np.mean(x[i_start:i_end+1])*250
                max_dd = SimulationAnalysis.cal_max_dd(x[i_start:i_end + 1], date[i_start:i_end + 1])
                result.update({str(date[i_start])[:4]+'max_drop': max_dd})
                i_start = i
                i_end = i
            elif i == len(x) - 1:
                # ret = np.mean(x[i_start:i_end+2])*250
                max_dd = SimulationAnalysis.cal_max_dd(x[i_start:i_end + 2], date[i_start:i_end + 2])
                result.update({str(date[i_start])[:4]+'max_drop': max_dd})
            else:
                i_end = i
        return result

    @staticmethod
    def cal_sharp_ratio(x):
        sharp = (np.mean(x) * 250) / (np.std(x) * (250 ** 0.5))
        return sharp

    @staticmethod
    def cal_annual_return(x):
        return (np.mean(x)+1) ** 250 - 1

    @staticmethod
    def cal_annual_volatility(x):
        return np.std(x) * (250 ** 0.5)

    @staticmethod
    def cal_day_win_rate(x):
        return sum(x > 0) / x.__len__()

    @staticmethod
    def cal_day_turnover(sell, stock_mv):
        return np.mean(sell / stock_mv)

    @staticmethod
    def cal_sep_annual_return(x, date):
        result = {}
        i_start = 0
        i_end = 0
        for i in range(1, len(x)):
            if str(date[i])[:4] != str(date[i - 1])[:4]:
                ret = (np.mean(x[i_start:i_end + 1])+1) ** 250 - 1
                result.update({date[i_start]: [ret]})
                i_start = i
                i_end = i
            elif i == len(x) - 1:
                ret = (np.mean(x[i_start:i_end + 2])+1) ** 250 - 1
                result.update({date[i_start]: [ret]})
            else:
                i_end = i
        return result


def main():
    signal_file_name = "signal_EnsembleMeanModel-ThreeModel-excess300-pred5-trainAlphastock"
    simulation_name = "simulation_" + "trade_cost_0.0012_hedge_index_300_risk_1_cost_0.2_max_weight_hs300_min_weight_None_return_True_style_{'All': [-0.2, 0.2]}_maxSN_200_minSN_None_hedge_barra_hs300"
    simulation_analysis = SimulationAnalysis(signal_file_name, simulation_name)
    simulation_analysis.analysis()
