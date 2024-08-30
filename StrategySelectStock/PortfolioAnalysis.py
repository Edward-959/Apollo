# -*- coding: utf-8 -*-
"""
Created on 2019/2/25 19:09

@author: 006547
"""
import pandas as pd
import platform
from os import environ
import os
import DataAPI.DataToolkit as Dtk
import datetime as dt
import numpy as np
import pickle
from sklearn.preprocessing import QuantileTransformer


class PortfolioAnalysis:
    def __init__(self, signal_file_name, header):
        if platform.system() == "Windows":  # 云桌面环境运行是Windows
            absolutePath = ''
        elif os.system("nvidia-smi") == 0:
            absolutePath = "/data/user/Apollo/StrategySelectStock/" + 'Model_File_' + signal_file_name[7:] + '/daily_stock_pool_barra/'
        else:
            user_id = environ['USER_ID']
            absolutePath = "/app/data/" + user_id + "/Apollo/StrategySelectStock/" + 'Model_File_' + signal_file_name[7:] + '/daily_stock_pool_barra/'
        self.signal_path = absolutePath
        self.signal_file_name = signal_file_name
        self.header = header

        with open(absolutePath + header + ".pickle", 'rb') as f:
            portfolio_file = pickle.load(f)

        self.portfolio_file = portfolio_file

        self.date = sorted(list(portfolio_file.keys()))

        self.result = None

    def analysis(self):
        start_date = self.date[0]
        end_date = self.date[-1]
        complete_stock_list = Dtk.get_complete_stock_list(end_date=end_date)
        industry3_df = Dtk.get_panel_daily_info(complete_stock_list, start_date, end_date, 'industry3')
        index_300_weight_df = Dtk.get_panel_daily_info(complete_stock_list, start_date, end_date, 'index_weight_hs300')
        index_500_weight_df = Dtk.get_panel_daily_info(complete_stock_list, start_date, end_date, 'index_weight_zz500')

        valid_end_date = Dtk.get_n_days_off(end_date, 2)[-1]
        price_df = Dtk.get_panel_daily_pv_df(complete_stock_list, start_date, valid_end_date,
                                             pv_type='twap', adj_type='FORWARD')
        return_rate_df = price_df.shift(-1) / price_df - 1

        portfolio_return_in_industry = pd.DataFrame(index=self.date, columns=range(1, 32))
        hs300_return_in_industry = pd.DataFrame(index=self.date, columns=range(1, 32))
        zz500_return_in_industry = pd.DataFrame(index=self.date, columns=range(1, 32))

        for date in self.date:
            print('calculating ' + str(date) + ' portfolio')
            portfolio_in_date = self.portfolio_file[date]['Code']
            industry_in_date = industry3_df.loc[date, portfolio_in_date].values
            return_in_date = return_rate_df.loc[date, portfolio_in_date].values
            weight_in_date = self.portfolio_file[date]['Weight'].values
            for i in range(1, 32):
                return_in_i_date = return_in_date[industry_in_date == i]
                if return_in_i_date.__len__() > 0:
                    return_in_i_date = np.nan_to_num(return_in_i_date)
                    weight_in_i_date = np.nan_to_num(weight_in_date[industry_in_date == i])
                    weight_in_i_date = weight_in_i_date / sum(weight_in_i_date)
                    weight_return_in_i_date = sum(return_in_i_date * weight_in_i_date)
                else:
                    weight_return_in_i_date = 0
                portfolio_return_in_industry.loc[date, i] = weight_return_in_i_date

        for date in self.date:
            print('calculating ' + str(date) + ' hs300')
            hs300_in_date = index_300_weight_df.loc[date, :]
            hs300_in_date = hs300_in_date.index[hs300_in_date > 0]
            industry_in_date = industry3_df.loc[date, hs300_in_date].values
            return_in_date = return_rate_df.loc[date, hs300_in_date].values
            weight_in_date = index_300_weight_df.loc[date, hs300_in_date].values
            for i in range(1, 32):
                return_in_i_date = return_in_date[industry_in_date == i]
                if return_in_i_date.__len__() > 0:
                    return_in_i_date = np.nan_to_num(return_in_i_date)
                    weight_in_i_date = np.nan_to_num(weight_in_date[industry_in_date == i])
                    weight_in_i_date = weight_in_i_date / sum(weight_in_i_date)
                    weight_return_in_i_date = sum(return_in_i_date * weight_in_i_date)
                else:
                    weight_return_in_i_date = 0
                hs300_return_in_industry.loc[date, i] = weight_return_in_i_date

        for date in self.date:
            print('calculating ' + str(date) + ' zz500')
            zz500_in_date = index_500_weight_df.loc[date, :]
            zz500_in_date = zz500_in_date.index[zz500_in_date > 0]
            industry_in_date = industry3_df.loc[date, zz500_in_date].values
            return_in_date = return_rate_df.loc[date, zz500_in_date].values
            weight_in_date = index_500_weight_df.loc[date, zz500_in_date].values
            for i in range(1, 32):
                return_in_i_date = return_in_date[industry_in_date == i]
                if return_in_i_date.__len__() > 0:
                    return_in_i_date = np.nan_to_num(return_in_i_date)
                    weight_in_i_date = np.nan_to_num(weight_in_date[industry_in_date == i])
                    weight_in_i_date = weight_in_i_date / sum(weight_in_i_date)
                    weight_return_in_i_date = sum(return_in_i_date * weight_in_i_date)
                else:
                    weight_return_in_i_date = 0
                zz500_return_in_industry.loc[date, i] = weight_return_in_i_date

        excess_300_in_industry = portfolio_return_in_industry - hs300_return_in_industry
        excess_500_in_industry = portfolio_return_in_industry - zz500_return_in_industry

        writer = pd.ExcelWriter(self.signal_path + self.header + '.xlsx')
        excess_300_in_industry.to_excel(writer, sheet_name='excess_300')
        excess_500_in_industry.to_excel(writer, sheet_name='excess_500')
        portfolio_return_in_industry.to_excel(writer, sheet_name='portfolio')
        hs300_return_in_industry.to_excel(writer, sheet_name='hs300')
        zz500_return_in_industry.to_excel(writer, sheet_name='zz500')
        writer.save()


def main():
    signal_file_name = 'signal_EnsembleMeanModel-ThreeModel-excess500-pred5-trainAlphastock'
    header = "risk_1_cost_0.2_max_weight_{'normal': 0.01}_min_weight_None_return_True_style_{'All': [-0.05, 0.05]}_maxSN_200_minSN_None_hedge_barra_zz500"
    signal_analysis = PortfolioAnalysis(signal_file_name, header)
    signal_analysis.analysis()


if __name__ == "__main__":
    main()
