# -*- coding: utf-8 -*-
"""
Created on 2018/11/12 9:43

@author: 006547
"""
import pickle
import DataAPI.DataToolkit as Dtk
import numpy as np
import pandas as pd
import logging
import platform
from Backtest.PortfolioBackTester import PortfolioBackTester


class Optimizer:
    index_component_to_weight = {"index_300": 'index_weight_hs300',
                                 'index_500': 'index_weight_zz500',
                                 'index_50': 'index_weight_sh50'}

    def __init__(self, signal_file_name: str=..., industry_neutral: bool=True, hedge_index: str='index_500', percent_selected: float=0.1):
        self.__signal_file_name = signal_file_name
        self.__industry_neutral = industry_neutral
        self.__hedge_index = hedge_index
        self.__percent_selected = percent_selected
        if platform.system() == 'Windows':
            with open(self.__signal_file_name + '.pickle', 'rb') as f:
                self.__signal_file = pickle.load(f)
        else:
            from xquant.pyfile import Pyfile
            py = Pyfile()
            with py.open(self.__signal_file_name + '.pickle', 'rb') as f:
                self.__signal_file = pickle.load(f)

        complete_stock_list = Dtk.get_complete_stock_list()
        model_date_keys = list(self.__signal_file.keys())
        self.__model_date_keys = sorted(model_date_keys)

        start_date = min(model_date_keys)
        end_date = max(model_date_keys)

        all_stock_list = []
        for i_date in self.__model_date_keys:
            all_stock_list += self.__signal_file[i_date]['infer_result']['Code'].tolist()
        all_stock_list = list(set(all_stock_list))
        df_open = Dtk.get_panel_daily_pv_df(all_stock_list, start_date, end_date, pv_type="open")
        df_pre_close = Dtk.get_panel_daily_pv_df(all_stock_list, start_date, end_date, pv_type="pre_close")
        df_volume = Dtk.get_panel_daily_pv_df(all_stock_list, start_date, end_date, pv_type="volume")

        df_volume_0 = df_volume == 0
        df_raising10per = df_pre_close.mul(1.1)
        df_raising10per = df_raising10per.round(2) == df_open

        df_untradeable = df_raising10per | df_volume_0
        for i_date in self.__model_date_keys:
            temp_stock_list = self.__signal_file[i_date]['infer_result']['Code'].tolist()
            temp_pred_list = self.__signal_file[i_date]['infer_result']['predict'].tolist()
            temp_untradeable = np.array(list(df_untradeable.columns))[df_untradeable.loc[i_date, :].values == True]
            temp_tradeable = list(filter(lambda _x: not _x[0] in temp_untradeable, zip(temp_stock_list, temp_pred_list)))
            tradeable_stock_list = np.array(list(map(lambda _x: _x[0], temp_tradeable)))
            tradeable_predict = np.array(list(map(lambda _x: _x[1], temp_tradeable)))
            self.__signal_file[i_date]['infer_result']['Code'] = tradeable_stock_list
            self.__signal_file[i_date]['infer_result']['predict'] = tradeable_predict

        self.hedge_code_df = Dtk.get_panel_daily_info(complete_stock_list,
                                                      start_date, end_date,
                                                      self.index_component_to_weight.get(self.__hedge_index))
        self.stocks_industry_df = Dtk.get_panel_daily_info(complete_stock_list, start_date,
                                                           end_date, 'industry3')
        self.__pre_infer_result = []

    def launch(self):
        print('start optimizing')
        result = {}
        if self.__industry_neutral:
            for i_date in self.__model_date_keys:
                print(i_date)
                stock_code_filtered_i_date = self.__signal_file[i_date]['infer_result']['Code']
                factor = self.__signal_file[i_date]['infer_result']['predict']
                factor = factor.reshape([1, factor.__len__()])
                factor = pd.DataFrame(factor, columns=stock_code_filtered_i_date)
                if factor.empty:
                    result.update({i_date: self.__pre_infer_result})
                    continue
                #######################################
                hedge_index_stock_code_series = self.hedge_code_df.loc[i_date]
                hedge_index_stock_code_series = hedge_index_stock_code_series[hedge_index_stock_code_series > 0]
                hedge_index_stock_weight_list = hedge_index_stock_code_series.tolist()
                hedge_index_stock_weight_list = np.array(hedge_index_stock_weight_list)
                stock_industry_series = self.stocks_industry_df.loc[i_date]
                stock_industry_series = stock_industry_series[stock_industry_series > 0]
                hedge_index_industry_series = stock_industry_series[list(hedge_index_stock_code_series.index)]
                hedge_index_industry_series = hedge_index_industry_series.tolist()
                hedge_index_industry_list = np.array(hedge_index_industry_series)

                industry_weight = []
                for i in range(1, int(max(hedge_index_industry_list) + 1)):
                    industry_weight.append(sum(hedge_index_stock_weight_list[hedge_index_industry_list == i]))

                industry_weight = np.array(industry_weight) / sum(industry_weight)

                codes_filt_industry_df = stock_industry_series[stock_code_filtered_i_date]
                infer_result = pd.DataFrame(columns=('Code', 'FactorValue', 'Industry', 'Weight'))
                for i in range(1, industry_weight.__len__() + 1):
                    code_in_i = codes_filt_industry_df[codes_filt_industry_df == i].index.tolist()
                    if code_in_i.__len__() > 0:
                        factor_sorted = factor[code_in_i].transpose().sort_values(factor[code_in_i].index[0], axis=0, ascending=0)
                        num_stock = code_in_i.__len__()
                        num_in_group = max(int(num_stock * self.__percent_selected), 1)
                        code_selected = list(factor_sorted.index[0:num_in_group])
                        factor_value = factor_sorted.iloc[0:num_in_group].values
                        infer_result_in_industry \
                            = pd.DataFrame({'Code': code_selected,
                                            'FactorValue': factor_value.flatten(),
                                            'Industry': np.repeat(np.float(i), code_selected.__len__()),
                                            'Weight': np.repeat(industry_weight[i - 1] / code_selected.__len__(),
                                                                code_selected.__len__())})
                        infer_result = infer_result.append(infer_result_in_industry, ignore_index=True)
                self.__pre_infer_result = infer_result
                result.update({i_date: infer_result})
        else:
            for i_date in self.__model_date_keys:
                print(i_date)
                stock_code_filtered_i_date = self.__signal_file[i_date]['infer_result']['Code']
                factor = self.__signal_file[i_date]['infer_result']['predict']
                factor = factor.reshape([1, factor.__len__()])
                factor = pd.DataFrame(factor, columns=stock_code_filtered_i_date)
                if factor.empty:
                    result.update({i_date: self.__pre_infer_result})
                    continue

                factor_sorted = factor.transpose().sort_values(factor.index[0], axis=0, ascending=0)
                num_stock = factor_sorted.shape[0]
                num_in_group = max(int(num_stock * self.__percent_selected), 1)
                code_selected = list(factor_sorted.index[0:num_in_group])
                factor_value = factor_sorted.iloc[0:num_in_group].values
                infer_result = pd.DataFrame({'Code': code_selected, 'FactorValue': factor_value.flatten(),
                                             'Weight': np.repeat(1 / code_selected.__len__(), code_selected.__len__())})
                self.__pre_infer_result = infer_result
                result.update({i_date: infer_result})

        print('finish optimizing')
        return result


if __name__ == "__main__":
    signal_file_name = 'result_single_Factor_Group_Model_2'
    need_hedge = True
    hedge_index = 'index_500'
    cost_ratio = 0.004
    logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.INFO)

    optimizer = Optimizer(signal_file_name=signal_file_name, industry_neutral=True, hedge_index=hedge_index, percent_selected=0.1)
    daily_stock_pool = optimizer.launch()

    portfolio_back_tester = PortfolioBackTester(daily_stock_pool,
                                                cost_ratio=cost_ratio,
                                                group_test_need_hedge=need_hedge,
                                                group_test_hedge_index=hedge_index)
    portfolio_back_tester.run_test()
    csv_result = portfolio_back_tester.get_result()
    pdx = pd.DataFrame(csv_result)
    pdx.to_csv("result{}.csv")
