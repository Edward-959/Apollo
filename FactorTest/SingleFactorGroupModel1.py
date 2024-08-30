# -*- coding: utf-8 -*-
"""
Created on 2018/8/9 17:01

@author: 006547
"""
from ModelSystem.Model import Model
from DataAPI.FactorLoader import *
from DataAPI.DataToolkit import *
import datetime as dt
import DataAPI.DataToolkit as Dtk
from ModelSystem.Tools import *


class SingleFactorGroupModel1(Model):
    def __init__(self, para_model, model_name, model_management):
        Model.__init__(self, para_model, model_name, model_management)
        model_management.register(self)
        self.code_list = []
        complete_stock_list = Dtk.get_complete_stock_list()
        if self.para_model['is_day_factor']:
            start_date = Dtk.get_n_days_off(self.trading_day[0], -2)[0]
        else:
            start_date = self.trading_day[0]
        self.alpha_universe = Dtk.get_panel_daily_info(complete_stock_list, start_date,
                                                       para_model["end_date"], para_model["universe"])

    def train(self, date):  # 数据集都是np.array格式
        pass
        return None

    def infer(self, i_date):
        test_factor = self.para_model['test_factor']
        group_num = self.para_model['group_num']
        is_day_factor = self.para_model['is_day_factor']
        if is_day_factor:
            i_date = Dtk.get_n_days_off(i_date, -2)[0]

        start = str(i_date) + ' 00:00:00'
        end = str(i_date) + ' 23:59:59'
        date_start = dt.datetime.strptime(start, '%Y%m%d %H:%M:%S')
        date_end = dt.datetime.strptime(end, '%Y%m%d %H:%M:%S')

        alpha_universe_i_date = self.alpha_universe.loc[i_date]
        alpha_universe_i_date = alpha_universe_i_date[alpha_universe_i_date > 0]
        alpha_universe_list_i_date = alpha_universe_i_date.index.tolist()

        if type(test_factor) == str:
            factor = load_factor(test_factor, alpha_universe_list_i_date, start_date=date_start, end_date=date_end)
            # stock_code_filtered_i_date = alpha_universe_i_date
        else:
            stock_code_of_test_factor = list(test_factor.columns)
            stock_code_filtered_i_date = list(
                set(stock_code_of_test_factor).intersection(set(alpha_universe_list_i_date)))
            factor = test_factor[stock_code_filtered_i_date]
            factor = factor[date_start.timestamp():date_end.timestamp()]

        factor = factor.dropna(1)
        num_stock = factor.shape[1]
        num_in_group = np.floor(num_stock / group_num)
        if factor.empty:
            return None

        factor_sorted = factor.transpose().sort_values(factor.index[0])
        infer_result = []
        for group in range(group_num):
            code_selected = list(factor_sorted.index[int(num_in_group*group):int(num_in_group*group+num_in_group)])
            factor_value = factor_sorted.iloc[int(num_in_group*group):int(num_in_group*group+num_in_group)].values
            data = {'Code': code_selected,
                    'FactorValue': factor_value.flatten(),
                    'Weight': np.repeat(1 / code_selected.__len__(), code_selected.__len__())}
            infer_result.append(pd.DataFrame(data))

        return {'infer_result': infer_result}
