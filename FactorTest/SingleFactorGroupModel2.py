# -*- coding: utf-8 -*-
"""
Created on 2018/8/9 17:01

@author: 006547
"""
from ModelSystem.Model import Model
from DataAPI.FactorLoader import *
import datetime as dt
import DataAPI.DataToolkit as Dtk
from ModelSystem.Tools import *


class SingleFactorGroupModel2(Model):
    index_component_to_weight = {"index_300": 'index_weight_hs300',
                                 'index_500': 'index_weight_zz500',
                                 'index_50': 'index_weight_sh50'}

    def __init__(self, para_model, model_name, model_management):
        Model.__init__(self, para_model, model_name, model_management)
        model_management.register(self)
        complete_stock_list = Dtk.get_complete_stock_list()
        if self.para_model['is_day_factor']:
            start_date = Dtk.get_n_days_off(self.trading_day[0], -2)[0]
        else:
            start_date = self.trading_day[0]
        self.hedge_code_df = Dtk.get_panel_daily_info(complete_stock_list,
                                                      start_date,
                                                      para_model["end_date"],
                                                      self.index_component_to_weight.get(para_model["hedge_index"]))
        self.alpha_universe = Dtk.get_panel_daily_info(complete_stock_list, start_date,
                                                       para_model["end_date"], para_model["universe"])
        self.stocks_industry_df = Dtk.get_panel_daily_info(complete_stock_list, start_date,
                                                           para_model["end_date"], 'industry3')

    def train(self, i_date):  # 数据集都是np.array格式
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
            stock_code_filtered_i_date = alpha_universe_list_i_date
        else:
            stock_code_of_test_factor = list(test_factor.columns)
            stock_code_filtered_i_date = list(
                set(stock_code_of_test_factor).intersection(set(alpha_universe_list_i_date)))
            factor = test_factor[stock_code_filtered_i_date]
            factor = factor[date_start.timestamp():date_end.timestamp()]

        stock_code_filtered_i_date = np.array(stock_code_filtered_i_date)[pd.isna(factor).values.flatten() == False].tolist()
        factor = factor.dropna(1)


        ########################################
        # 如果当日因子数据没有,则返回None
        if factor.empty:
            return None
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
        for i in range(1, int(max(hedge_index_industry_list)+1)):
            industry_weight.append(sum(hedge_index_stock_weight_list[hedge_index_industry_list == i]))

        industry_weight = np.array(industry_weight) / sum(industry_weight)

        codes_filt_industry_df = stock_industry_series[stock_code_filtered_i_date]
        group_signal = []
        for group in range(group_num):
            infer_result = pd.DataFrame(columns=('Code', 'FactorValue', 'Industry', 'Weight'))
            for i in range(1, industry_weight.__len__() + 1):
                code_in_i = codes_filt_industry_df[codes_filt_industry_df == i].index.tolist()
                if code_in_i.__len__() > 0:
                    factor_sorted = factor[code_in_i].transpose().sort_values(factor[code_in_i].index[0])
                    num_stock = code_in_i.__len__()
                    num_in_group = num_stock / group_num
                    code_selected = list(factor_sorted.index[int(np.floor(num_in_group*group)):
                                                             int(np.ceil(num_in_group*group+num_in_group))])
                    factor_value = factor_sorted.iloc[int(np.floor(num_in_group*group)):
                                                      int(np.ceil(num_in_group*group+num_in_group))].values
                    infer_result_in_industry \
                        = pd.DataFrame({'Code': code_selected,
                                        'FactorValue': factor_value.flatten(),
                                        'Industry': np.repeat(np.float(i), code_selected.__len__()),
                                        'Weight': np.repeat(industry_weight[i-1]/code_selected.__len__(),
                                                            code_selected.__len__())})
                    infer_result = infer_result.append(infer_result_in_industry, ignore_index=True)
            group_signal.append(infer_result)
        return {'infer_result': group_signal}
