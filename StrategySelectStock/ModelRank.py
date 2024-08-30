# -*- coding: utf-8 -*-
"""
Created on 2018/8/9 17:01

@author: 006547
"""
from ModelSystem.Model import Model
from DataAPI.DataToolkit import *
import datetime as dt
import DataAPI.DataToolkit as Dtk
from ModelSystem.Tools import *
# import sklearn.preprocessing as sk_preprocessing


class ModelRank(Model):
    def __init__(self, para_model, model_name, model_management):
        Model.__init__(self, para_model, model_name, model_management)
        model_management.register(self)
        self.code_list = []
        if self.para_model['is_day_factor']:
            start_date = Dtk.get_n_days_off(self.trading_day[0], -2)[0]
        else:
            start_date = get_n_days_off(self.trading_day[0], -1)[0]
        self.alpha_universe = Dtk.get_panel_daily_info(self.complete_stock_list, start_date,
                                                       para_model["end_date"], para_model["universe"])
        self.original_day_factor_data_df = self.load_day_factor(start_date, para_model["end_date"],
                                                                outlier_filter=para_model["outlier_filter"],
                                                                z_score_standardizer=para_model["z_score_standardizer"],
                                                                neutralize=para_model["neutralize"])

    def train(self, date):  # 数据集都是np.array格式
        pass

    def infer(self, date):
        predict_date_day_factor = get_n_days_off(date, -2)[0]
        # train_date_min_factor = date

        start = str(predict_date_day_factor) + ' 00:00:00'
        end = str(predict_date_day_factor) + ' 23:59:59'
        date_start = dt.datetime.strptime(start, '%Y%m%d %H:%M:%S').timestamp()
        date_end = dt.datetime.strptime(end, '%Y%m%d %H:%M:%S').timestamp()
        # date_start = convert_date_or_time_int_to_datetime(date)
        # date_end = convert_date_or_time_int_to_datetime(date)

        alpha_universe_i_date = self.alpha_universe.loc[predict_date_day_factor]
        alpha_universe_i_date = alpha_universe_i_date[alpha_universe_i_date > 0]
        alpha_universe_list_i_date = alpha_universe_i_date.index.tolist()
        self.code_list = alpha_universe_list_i_date

        day_factor_cell = []
        for day_factor_name in self.para_model['test_day_factor']:
            temp_factor = self.original_day_factor_data_df[day_factor_name].loc[date_start:date_end, self.code_list]
            day_factor_cell.append(temp_factor.values.flatten())

        temp_predict_data = np.array(day_factor_cell).transpose([1, 0])
        data_list_filtration, judge = remove_void_data([temp_predict_data])
        predict_data = data_list_filtration[0]

        predict = np.sum(predict_data * np.tile(self.para_model['test_day_factor_direction'], [predict_data.shape[0], 1]), 1)
        code_list_filtration = np.array(self.code_list)[judge == False]

        infer_result = {'Code': code_list_filtration, 'predict': predict.flatten()}
        return {'infer_result': infer_result}
