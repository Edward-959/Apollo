# -*- coding: utf-8 -*-
"""
Created on 2018/8/9 17:01

@author: 006547
"""
from ModelSystem.Model import Model
from DataAPI.FactorLoader import *
from DataAPI.DataToolkit import *
import datetime as dt
from sklearn import linear_model
import sklearn.preprocessing as sk_preprocessing
from ModelSystem.Tools import *


class ModelDemo(Model):
    def __init__(self, para_model, model_name, model_management):
        Model.__init__(self, para_model, model_name, model_management)
        model_management.register(self)
        self.code_list = []

    def train(self, date):  # 数据集都是np.array格式
        train_date = get_n_days_off(date, -3)[0]
        start = str(train_date) + ' 09:30:00'
        end = str(train_date) + ' 15:00:00'
        date_start = dt.datetime.strptime(start, '%Y%m%d %H:%M:%S')
        date_end = dt.datetime.strptime(end, '%Y%m%d %H:%M:%S')
        # date_start = convert_date_or_time_int_to_datetime(date)
        # date_end = convert_date_or_time_int_to_datetime(date)

        hs300_stock_code_list, hs300_stock_weight_list = get_index_component('000300.SH', train_date)
        #zz500_stock_code_list, zz500_stock_weight_list = get_index_component('000905.SH', train_date)

        #codes = hs300_stock_code_list + zz500_stock_code_list
        codes = hs300_stock_code_list
        self.code_list = codes
        factor_name = "F_M_Growth180"  # 云桌面S盘的例子
        factor = load_factor(factor_name, codes, start_date=date_start, end_date=date_end)
        tag_name = "TagMinHighGrowth242_20180601_20180630_fast"  # 云桌面S盘的例子
        tag_high = load_factor(tag_name, codes, start_date=date_start, end_date=date_end)
        tag_name = "TagMinLowGrowth242_20180601_20180630_fast"  # 云桌面S盘的例子
        tag_low = load_factor(tag_name, codes, start_date=date_start, end_date=date_end)

        x_all = factor.values
        x_all.resize((x_all.shape[0]*x_all.shape[1], 1))

        y_high = tag_high.values
        y_high.resize((y_high.shape[0] * y_high.shape[1], 1))

        y_low = tag_low.values
        y_low.resize((y_low.shape[0] * y_low.shape[1], 1))

        data_list_filtration, judge = remove_void_data([x_all, y_high, y_low])
        x_all_filtration = data_list_filtration[0]
        y_high_filtration = data_list_filtration[1]
        y_low_filtration = data_list_filtration[2]

        scaler = sk_preprocessing.MinMaxScaler(feature_range=(-3, 3)).fit(x_all_filtration)
        x_all_filtration_scale = scaler.transform(x_all_filtration)

        regr_high = linear_model.LinearRegression()
        regr_high.fit(x_all_filtration_scale, y_high_filtration)
        regr_low = linear_model.LinearRegression()
        regr_low.fit(x_all_filtration_scale, y_low_filtration)
        return {'model_high': regr_high, 'model_low': regr_low, 'scaler': scaler}

    def infer(self, date):
        model_date_keys = list(self.model_management.model_saved.keys())
        model_date = max(model_date_keys)
        regr_high = self.model_management.model_saved[model_date]['model_high']
        regr_low = self.model_management.model_saved[model_date]['model_low']
        scaler = self.model_management.model_saved[model_date]['scaler']

        start = str(date) + ' 09:30:00'
        end = str(date) + ' 15:00:00'
        date_start = dt.datetime.strptime(start, '%Y%m%d %H:%M:%S')
        date_end = dt.datetime.strptime(end, '%Y%m%d %H:%M:%S')
        # date_start = convert_date_or_time_int_to_datetime(date)
        # date_end = convert_date_or_time_int_to_datetime(date)

        hs300_stock_code_list, hs300_stock_weight_list = get_index_component('000300.SH', date)
        zz500_stock_code_list, zz500_stock_weight_list = get_index_component('000905.SH', date)

        #codes = hs300_stock_code_list + zz500_stock_code_list
        codes = hs300_stock_code_list
        factor_name = "F_M_Growth180"  # 云桌面S盘的例子
        factor = load_factor(factor_name, codes, start_date=date_start, end_date=date_end)
        tag_name = "TagMinHighGrowth242_20180601_20180630_fast"  # 云桌面S盘的例子
        tag_high = load_factor(tag_name, codes, start_date=date_start, end_date=date_end)
        tag_name = "TagMinLowGrowth242_20180601_20180630_fast"  # 云桌面S盘的例子
        tag_low = load_factor(tag_name, codes, start_date=date_start, end_date=date_end)

        code_list = np.repeat(np.array(factor.columns), factor.shape[0])
        timestamp_list = np.tile(np.array(factor.index), factor.shape[1])

        x_all = factor.values
        x_all.resize((x_all.shape[0]*x_all.shape[1], 1))

        y_high = tag_high.values
        y_high.resize((y_high.shape[0] * y_high.shape[1], 1))

        y_low = tag_low.values
        y_low.resize((y_low.shape[0] * y_low.shape[1], 1))

        data_list_filtration, judge = remove_void_data([x_all])
        x_all_filtration = data_list_filtration[0]
        # y_high_filtration = data_list_filtration[1]
        # y_low_filtration = data_list_filtration[2]

        x_all_filtration_scale = scaler.transform(x_all_filtration)

        y_high_predict = regr_high.predict(x_all_filtration_scale)
        y_low_predict = regr_low.predict(x_all_filtration_scale)

        code_list_filtration = code_list[judge == False]
        timestamp_list_filtration = timestamp_list[judge == False]

        data = {'Timestamp': timestamp_list_filtration, 'Code': code_list_filtration, 'PredictHigh': y_high_predict.flatten(), 'PredictLow': y_low_predict.flatten()}
        infer_result = pd.DataFrame(data)
        return {'infer_result': infer_result}

