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
from sklearn import linear_model
import sklearn.preprocessing as sk_preprocessing


class ModelRollingLinear(Model):
    def __init__(self, para_model, model_name, model_management):
        Model.__init__(self, para_model, model_name, model_management)
        model_management.register(self)
        self.code_list = []
        self.rolling_model = []
        test_pred_period = self.para_model['test_pred_period']
        train_lag = self.para_model['train_lag']
        if self.para_model['is_day_factor']:
            start_date = Dtk.get_n_days_off(self.trading_day[0], -test_pred_period-train_lag-2-(self.para_model['model_rolling_lag']-1)*train_lag)[0]
        else:
            start_date = get_n_days_off(self.trading_day[0], -test_pred_period-train_lag-1-(self.para_model['model_rolling_lag']-1)*train_lag)[0]
        self.alpha_universe = Dtk.get_panel_daily_info(self.complete_stock_list, start_date,
                                                       para_model["end_date"], para_model["universe"])

        self.original_day_factor_data_df = self.load_day_factor(start_date, para_model["end_date"],
                                                                outlier_filter=para_model["outlier_filter"],
                                                                z_score_standardizer=para_model["z_score_standardizer"],
                                                                neutralize=para_model["neutralize"])
        self.original_label_data_df = self.load_label(start_date, para_model["end_date"], label_type=self.para_model['test_tag'][0],
                                                      holding_period=test_pred_period)

    def train(self, date):
        date_orignal = date
        if not self.rolling_model:
            date = get_n_days_off(date, -(self.para_model['model_rolling_lag']-1)*self.para_model['train_lag']-1)[0]
        while date <= date_orignal:
            test_pred_period = self.para_model['test_pred_period']
            train_lag = self.para_model['train_lag']

            train_date_day_factor_end = get_n_days_off(date, -test_pred_period - 3)[0]
            train_date_day_factor_start = get_n_days_off(date, -test_pred_period - train_lag - 2)[0]
            # train_date_min_factor = get_n_days_off(date, -test_pred_period-1)[0]
            tag_date_end = get_n_days_off(date, -test_pred_period - 2)[0]
            tag_date_start = get_n_days_off(date, -test_pred_period - train_lag - 1)[0]

            date = get_n_days_off(date, self.para_model['train_lag'] + 1)[-1]

            start = str(train_date_day_factor_start) + ' 00:00:00'
            end = str(train_date_day_factor_end) + ' 23:59:59'
            start_tag = str(tag_date_start) + ' 00:00:00'
            end_tag = str(tag_date_end) + ' 23:59:59'
            date_start = dt.datetime.strptime(start, '%Y%m%d %H:%M:%S').timestamp()
            date_end = dt.datetime.strptime(end, '%Y%m%d %H:%M:%S').timestamp()
            tag_date_start = dt.datetime.strptime(start_tag, '%Y%m%d %H:%M:%S').timestamp()
            tag_date_end = dt.datetime.strptime(end_tag, '%Y%m%d %H:%M:%S').timestamp()
            # date_start = convert_date_or_time_int_to_datetime(date)
            # date_end = convert_date_or_time_int_to_datetime(date)

            alpha_universe_i_date = self.alpha_universe.loc[train_date_day_factor_start]
            alpha_universe_i_date = alpha_universe_i_date[alpha_universe_i_date > 0]
            alpha_universe_list_i_date = alpha_universe_i_date.index.tolist()
            self.code_list = alpha_universe_list_i_date

            day_factor_cell = []
            for day_factor_name in self.para_model['test_day_factor']:
                temp_factor = self.original_day_factor_data_df[day_factor_name].loc[date_start:date_end, self.code_list]
                day_factor_cell.append(temp_factor.values.flatten())

            temp_train_data = np.array(day_factor_cell).transpose([1, 0])

            temp_train_tag = self.original_label_data_df.loc[tag_date_start:tag_date_end, self.code_list].values.flatten()
            temp_train_tag.resize((temp_train_tag.__len__(), 1))

            data_list_filtration, judge = remove_void_data([temp_train_data, temp_train_tag])
            train_data = data_list_filtration[0]
            train_tag = data_list_filtration[1]

            if train_data.__len__() > 0:
                scaler = sk_preprocessing.StandardScaler().fit(train_data)
                train_data_scale = scaler.transform(train_data)
                train_data_scale[train_data_scale > 3] = 3
                train_data_scale[train_data_scale < -3] = -3

                model_linear = linear_model.LinearRegression()
                model_linear.fit(train_data_scale, train_tag)
            else:
                model_linear = self.rolling_model[-1]['model_linear']
                scaler = self.rolling_model[-1]['scaler']

            self.rolling_model.append({'model_linear': model_linear, 'scaler': scaler})

        beta_mat = []
        scaler_mean_mat = []
        scaler_std_mat = []
        for i in range(-1, -self.para_model['model_rolling_lag']-1, -1):
            model_linear = self.rolling_model[i]['model_linear']
            scaler = self.rolling_model[i]['scaler']
            if i == -1:
                beta_mat = np.hstack((model_linear.coef_, [model_linear.intercept_]))
                scaler_mean_mat = scaler.mean_.reshape(1, scaler.mean_.__len__())
                scaler_std_mat = scaler.scale_.reshape(1, scaler.mean_.__len__())
            else:
                scaler_mean = scaler.mean_.reshape(1, scaler.mean_.__len__())
                scaler_std = scaler.scale_.reshape(1, scaler.mean_.__len__())
                beta = np.hstack((model_linear.coef_, [model_linear.intercept_]))
                beta_mat = np.vstack((beta_mat, beta))
                scaler_mean_mat = np.vstack((scaler_mean_mat, scaler_mean))
                scaler_std_mat = np.vstack((scaler_std_mat, scaler_std))

        beta_average = np.mean(beta_mat, 0).reshape(beta_mat.shape[1], 1)
        scaler_mean_average = np.mean(scaler_mean_mat, 0).reshape(1, scaler_mean_mat.shape[1])
        scaler_std_average = np.mean(scaler_mean_mat, 0).reshape(1, scaler_mean_mat.shape[1])

        return {'beta_average': beta_average, 'scaler_mean_average': scaler_mean_average, 'scaler_std_average': scaler_std_average}

    def infer(self, date):
        model_date_keys = list(self.model_management.model_saved.keys())
        model_date = max(model_date_keys)
        beta_average = self.model_management.model_saved[model_date]['beta_average']
        scaler_mean_average = self.model_management.model_saved[model_date]['scaler_mean_average']
        scaler_std_average = self.model_management.model_saved[model_date]['scaler_std_average']

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

        predict_data_scale = (predict_data - scaler_mean_average)/scaler_std_average
        predict_data_scale[predict_data_scale > 3] = 3
        predict_data_scale[predict_data_scale < -3] = -3
        predict_data_scale = np.hstack((predict_data_scale, np.ones([predict_data_scale.shape[0], 1])))

        predict = np.dot(predict_data_scale, beta_average)
        code_list_filtration = np.array(self.code_list)[judge == False]

        infer_result = {'Code': code_list_filtration, 'predict': predict.flatten()}
        return {'infer_result': infer_result}
