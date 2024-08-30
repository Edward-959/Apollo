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
import pickle
import math


class ModelLinear(Model):
    def __init__(self, para_model, model_name, model_management):
        Model.__init__(self, para_model, model_name, model_management)
        model_management.register(self)
        self.code_list = []
        test_pred_period = self.para_model['test_pred_period']
        train_lag = self.para_model['train_lag']
        if self.para_model['is_day_factor']:
            start_date = Dtk.get_n_days_off(self.trading_day[0], -test_pred_period-train_lag-2)[0]
        else:
            start_date = get_n_days_off(self.trading_day[0], -test_pred_period-train_lag-1)[0]
        self.alpha_universe = Dtk.get_panel_daily_info(self.complete_stock_list, start_date,
                                                       para_model["end_date"], para_model["universe"])

        self.original_day_factor_data_df = self.load_day_factor(start_date, para_model["end_date"],
                                                                outlier_filter=para_model["outlier_filter"],
                                                                z_score_standardizer=para_model["z_score_standardizer"],
                                                                neutralize=para_model["neutralize"],
                                                                fill=para_model["fill"])
        self.original_label_data_df = self.load_label(start_date, para_model["end_date"], label_type=self.para_model['test_tag'][0], holding_period=test_pred_period)
        if self.para_model['hedge_index'] == 'index_500':
            self.original_predict_tag_df = self.load_label(start_date, para_model["end_date"], label_type='twap_excess_500', holding_period=test_pred_period)
        elif self.para_model['hedge_index'] == 'index_300':
            self.original_predict_tag_df = self.load_label(start_date, para_model["end_date"], label_type='twap_excess_300', holding_period=test_pred_period)

    def train(self, date):
        absolutePath = self.para_model['absolutePath']
        if not os.path.exists(absolutePath + 'ModelSaved/' + self.model_name + '_' + str(date) + '.pickle'):
            test_pred_period = self.para_model['test_pred_period']
            train_lag = self.para_model['train_lag']

            train_date_day_factor_end = get_n_days_off(date, -test_pred_period - 3)[0]
            train_date_day_factor_start = get_n_days_off(date, -test_pred_period - train_lag - 2)[0]
            # train_date_min_factor = get_n_days_off(date, -test_pred_period-1)[0]
            tag_date_end = get_n_days_off(date, -test_pred_period - 2)[0]
            tag_date_start = get_n_days_off(date, -test_pred_period - train_lag - 1)[0]

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

            day_factor_cell_train = []
            for day_factor_name in self.para_model['test_day_factor']:
                temp_factor = self.original_day_factor_data_df[day_factor_name].loc[date_start:date_end, self.code_list]
                day_factor_cell_train.append(temp_factor.values)
            temp_code = list(temp_factor.columns)
            temp_train_code = np.repeat(temp_code, temp_factor.shape[0])
            temp_train_data = np.array(day_factor_cell_train).transpose([2, 1, 0])
            temp_train_data = temp_train_data.reshape(
                [temp_train_code.__len__(), self.para_model['test_day_factor'].__len__()])

            temp_tag = self.original_label_data_df.loc[tag_date_start:tag_date_end, self.code_list].values
            temp_train_tag = temp_tag.transpose([1, 0])
            temp_train_tag = temp_train_tag.reshape([temp_train_code.__len__(), 1])

            data_list_filtration, judge = remove_void_data([temp_train_tag])
            trainData = temp_train_data[judge == False, :]
            trainSubTag = data_list_filtration[0]
            code_list_train = np.array(temp_train_code)[judge == False]

            if not os.path.exists(absolutePath):
                os.makedirs(absolutePath)
            with open(absolutePath + 'train_valid_data.pickle', 'wb') as f:
                pickle.dump((trainData, trainSubTag), f)

            if trainData.__len__() > 0:
                trainData = fill_void_data([trainData], fill='mean')
                scaler = sk_preprocessing.StandardScaler().fit(trainData[0])
                train_data_scale = scaler.transform(trainData[0])
                train_data_scale[train_data_scale > 3] = 3
                train_data_scale[train_data_scale < -3] = -3

                model_linear = linear_model.LinearRegression()
                model_linear.fit(train_data_scale, trainSubTag)
                predict_train = model_linear.predict(train_data_scale)
                train_IC = np.corrcoef(predict_train.flatten(), trainSubTag.flatten())[0, 1]

                print('train_IC %.2f' % (train_IC))
            else:
                print('No train data, use previous model')
                model_date_keys = list(self.model_management.model_saved.keys())
                model_date = max(model_date_keys)
                model_linear = self.model_management.model_saved[model_date]['model_linear']
                scaler = self.model_management.model_saved[model_date]['scaler']
            if not os.path.exists(absolutePath + 'ModelSaved'):
                os.makedirs(absolutePath + 'ModelSaved')
            with open(absolutePath + 'ModelSaved/' + self.model_name + '_' + str(date) + '.pickle', 'wb') as f:
                pickle.dump((model_linear, scaler), f)
        else:
            with open(absolutePath + 'ModelSaved/' + self.model_name + '_' + str(date) + '.pickle', 'rb') as f:
                model_linear, scaler = pickle.load(f)

        return {'model_linear': model_linear, 'scaler': scaler}

    def infer(self, date):
        model_date_keys = list(self.model_management.model_saved.keys())
        model_date = max(model_date_keys)
        model = self.model_management.model_saved[model_date]['model_linear']
        scaler = self.model_management.model_saved[model_date]['scaler']

        predict_date_day_factor_start = get_n_days_off(date, -2)[0]
        predict_date_day_factor_end = get_n_days_off(date, -2)[0]
        # train_date_min_factor = date

        start = str(predict_date_day_factor_start) + ' 00:00:00'
        end = str(predict_date_day_factor_end) + ' 23:59:59'
        start_tag = str(date) + ' 00:00:00'
        end_tag = str(date) + ' 23:59:59'
        date_start = dt.datetime.strptime(start, '%Y%m%d %H:%M:%S').timestamp()
        date_end = dt.datetime.strptime(end, '%Y%m%d %H:%M:%S').timestamp()
        # date_start = convert_date_or_time_int_to_datetime(date)
        # date_end = convert_date_or_time_int_to_datetime(date)
        tag_date_start = dt.datetime.strptime(start_tag, '%Y%m%d %H:%M:%S').timestamp()
        tag_date_end = dt.datetime.strptime(end_tag, '%Y%m%d %H:%M:%S').timestamp()

        alpha_universe_i_date = self.alpha_universe.loc[predict_date_day_factor_start]
        alpha_universe_i_date = alpha_universe_i_date[alpha_universe_i_date > 0]
        alpha_universe_list_i_date = alpha_universe_i_date.index.tolist()
        self.code_list = alpha_universe_list_i_date

        day_factor_cell = []
        for day_factor_name in self.para_model['test_day_factor']:
            temp_factor = self.original_day_factor_data_df[day_factor_name].loc[date_start:date_end, self.code_list]
            # temp_factor = temp_factor.rolling(5).mean()
            # temp_factor = temp_factor.iloc[-1, :]
            # day_factor_cell.append(temp_factor.values.flatten())
            # for i in range(5):
            #     day_factor_cell.append(temp_factor.iloc[i, :].values.flatten())
            day_factor_cell.append(temp_factor.values.flatten())

        temp_predict_data = np.array(day_factor_cell).transpose([1, 0])

        temp_predict_tag = self.original_predict_tag_df.loc[tag_date_start:tag_date_end,
                           self.code_list].values.flatten()
        temp_predict_tag.resize((temp_predict_tag.__len__(), 1))

        # data_list_filtration, judge = remove_void_data([temp_predict_tag])
        # predict_data = temp_predict_data[judge == False, :]
        # predict_tag = data_list_filtration[0]

        predict_data = temp_predict_data[:]
        code_list_filtration = np.array(self.code_list)

        if predict_data.__len__() > 0:
            predict_data = fill_void_data([predict_data], fill='mean')
            predict_data_scale = scaler.transform(predict_data[0])
            predict_data_scale[predict_data_scale > 3] = 3
            predict_data_scale[predict_data_scale < -3] = -3

            predict = model.predict(predict_data_scale)

            data_list_filtration, judge = remove_void_data([temp_predict_tag])
            predict_tag = data_list_filtration[0]
            predict2 = predict[judge == False]

            predict_IC = np.corrcoef(predict2.flatten(), predict_tag.flatten())[0, 1]
            predict_data_len = predict.__len__()

            top_index = np.argsort(-predict2)[:int(predict2.__len__() / 10)]
            top_return = np.mean(predict_tag[top_index])
            top_excess_return = top_return - np.mean(predict_tag.flatten())
            print('predict_IC %.2f, predict_data_len %d, top_excess_return %.5f' % (
            predict_IC, predict_data_len, top_excess_return))

            infer_result = {'Code': code_list_filtration, 'predict': predict.flatten(), 'predict2': predict2,
                            'predict_tag': predict_tag.flatten(), 'predict_IC': predict_IC,
                            'predict_data_len': predict_data_len, 'top_excess_return': top_excess_return}


        else:
            model_date_keys = list(self.model_management.infer_result.keys())
            model_date = max(model_date_keys)
            infer_result = self.model_management.infer_result[model_date]['infer_result']
        return {'infer_result': infer_result}
