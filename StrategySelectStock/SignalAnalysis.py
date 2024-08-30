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


class SignalAnalysis:
    def __init__(self, signal_file_name):
        if platform.system() == "Windows":  # 云桌面环境运行是Windows
            absolutePath = ''
        elif os.system("nvidia-smi") == 0:
            absolutePath = "/data/user/Apollo/StrategySelectStock/" + 'Model_File_' + signal_file_name[7:] + '/'
        else:
            user_id = environ['USER_ID']
            absolutePath = "/app/data/" + user_id + "/Apollo/StrategySelectStock/" + 'Model_File_' + signal_file_name[7:] + '/'
        self.signal_path = absolutePath
        self.signal_file_name = signal_file_name

        with open(absolutePath + signal_file_name + ".pickle", 'rb') as f:
            signal_file = pickle.load(f)

        self.signal_file = signal_file

        self.date = sorted(list(signal_file.keys()))

        self.group_num = 10

        self.result = None

    def analysis(self):
        # top_return_list = []
        # top_excess_return_list = []
        group_percentile = {}
        group_return = {}
        group_excess_return = {}

        for i in range(self.group_num):
            group_percentile.update({'group_percentile_'+str(i+1): []})
            group_return.update({'group_return_' + str(i + 1): []})
            group_excess_return.update({'group_excess_return_' + str(i + 1): []})

        #  计算分组的收益
        for date in self.date:
            predict = self.signal_file[date]['infer_result']['predict2'].flatten()
            predict_tag = self.signal_file[date]['infer_result']['predict_tag'].flatten()
            # top_index = np.argsort(-predict)[:int(predict.__len__()/10)]
            #
            # top_return = np.mean(predict_tag[top_index])
            # top_excess_return = top_return - np.mean(predict_tag.flatten())
            #
            # top_return_list.append(top_return)
            # top_excess_return_list.append(top_excess_return)

            for i in range(self.group_num):
                index = np.argsort(-predict)[int(predict.__len__() / 10) * i:int(predict.__len__() / 10 * (i + 1))]

                return_in_group = np.mean(predict_tag[index])
                excess_return_in_group = return_in_group - np.mean(predict_tag.flatten())

                group_return['group_return_' + str(i + 1)].append(return_in_group)
                group_excess_return['group_excess_return_' + str(i + 1)].append(excess_return_in_group)

                qt = QuantileTransformer()
                qt.fit_transform(predict_tag.reshape(-1, 1))
                group_percentile['group_percentile_' + str(i + 1)].append(qt.transform(return_in_group)[0, 0])

        result = {}
        result.update(group_percentile)
        result.update(group_return)
        result.update(group_excess_return)
        result = pd.DataFrame(result, index=self.date)

        self.result = result

        for key in result.keys():
            print(key, end=' ')
            print(np.mean(self.result[key]))

        pdx = pd.DataFrame(self.result)
        pdx.to_csv(self.signal_path + "signal_analysis_" + self.signal_file_name[7:] + ".csv")

        # path_dir = r"D:\006566\Desktop\x1\FinIndustry"
        # signal_file_name = "signal_rdf_lab5_re10_tdl120_val0.01_lb0.005_ub_20190327neutral_cubic_excess.pickle"
        # full_path = os.path.join(path_dir, signal_file_name)
        #
        holding_period = 5  # 展望未来n天收益率

        group_num = 5  # 分几组分析，因为已经细分到行业、股票数量不多，因此建议不要设太大
        #
        # # 载入预测值pickle文件
        # with open(full_path, 'rb') as f:
        #     signal = pickle.load(f)

        inf_date_list = list(self.signal_file.keys())
        inf_date_list.sort()

        start_date = inf_date_list[0]  # 也可自己指定起、止日期
        end_date = inf_date_list[-1]
        valid_end_date = Dtk.get_n_days_off(end_date, holding_period + 1)[-1]

        # 将预测值转为DataFrame格式
        signal_df = pd.DataFrame()
        print('Converting signal to signal_df')
        t1 = dt.datetime.now()
        for i_date in inf_date_list:
            inf_i_date_pd = pd.DataFrame(self.signal_file[i_date]['infer_result']['predict'],
                                         index=self.signal_file[i_date]['infer_result']['Code'], columns=[i_date])
            inf_i_date_pd = inf_i_date_pd.T
            signal_df = pd.concat([signal_df, inf_i_date_pd], sort=False)
        t2 = dt.datetime.now()
        print('Signal converted, and it cost ', t2 - t1)

        complete_stock_list = Dtk.get_complete_stock_list()
        industry3_df = Dtk.get_panel_daily_info(complete_stock_list, start_date, end_date, 'industry3')
        data_df_deal_price = Dtk.get_panel_daily_pv_df(complete_stock_list, start_date, valid_end_date, pv_type='twap',
                                                       adj_type='FORWARD')
        return_twap_df = data_df_deal_price.shift(-holding_period) / data_df_deal_price - 1  # 计算标签
        return_twap_df = return_twap_df.reindex(index=inf_date_list)

        # 针对每一行业逐个循环、分析
        for x in range(31):
            specific_industry = x + 1
            #  21: '银行',  29: '证券Ⅱ', 30: '保险Ⅱ', 31: '信托及其他'
            output_file_name = "ind_" + str(specific_industry) + ".xlsx"
            industry_analysis_dir = "industry_analysis_" + str(start_date) + "_" + str(end_date)
            output_full_path = os.path.join(self.signal_path, industry_analysis_dir, output_file_name)

            # 仅保留曾是该industry的股票
            specific_ind_df = industry3_df.copy()
            specific_ind_df[:] = np.nan
            specific_ind_df[industry3_df == specific_industry] = specific_industry
            specific_ind_df = specific_ind_df.dropna(axis=1, how="all")

            signal_df2 = signal_df.reindex(index=specific_ind_df.index, columns=specific_ind_df.columns)
            return_twap_df2 = return_twap_df.reindex(index=specific_ind_df.index, columns=specific_ind_df.columns)

            # 因为个股可能出现过改行业的情况，这里仅保留是该行业时的信号和标签
            signal_df2 = signal_df2 * specific_ind_df / specific_ind_df
            return_twap_df2 = return_twap_df2 * specific_ind_df / specific_ind_df

            signal_df2 = signal_df2 * return_twap_df2 / return_twap_df2

            signal_rank_df = signal_df2.rank(axis=1, ascending=False)  # 将信号强度降序排列
            return_twap_rank_df = return_twap_df2.rank(axis=1, ascending=False)  # 将标签（收益率）降序排列

            signal_rank_array = signal_rank_df.values
            return_twap_rank_array = return_twap_rank_df.values

            result = {}
            for i in range(group_num):
                result.update({'group_quantile_' + str(i + 1): []})

            for i_date in range(signal_rank_df.__len__()):  # 逐日循环
                signal_rank_i_date = signal_rank_array[i_date]
                signal_rank_i = signal_rank_i_date[~np.isnan(signal_rank_i_date)]
                return_rank_i_date = return_twap_rank_array[i_date]
                return_rank_i = return_rank_i_date[~np.isnan(return_rank_i_date)]
                for i in range(group_num):  # 逐组循环
                    index = np.argsort(signal_rank_i)[
                            int(signal_rank_i.__len__() / group_num) * i:int(
                                signal_rank_i.__len__() / group_num * (i + 1))]
                    result['group_quantile_' + str(i + 1)].append(np.mean(return_rank_i[index]))

            signal_quantile_pd = pd.DataFrame(result, index=signal_rank_df.index)

            print(signal_quantile_pd.mean(axis=0))
            print(signal_quantile_pd.std(axis=0))

            if not os.path.exists(os.path.join(self.signal_path, industry_analysis_dir)):
                os.makedirs(os.path.join(self.signal_path, industry_analysis_dir))

            with pd.ExcelWriter(output_full_path) as writer:
                signal_quantile_pd.to_excel(writer, sheet_name='signal_quantile')
                signal_rank_df.to_excel(writer, sheet_name='signal_rank')
                return_twap_rank_df.to_excel(writer, sheet_name='return_rank')
                signal_df2.to_excel(writer, sheet_name='signal')
                return_twap_df2.to_excel(writer, sheet_name='return')


def main():
    signal_file_name = 'signal_XGBoostModel-FactorList_89-hold1-excess500-pred5-bestPara-fill0'
    signal_analysis = SignalAnalysis(signal_file_name)
    signal_analysis.analysis()


if __name__ == "__main__":
    main()
