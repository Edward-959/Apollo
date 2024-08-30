"""
updated on 2018/12/19 将分组测试的结果逐年呈现; self.__nav_series_annually_stat的收益率计算由复利改为单利
updated on 2018/12/20 用group_rank统计分组测试的结果
updated on 2019/1/16, 17 新增以MktMedian和MktMean为基准，评价top组的绩效；新增超额收益月胜率、日胜率的统计
updated on 2019/3/31, 新增行业内分组效果统计
updated on 2019/4/45, 将一些可复用的静态方法剥离到HelperFunctions以减少本函数的篇幅； 在universe内以市值对因子进行3等分，
                      输出3个子集上top组和bottom组的收益率均值在universe子集上的分位数
"""

import matplotlib

matplotlib.use('Agg')  # Generate images without having a window appear; must be imported before pylab is imported

import DataAPI.DataToolkit as Dtk
import DataAPI.FactorTestloader
import pandas as pd
import sys
import os
from matplotlib.pylab import *
import datetime as dt
from reportlab.lib.enums import TA_JUSTIFY
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Image, Table
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
import matplotlib.pyplot as plt
import numpy as np
import time
from platform import system
import json
import uuid
from Utils.HelperFunctions import factor_distribution_calc, outlier_filter, z_score_standardizer, factor_neutralizer, \
    equally_wt_fast_nav, fast_long_short_nav, nav_series_annually_stat
from Utils.PlotFunctions import plot_group_bar2, plot_series, plot_one_series
from ModelSystem.Tools import remove_void_data
from sklearn.preprocessing import QuantileTransformer


if system() == "Windows":
    report_address = sys.path[0]  # 保存pdf报告的地址
    top_group_path_universe = "S:\\Apollo\\Top_group_nav_df_alpha_universe\\"  # 存放top组每日超额收益的矩阵
    top_group_path_index_800 = "S:\\Apollo\\Top_group_nav_df_index_800\\"
else:
    user_id = os.environ['USER_ID']
    report_address = "/app/data/" + user_id + "/Apollo/"
    if not os.path.exists(report_address):
        os.mkdir(report_address)
    top_group_path_universe = "/app/data/666889/Apollo/Top_group_nav_df_alpha_universe/"
    top_group_path_index_800 = "/app/data/666889/Apollo/Top_group_nav_df_index_800/"


class SingleFactorTest:
    def __init__(self, factor_name, start_date, end_date, is_day_factor=True, holding_period=3, group_num=20,
                 label_type='twap', universe='alpha_universe', neutral_factor_set={'size', 'industry3'},
                 outlier_filtering_method="MAD", stock_cost_rate=0, industry_analysis_group_num=5,
                 factor_path="S:\\Apollo\\AlphaFactors\\"):
        # 初始设置
        query_trade_date_list = Dtk.get_trading_day(start_date, end_date)
        self.factor_name = factor_name
        self.is_day_factor = is_day_factor
        self.start_date_orignal = query_trade_date_list[0]
        if self.is_day_factor:
            self.start_date = Dtk.get_n_days_off(query_trade_date_list[0], -2)[0]
        else:
            self.start_date = query_trade_date_list[0]
        self.end_date = query_trade_date_list[-1]
        self.holding_period = holding_period
        self.label_type = label_type
        self.universe = universe
        self.group_num = group_num
        self.neutral_factor_set = neutral_factor_set
        self.stock_cost_rate = stock_cost_rate
        self.outlier_filter_method = outlier_filtering_method
        self.factor_path = factor_path  # 原始因子的路径
        self.benchmark_list = ["000300.SH", "000905.SH", "MktMedian", "MktMean"]
        self.industry_analysis_group_num = industry_analysis_group_num  # 行业内分组测试的组数

        # 中间变量和最终结果
        self.complete_stock_list = Dtk.get_complete_stock_list()
        self.original_factor_data_df = None  # original factor dataframe -- won't modified it
        self.stock_universe_df = None
        self.label_data = None
        self.factor_test_report = {}
        self.factor_neutralized_test_report = {}
        self.group_nav_list = []  # 快速分组测试（不对冲）每组的回测净值
        self.group_total_annualized_return_dict = {}  # 快速分组测试（不对冲）每组的年化收益
        self.daily_return_median = pd.DataFrame()
        self.daily_return_mean = pd.DataFrame()
        self.industry_group_analysis_result = pd.DataFrame()
        self.worst_grouping_industries_list = []  # 分组效果最差的行业（第1组最差）
        self.poor_grouping_industries_list = []  # 分组效果比较差的行业（第1组倒数第2差）
        # Top组相对基准的超额收益净值(nav)序列，有4个key: "000300.SH" , "000905.SH", "MktMean"和"MktMedian"
        self.top_group_hedge_nav_dict = {}
        self.top_group_excess_return_each_year = {}  # Top组相对基准每年的超额收益
        self.top_group_excess_return_each_month = {}  # Top组相对基准每月的超额收益
        self.top_group_monthly_winning_pct_stat = {}  # Top组相对基准超额收益月胜率统计，4个key
        self.top_group_daily_winning_pct_stat = {}  # Top组相对基准超额收益日胜率统计，4个key
        self.long_short_nav = None  # Long-short净值
        self.long_short_return_each_year = {}  # 每年Long-short的收益率
        self.report_timestamp = dt.datetime.now()
        self.factor_avg_turnover_rate = None  # 平均换手率
        self.factor_stat_output = {}  # 最终结果放在里面
        self.each_year_group_return_dict = {}  # 分组测试每年的收益率
        self.group_return_rank_coef = {}  # 分组测试收益率与组号的相关性
        self.corr_information = {}  # 储存与现在因子的相关性信息
        self.size_analysis_result = None  # 因子市值分析结果

    def load_factor(self):
        start_date_datetime = Dtk.convert_date_or_time_int_to_datetime(self.start_date)
        end_date_datetime = Dtk.convert_date_or_time_int_to_datetime(self.end_date)
        self.original_factor_data_df = DataAPI.FactorTestloader.load_factor(self.factor_name, self.complete_stock_list,
                                                                            start_date_datetime, end_date_datetime,
                                                                            self.factor_path)

    def load_label(self):
        if self.label_type == 'coda':
            valid_end_date = Dtk.get_n_days_off(self.end_date, self.holding_period + 2)[-1]
            data_df_deal_price = Dtk.get_panel_daily_pv_df(self.complete_stock_list, self.start_date, valid_end_date,
                                                           pv_type='twp_coda', adj_type='FORWARD')
            return_rate_df = data_df_deal_price.shift(-self.holding_period) / data_df_deal_price - 1  # 计算收益率
            # factor的index是timestamp, 而非8位数的日期，这里做转化，以便后续可比
            return_rate_df = Dtk.convert_df_index_type(return_rate_df, 'date_int', 'timestamp')
            daily_return_df = data_df_deal_price / data_df_deal_price.shift(1) - 1
            stock_universe_df = Dtk.convert_df_index_type(self.stock_universe_df, 'timestamp', 'date_int')
            daily_return_df_filtered = daily_return_df.mul(stock_universe_df).div(stock_universe_df)
            self.daily_return_median = daily_return_df_filtered.median(axis=1)
            self.daily_return_mean = daily_return_df_filtered.mean(axis=1)
            return return_rate_df
        elif self.label_type == 'vwap':
            valid_end_date = Dtk.get_n_days_off(self.end_date, self.holding_period + 2)[-1]
            data_df_amt = Dtk.get_panel_daily_pv_df(self.complete_stock_list, self.start_date, valid_end_date,
                                                    pv_type='amt', adj_type='NONE')
            data_df_volume = Dtk.get_panel_daily_pv_df(self.complete_stock_list, self.start_date, valid_end_date,
                                                       pv_type='volume', adj_type='NONE')
            data_vwap = data_df_amt / data_df_volume  # 计算vwap
            adj_df = Dtk.get_panel_daily_info(self.complete_stock_list, self.start_date, valid_end_date, 'adjfactor')
            data_vwap = data_vwap * adj_df  # 计算后复权的vwap
            return_rate_df = data_vwap.shift(-self.holding_period) / data_vwap - 1  # 计算收益率
            # factor的index是timestamp, 而非8位数的日期，这里做转化，以便后续可比
            return_rate_df = Dtk.convert_df_index_type(return_rate_df, 'date_int', 'timestamp')
            daily_return_df = data_vwap / data_vwap.shift(1) - 1
            stock_universe_df = Dtk.convert_df_index_type(self.stock_universe_df, 'timestamp', 'date_int')
            daily_return_df_filtered = daily_return_df.mul(stock_universe_df).div(stock_universe_df)
            self.daily_return_median = daily_return_df_filtered.median(axis=1)
            self.daily_return_mean = daily_return_df_filtered.mean(axis=1)
            return return_rate_df
        elif self.label_type == 'twap':
            valid_end_date = Dtk.get_n_days_off(self.end_date, self.holding_period + 2)[-1]
            data_df_deal_price = Dtk.get_panel_daily_pv_df(self.complete_stock_list, self.start_date, valid_end_date,
                                                           pv_type='twap', adj_type='FORWARD')
            return_rate_df = data_df_deal_price.shift(-self.holding_period) / data_df_deal_price - 1  # 计算收益率
            # factor的index是timestamp, 而非8位数的日期，这里做转化，以便后续可比
            return_rate_df = Dtk.convert_df_index_type(return_rate_df, 'date_int', 'timestamp')
            daily_return_df = data_df_deal_price / data_df_deal_price.shift(1) - 1
            stock_universe_df = Dtk.convert_df_index_type(self.stock_universe_df, 'timestamp', 'date_int')
            daily_return_df_filtered = daily_return_df.mul(stock_universe_df).div(stock_universe_df)
            self.daily_return_median = daily_return_df_filtered.median(axis=1)
            self.daily_return_mean = daily_return_df_filtered.mean(axis=1)
            return return_rate_df

    @staticmethod
    def ic_stat_calc(ic_array, ic_name):
        ic_array = ic_array.dropna()
        ic_mean = np.mean(ic_array)
        ic_std = np.std(ic_array)
        icir = ic_mean / ic_std * np.sqrt(244)
        abs_ic = np.abs(ic_array)
        ic_greater_than_0p02_pct = abs_ic[abs_ic > 0.02].__len__() / abs_ic.__len__()

        def autocorr(x, t=1):
            return np.corrcoef(x[0:len(x) - t], x[t:len(x)])[0, 1]

        ic_auto_corr_1 = autocorr(ic_array.values, 1)
        ic_auto_corr_2 = autocorr(ic_array.values, 2)
        ic_auto_corr_3 = autocorr(ic_array.values, 3)

        ic_df = ic_array.to_frame()
        cumsum_ic_df = ic_df.cumsum()
        cumsum_ic_df = Dtk.convert_df_index_type(cumsum_ic_df, 'timestamp', 'date_int')

        index_date = list(cumsum_ic_df.index)
        date_year_list = [i // 10000 for i in index_date]
        year_list = list(set(date_year_list))
        year_list.sort()
        year_begin_idx = {}  # 记录每年首日在index_date中的位置索引
        year_idx = 0
        year_begin_idx.update({year_list[year_idx]: 0})
        year_end_idx = {}  # 记录每年末日在index_date中的位置索引
        for j, i_date in enumerate(date_year_list):
            if date_year_list[j] > year_list[year_idx]:
                year_end_idx.update({year_list[year_idx]: j - 1})
                year_idx += 1
                year_begin_idx.update({year_list[year_idx]: j})
            if j == date_year_list.__len__() - 1:
                year_end_idx.update({year_list[year_idx]: j})
        year_dates_count = {}  # 记录回测期间每年的交易日天数
        for i_year in year_begin_idx.keys():
            year_dates_count.update({i_year: year_end_idx[i_year] - year_begin_idx[i_year] + 1})
        ic_each_year = {}
        if year_dates_count[year_list[0]] < 30:  # 如果第1年的交易日小于30天，那么第1年的IC就没有计算的必要
            for j, i_year in enumerate(year_begin_idx.keys()):
                if j > 0:
                    ic_each_year.update({"IC_mean" + str(i_year): ((cumsum_ic_df.iloc[year_end_idx[i_year]] -
                                                                    cumsum_ic_df.iloc[year_end_idx[i_year - 1]]) /
                                        year_dates_count[i_year]).values[0]})
        else:
            for j, i_year in enumerate(year_begin_idx.keys()):
                if j == 0:
                    ic_each_year.update({"IC_mean" + str(i_year): ((cumsum_ic_df.iloc[year_end_idx[i_year]] -
                                                                    cumsum_ic_df.iloc[year_begin_idx[i_year]]) /
                                        year_dates_count[i_year]).values[0]})
                else:
                    ic_each_year.update({"IC_mean" + str(i_year): ((cumsum_ic_df.iloc[year_end_idx[i_year]] -
                                                                    cumsum_ic_df.iloc[year_end_idx[i_year - 1]]) /
                                        year_dates_count[i_year]).values[0]})

        ic_stat_value_list = [ic_mean, ic_std, icir, ic_greater_than_0p02_pct, ic_auto_corr_1, ic_auto_corr_2,
                              ic_auto_corr_3]
        for ic_mean_key in ic_each_year.keys():
            ic_stat_value_list.append(ic_each_year[ic_mean_key])
        ic_stat_df_index_list = ['IC_mean', 'IC_std', 'ICIR', '|IC|>0.02_pct', 'IC_AutoCorr_1', 'IC_AutoCorr_2',
                                 'IC_AutoCorr_3']
        for ic_mean_key in ic_each_year.keys():
            ic_stat_df_index_list.append(ic_mean_key)
        ic_stat_df = pd.DataFrame(ic_stat_value_list, index=ic_stat_df_index_list, columns=[ic_name])
        return ic_stat_df

    def corr_old_factors(self):
        # 计算与已入库所有因子的相关性
        if self.universe == 'alpha_universe':
            top_group_filename = os.listdir(top_group_path_universe)[0]
            with open(top_group_path_universe + top_group_filename, 'rb') as c:
                hedged_df = pd.read_csv(c, index_col=0)
            hedged_nav_series = self.top_group_hedge_nav_dict['000905.SH']
        else:
            top_group_filename = os.listdir(top_group_path_index_800)[0]
            with open(top_group_path_index_800 + top_group_filename, 'rb') as c:
                hedged_df = pd.read_csv(c, index_col=0)
            hedged_nav_series = self.top_group_hedge_nav_dict['000300.SH']
        if self.factor_name in hedged_df.columns.tolist():
            hedged_df = hedged_df.drop(columns=[self.factor_name])
        hedged_nav_dayret_series = hedged_nav_series - hedged_nav_series.shift(1)
        corr_series = hedged_df.loc[self.start_date:self.end_date].corrwith(
            hedged_nav_dayret_series.loc[self.start_date:self.end_date])
        corr_series_abs = corr_series.abs()
        max_corr_factor_id = corr_series_abs.nlargest(5)
        max_hedged_nav_truevalue = corr_series[max_corr_factor_id.index]
        max_hedged_nav_truevalue = max_hedged_nav_truevalue.round(3)
        self.corr_information.update({'max_corr_factor': max_hedged_nav_truevalue.index.values})
        self.corr_information.update({'max_corr_value': max_hedged_nav_truevalue.values})


    def size_analysis(self):
        # 将因子值在universe内按市值分为3块（小、中、大），在每块内再按因子值分self.group_num/2组，
        # 计算每块内top组和bottom组的标签（收益率）在其相应块内的分位数
        group_num2 = int(self.group_num / 2)
        group_percentile_size_1 = {}
        group_percentile_size_2 = {}
        group_percentile_size_3 = {}
        for i in range(10):
            group_percentile_size_1.update({'group_percentile_size_1_group_' + str(i + 1): []})
            group_percentile_size_2.update({'group_percentile_size_2_group_' + str(i + 1): []})
            group_percentile_size_3.update({'group_percentile_size_3_group_' + str(i + 1): []})

        factor_value_df = self.factor_neutralized_test_report["factor_data"].copy()
        factor_value_df = Dtk.convert_df_index_type(factor_value_df, 'timestamp', 'date_int')
        factor_value_df = factor_value_df.shift(1).iloc[1:]

        # 如IC为负，则需要把因子值调转一下符号，以便后续分析
        if self.factor_neutralized_test_report["IC_cumsum"].mean() < 0:
            factor_value_df = factor_value_df.mul(-1)

        label_df = Dtk.convert_df_index_type(self.label_data, 'timestamp', 'date_int')
        label_df = label_df.reindex(index=factor_value_df.index, columns=factor_value_df.columns)

        mkt_cap_df = Dtk.get_panel_daily_info(self.complete_stock_list, self.start_date, self.end_date, 'mkt_cap_ard')
        mkt_cap_df = mkt_cap_df.reindex(index=factor_value_df.index, columns=factor_value_df.columns)

        for j, i_date in enumerate(factor_value_df.index):
            factor_array = factor_value_df.loc[i_date].values
            label_array = label_df.loc[i_date].values
            mkt_cap_in_date = mkt_cap_df.loc[i_date].values

            data_list_filtration, judge = remove_void_data([label_array.reshape([-1, 1])])
            predict_tag = data_list_filtration[0].flatten()
            mkt_cap_in_date = mkt_cap_in_date[~judge]
            predict = factor_array[~judge]

            # 按市值分3块（升序）
            index_sorted = np.argsort(mkt_cap_in_date)
            index_size_1 = index_sorted[:int(index_sorted.__len__() / 3)]
            index_size_2 = index_sorted[int(index_sorted.__len__() / 3):int(index_sorted.__len__() / 3 * 2)]
            index_size_3 = index_sorted[int(index_sorted.__len__() / 3 * 2):index_sorted.__len__()]

            for i in range(group_num2):
                predict_size_1 = predict[index_size_1]
                predict_tag_size_1 = predict_tag[index_size_1]
                index = np.argsort(-predict_size_1)[
                            int(predict_size_1.__len__() / group_num2) * i:int(predict_size_1.__len__() / group_num2
                                                                               * (i + 1))]
                return_in_group = np.mean(predict_tag_size_1[index])
                qt = QuantileTransformer()
                qt.fit_transform(predict_tag_size_1.reshape(-1, 1))
                group_percentile_size_1['group_percentile_size_1_group_' + str(i + 1)].append(
                    qt.transform(return_in_group)[0, 0])

                predict_size_2 = predict[index_size_2]
                predict_tag_size_2 = predict_tag[index_size_2]
                index = np.argsort(-predict_size_2)[
                            int(predict_size_2.__len__() / group_num2) * i:int(predict_size_2.__len__() / group_num2
                                                                               * (i + 1))]
                return_in_group = np.mean(predict_tag_size_2[index])
                qt = QuantileTransformer()
                qt.fit_transform(predict_tag_size_2.reshape(-1, 1))
                group_percentile_size_2['group_percentile_size_2_group_' + str(i + 1)].append(
                    qt.transform(return_in_group)[0, 0])

                predict_size_3 = predict[index_size_3]
                predict_tag_size_3 = predict_tag[index_size_3]
                index = np.argsort(-predict_size_3)[
                            int(predict_size_3.__len__() / group_num2) * i:int(predict_size_3.__len__() / group_num2
                                                                               * (i + 1))]
                return_in_group = np.mean(predict_tag_size_3[index])
                qt = QuantileTransformer()
                qt.fit_transform(predict_tag_size_3.reshape(-1, 1))
                group_percentile_size_3['group_percentile_size_3_group_' + str(i + 1)].append(
                    qt.transform(return_in_group)[0, 0])

        result = {}
        result.update(group_percentile_size_1)
        result.update(group_percentile_size_2)
        result.update(group_percentile_size_3)
        result = pd.DataFrame(result, index=mkt_cap_df.index)

        result2 = {}
        result2.update({'group_pct_size_1_group_1': result['group_percentile_size_1_group_1'].mean()})
        result2.update({'group_pct_size_1_group_' + str(group_num2): result[
            'group_percentile_size_1_group_' + str(group_num2)].mean()})
        result2.update({'group_pct_size_2_group_1': result['group_percentile_size_2_group_1'].mean()})
        result2.update({'group_pct_size_2_group_' + str(group_num2): result[
            'group_percentile_size_2_group_' + str(group_num2)].mean()})
        result2.update({'group_pct_size_3_group_1': result['group_percentile_size_3_group_1'].mean()})
        result2.update({'group_pct_size_3_group_' + str(group_num2): result[
            'group_percentile_size_3_group_' + str(group_num2)].mean()})
        self.size_analysis_result = result2


    def industry_group_analysis(self):
        # 行业内分组效果统计
        industry3_df = Dtk.get_panel_daily_info(self.complete_stock_list, self.start_date, self.end_date, 'industry3')
        factor_value_df = self.factor_neutralized_test_report["factor_data"].copy()
        factor_value_df = Dtk.convert_df_index_type(factor_value_df, 'timestamp', 'date_int')
        factor_value_df = factor_value_df.shift(1).iloc[1:]
        label_df = self.label_data.copy()
        label_df = Dtk.convert_df_index_type(label_df, 'timestamp', 'date_int')
        label_df = label_df.iloc[1:]

        factor_industry_analysis = pd.DataFrame()
        for specific_industry in range(1, 32):
            # 仅保留曾是该industry的股票
            specific_ind_df = industry3_df.copy()
            specific_ind_df[:] = np.nan
            specific_ind_df[industry3_df == specific_industry] = specific_industry
            specific_ind_df = specific_ind_df.dropna(axis=1, how="all")

            factor_value_df2 = factor_value_df.reindex(index=specific_ind_df.index, columns=specific_ind_df.columns)
            label_df2 = label_df.reindex(index=specific_ind_df.index, columns=specific_ind_df.columns)

            # 因为个股可能出现过改行业的情况，这里仅保留是该行业时的信号和标签
            factor_value_df2 = factor_value_df2 * specific_ind_df / specific_ind_df
            label_df2 = label_df2 * specific_ind_df / specific_ind_df
            factor_value_df2 = factor_value_df2 * label_df2 / label_df2

            if self.factor_neutralized_test_report["IC_cumsum"].mean() > 0:  # 如IC为正，则因子值越大、收益率越高
                factor_value_rank = factor_value_df2.rank(axis=1, ascending=False)  # 将信号强度降序排列
            else:  # 如IC为负，则因子值越小、收益率越高
                factor_value_rank = factor_value_df2.rank(axis=1, ascending=True)  # 将信号强度升序排列
            label_rank = label_df2.rank(axis=1, ascending=False)  # 将标签（收益率）降序排列

            factor_rank_array = factor_value_rank.values
            label_rank_array = label_rank.values

            temp_result = {}
            for i in range(self.industry_analysis_group_num):
                temp_result.update({'group_quantile_' + str(i + 1): []})

            for i_date in range(factor_value_rank.__len__()):  # 逐日循环
                factor_rank_i_date = factor_rank_array[i_date]
                factor_rank_i = factor_rank_i_date[~np.isnan(factor_rank_i_date)]
                label_rank_i_date = label_rank_array[i_date]
                label_rank_i = label_rank_i_date[~np.isnan(label_rank_i_date)]
                for i in range(self.industry_analysis_group_num):  # 逐组循环
                    index = np.argsort(factor_rank_i)[
                            int(factor_rank_i.__len__() / self.industry_analysis_group_num) * i:int(
                                factor_rank_i.__len__() / self.industry_analysis_group_num * (i + 1))]
                    temp_result['group_quantile_' + str(i + 1)].append(np.mean(label_rank_i[index]))

            factor_industry_quantile_df = pd.DataFrame(temp_result, index=factor_value_rank.index)
            factor_industry_quantile_df2 = factor_industry_quantile_df.mean(axis=0)
            factor_industry_quantile_df2 = factor_industry_quantile_df2.to_frame(specific_industry)
            factor_industry_quantile_df2 = factor_industry_quantile_df2.T
            factor_industry_analysis = pd.concat([factor_industry_analysis, factor_industry_quantile_df2])

        factor_industry_analysis = factor_industry_analysis.rank(axis=1)
        self.industry_group_analysis_result = factor_industry_analysis
        # 如分组中第1组是最差的，那么计入poor_grouping_industries
        for specific_industry in range(1, 32):
            if factor_industry_analysis.at[specific_industry, 'group_quantile_1'] == self.industry_analysis_group_num:
                self.worst_grouping_industries_list.append(specific_industry)
            if factor_industry_analysis.at[specific_industry, 'group_quantile_1'] == (
                    self.industry_analysis_group_num - 1):
                self.poor_grouping_industries_list.append(specific_industry)

    #####################################################################
    # -------------------- 以下是生成pdf报告的代码 -------------------- #
    def __pdf_output(self):
        uuid0 = str(uuid.uuid1())
        abs_address = report_address + '//factor_report//'
        if not os.path.exists(abs_address):
            os.makedirs(abs_address)
        doc = SimpleDocTemplate(abs_address + 'factor ' + self.factor_name + '_' + str(
            self.report_timestamp.strftime("%Y%m%d_%H%M%S")) + '.pdf', rightMargin=40, leftMargin=20, topMargin=50,
            bottomMargin=20)
        story = []
        style_type = getSampleStyleSheet()
        style_type.add(ParagraphStyle(name='Justify', alignment=TA_JUSTIFY))
        story.append(Paragraph(self.factor_name + ' test', style_type['Title']))
        story.append(Spacer(1, 24))
        text_data = '<font size=9.5>%s</font>' % time.ctime()
        text_data = 'Report date : ' + text_data
        story.append(Paragraph(text_data, style_type['Normal']))
        story.append(Spacer(1, 48))
        text_data = 'Factor Information'
        story.append(Paragraph(text_data, style_type['Justify']))
        story.append(Spacer(1, 12))
        dic = {
            'Factor_Name': self.factor_name,
            'Test_Period': str(self.start_date_orignal) + ' --> ' + str(self.end_date),
            'Stock Universe': self.universe,
            'Date Count': Dtk.get_trading_day(self.start_date_orignal, self.end_date).__len__(),
            'Holding Period': self.holding_period,
            'Neutral Factors': self.neutral_factor_set,
            'Outlier Filter': self.outlier_filter_method,
            'Stock Cost Rate': self.stock_cost_rate,
        }
        for item in dic.keys():
            story.append(Paragraph(item.rjust(20) + ' : ' + str(dic[item]), style_type['Normal']))
            story.append(Spacer(1, 6))
        story.append(Spacer(1, 24))
        story.append(Paragraph('Factor Distribution:', style_type['Justify']))
        story.append(Spacer(1, 6))
        data = self._dic2list()
        tb = self._table_model(data)
        story.append(tb)
        story.append(Spacer(1, 24))
        story.append(Paragraph('IC Statistics:', style_type['Justify']))
        data = self._dic2list_ic()
        tb = self._table_model(data)
        story.append(tb)
        story.append(Spacer(1, 24))
        story.append(Paragraph('Other Performance Statistics', style_type['Justify']))
        data = self.__other_performance_stat()
        tb = self._table_model(data)
        story.append(tb)
        story.append(Spacer(1, 24))
        story.append(Paragraph('Industry Grouping Analysis', style_type['Justify']))
        data = self.__industry_group_analysis_output_pdf()
        tb = self._table_model(data)
        story.append(tb)

        story.append(Spacer(1, 24))
        ic_cumsum_pic_name = uuid0 + "ic_cumsum.png"
        series_input = [self.factor_test_report['IC_cumsum'], self.factor_neutralized_test_report['IC_cumsum']]
        ic_cum_pic = plot_series(series_input, ['factor', 'factor_n'], 'IC_cumsum', 'IC_cumsum',
                                 ic_cumsum_pic_name, report_address)
        story.append(ic_cum_pic)

        story.append(Spacer(1, 24))
        ic_rolling_pic_name = uuid0 + "ic_rolling.png"
        series_input = [self.factor_test_report['IC_rolling'], self.factor_neutralized_test_report['IC_rolling']]
        ic_rolling = plot_series(series_input, ['factor', 'factor_n'], 'IC_rolling', 'IC_rolling',
                                 ic_rolling_pic_name, report_address)
        story.append(ic_rolling)

        story.append(Spacer(1, 24))
        universe_coverage_pic_name = uuid0 + 'universe_coverage.png'
        series_input = [self.factor_test_report['Universe_coverage_series'],
                        self.factor_neutralized_test_report['Universe_coverage_series']]
        universe_coverage_pic = plot_series(series_input, ['factor', 'factor_n'],
                                            'Factor_coverage_ratio_in_universe',
                                            'Factor_coverage_ratio_in_stock_universe', universe_coverage_pic_name,
                                            report_address)
        story.append(universe_coverage_pic)

        story.append(Spacer(1, 24))
        group_test_nav_pic_name = uuid0 + "group_nav.png"
        group_test_nav_pic = self._draw_group_nav(group_test_nav_pic_name)
        story.append(group_test_nav_pic)

        story.append(Spacer(1, 24))
        top_group_hedged_pic_name = uuid0 + 'top_group_hedged_nav.png'
        df_0 = self.top_group_hedge_nav_dict["000300.SH"].to_frame()
        df_1 = self.top_group_hedge_nav_dict["000905.SH"].to_frame()
        df_2 = self.top_group_hedge_nav_dict["MktMedian"].to_frame()
        df_3 = self.top_group_hedge_nav_dict["MktMean"].to_frame()
        df_0 = Dtk.convert_df_index_type(df_0, 'date_int', 'timestamp')
        df_1 = Dtk.convert_df_index_type(df_1, 'date_int', 'timestamp')
        df_2 = Dtk.convert_df_index_type(df_2, 'date_int', 'timestamp')
        df_3 = Dtk.convert_df_index_type(df_3, 'date_int', 'timestamp')
        series_0 = df_0[0]
        series_1 = df_1[0]
        series_2 = df_2[0]
        series_3 = df_3[0]
        series_input = [series_0, series_1, series_2, series_3]
        top_group_hedged_nav_pic = plot_series(series_input, ["hedged_000300", "hedged_000905",
                                                              "hedged_MktMedian", "hedged_MktMean"],
                                               'top_group_hedged_nav', 'Top_group_hedged_nav',
                                               top_group_hedged_pic_name, report_address, 'upper left')
        story.append(top_group_hedged_nav_pic)

        story.append(Spacer(1, 24))
        group_annualized_return_pic_name = uuid0 + "group_annualized_return.png"
        group_annualized_return_pic = self.__plot_group_annualized_bar(group_annualized_return_pic_name)
        story.append(group_annualized_return_pic)

        story.append(Spacer(1, 24))
        long_short_pic_name = uuid0 + "Long_short_nav.png"
        long_short_df = self.long_short_nav.to_frame()
        long_short_df = Dtk.convert_df_index_type(long_short_df, 'date_int', 'timestamp')
        long_short_nav_series = long_short_df[0]
        long_short_nav_pic = plot_one_series(long_short_nav_series, "long_short_nav", "long_short_nav",
                                             "Long_short_nav", long_short_pic_name, report_address)
        story.append(long_short_nav_pic)

        group_pic = None
        story.append(Spacer(1, 24))
        for i_year in self.each_year_group_return_dict.keys():
            i_pic_name = uuid0 + "Group_return" + i_year + ".png"
            group_pic = plot_group_bar2(self.each_year_group_return_dict[i_year], i_year, i_pic_name, report_address)
            story.append(group_pic)
            story.append(Spacer(1, 24))

        doc.build(story)
        del ic_cum_pic, ic_rolling, group_test_nav_pic, universe_coverage_pic, top_group_hedged_nav_pic, \
            group_annualized_return_pic, long_short_nav_pic, group_pic
        os.remove(os.path.join(report_address, ic_cumsum_pic_name))
        os.remove(os.path.join(report_address, ic_rolling_pic_name))
        os.remove(os.path.join(report_address, group_test_nav_pic_name))
        os.remove(os.path.join(report_address, universe_coverage_pic_name))
        os.remove(os.path.join(report_address, top_group_hedged_pic_name))
        os.remove(os.path.join(report_address, group_annualized_return_pic_name))
        os.remove(os.path.join(report_address, long_short_pic_name))
        for i_year in self.each_year_group_return_dict.keys():
            i_pic_name = uuid0 + "Group_return" + i_year + ".png"
            os.remove(os.path.join(report_address, i_pic_name))

    @staticmethod
    def _table_model(data):
        width = 5.2
        col_widths = (width / len(data[0])) * inch
        dis_list = []
        for x in data:
            dis_list.append(x)
        component_table = Table(dis_list, colWidths=col_widths)
        return component_table

    def _dic2list(self):
        result = []
        keys = list(self.factor_neutralized_test_report['factor_distribution'].index)
        result.append([' ', 'Factor', 'Factor Neutralized'])
        for item in keys:
            result.append(
                [item, round(self.factor_test_report['factor_distribution'].loc[item, 'factor_distribution'], 5),
                 round(self.factor_neutralized_test_report['factor_distribution'].loc[item, 'factor_distribution'], 5)])
        return result

    def _dic2list_ic(self):
        result = []
        keys = list(self.factor_neutralized_test_report['IC_stat'].index)
        result.append([' ', 'Factor', 'Factor Neutralized'])
        for item in keys:
            result.append([item, round(self.factor_test_report['IC_stat'].loc[item, 'IC'], 5),
                           round(self.factor_neutralized_test_report['IC_stat'].loc[item, 'IC_of_factor_neutralized'],
                                 5)])
        return result

    def __other_performance_stat(self):
        result = list()
        result.append([' ', 'Stat'])
        result.append(['Average Turnover Rate', round(self.factor_avg_turnover_rate, 4)])
        for i_key in self.group_return_rank_coef.keys():
            result.append(["GroupRank_" + str(i_key), round(self.group_return_rank_coef[i_key], 4)])
        for i_key in self.long_short_return_each_year.keys():
            result.append([i_key, round(self.long_short_return_each_year[i_key], 4)])
        for i_key in self.top_group_excess_return_each_year.keys():
            result.append([i_key, round(self.top_group_excess_return_each_year[i_key], 4)])
        for i_key in self.top_group_monthly_winning_pct_stat.keys():
            result.append(["MonthlyWinningPct_" + str(i_key),
                           round(self.top_group_monthly_winning_pct_stat[i_key], 4)])

        result.append(['max_corr_factor', 'max_corr_value'])
        for i in range(5):
            result.append([self.corr_information['max_corr_factor'][i], str(self.corr_information['max_corr_value'][i])])

        problem_str=self.__satisfied()
        lenth = len(problem_str)
        for i in range(0, lenth // 50 + 1):
            if i == 0:
                result.append(['Problem', problem_str[i * 50:min(len(problem_str), (i + 1) * 50)]])
            else:
                result.append(['       ', problem_str[i * 50:min(len(problem_str), (i + 1) * 50)]])
        result.append(['Satisfied', str(self.__is_satisfied)])

        if self.worst_grouping_industries_list.__len__() > 0:
            worst_group_list = ['Worst_grouping_industries']
            worst_grouping_str = None
            for k, element in enumerate(self.worst_grouping_industries_list):
                if k > 0:
                    worst_grouping_str = worst_grouping_str + ', ' + str(element)
                else:
                    worst_grouping_str = str(element)
            worst_group_list.append(worst_grouping_str)
        else:
            worst_group_list = ['Worst_grouping_industries', ' ']
        result.append(worst_group_list)

        if self.poor_grouping_industries_list.__len__() > 0:
            poor_group_list = ['Poor_grouping_industries']
            poor_grouping_str = None
            for k, element in enumerate(self.poor_grouping_industries_list):
                if k > 0:
                    poor_grouping_str = poor_grouping_str + ', ' + str(element)
                else:
                    poor_grouping_str = str(element)
            poor_group_list.append(poor_grouping_str)
        else:
            poor_group_list = ['Poor_grouping_industries', ' ']
        result.append(poor_group_list)

        return result

    def __industry_group_analysis_output_pdf(self):
        result = [' ']
        for i in range(self.industry_analysis_group_num):
            result.extend(['Quantile_' + str(i+1)])
        result = [result]
        industry_group_value = self.industry_group_analysis_result.values
        for i in range(31):
            temp_value = ['Industry ' + str(i+1)]
            temp_value.extend(industry_group_value[i])
            result.append(temp_value)
        return result

    def _draw_group_nav(self, pic_name):
        time_stamp_list = self.group_nav_list[0].index
        index = []
        index_number = []
        x_number = []
        for i, time_stamp in enumerate(time_stamp_list):
            x_number.append(i)
            if i % int(time_stamp_list.__len__() / 6) == 0:
                index_number.append(i)
                index.append(str(time_stamp))
        x_number = np.array(x_number)
        plt.figure(figsize=(6, 2), dpi=300)
        for i in range(self.group_num):
            plt.plot(x_number, self.group_nav_list[i].values, linewidth=0.4, label=str(i))
        plt.xticks(index_number, index, fontsize=5, rotation=0)
        plt.ylabel('group_nav', fontsize=5)
        plt.yticks(fontsize=5)
        plt.title('group_nav', fontsize=5)
        if self.group_num <= 10:
            plt.legend(loc='lower left', fontsize=5)
        else:
            plt.legend(loc='lower left', fontsize=3)
        file_name = os.path.join(report_address, pic_name)
        plt.savefig(file_name, format='png')
        im = Image(file_name, 9 * inch, 3 * inch)
        return im

    def __plot_group_annualized_bar(self, pic_name):
        def autolabel(rects):  # 为柱状图标上数字
            for i_rect in rects:
                height = i_rect.get_height()
                if height >= 0:
                    plt.text(i_rect.get_x(), 1.03 * height, '%.4f' % float(height), fontsize=3)
                else:
                    plt.text(i_rect.get_x(), 0.97 * height, '%.4f' % float(height), fontsize=3)
        plt.figure(figsize=(6, 2), dpi=300)
        rect = plt.bar(np.arange(self.group_num + self.benchmark_list.__len__()),
                       [self.group_total_annualized_return_dict[group] for group in
                        self.group_total_annualized_return_dict.keys()])
        plt.xticks(np.arange(self.group_num + self.benchmark_list.__len__()),
                   list(self.group_total_annualized_return_dict.keys()), fontsize=3, rotation=30)
        plt.yticks(fontsize=5)
        plt.title("Group annualized return", fontsize=5)
        autolabel(rect)
        file_name = os.path.join(report_address, pic_name)
        plt.savefig(file_name, format='png')
        im = Image(file_name, 9 * inch, 3 * inch)
        return im

    # -------------------- 以上是生成pdf报告的代码 -------------------- #
    #####################################################################

    def __satisfied(self):
        # 判断因子是否满足入库标准
        abs_icir_threshold = 1.5
        winning_rate_against_mkt_median = 0.85
        mkt_median_return_threshold = 0.1
        long_short_return_threshold = 0
        avg_turnover_threshold = 0.5
        max_correlation_threshold = 0.9

        def func_same_sign(*args):
            first_value = args[0]
            for value in args:
                if value * first_value < 0:
                    return False
            return True

        def year_return(year: str):
            trading_day_number = ((self.daily_return_mean.index // 10000).astype(str) == year).sum()
            return self.top_group_excess_return_each_year[year + '-MktMedian'] * 250 / trading_day_number

        ic_data = self.factor_neutralized_test_report['IC_stat'].loc[:, 'IC_of_factor_neutralized']
        condition_0 = func_same_sign(ic_data.loc['IC_mean2015'], ic_data.loc['IC_mean2016'], ic_data.loc['IC_mean2017'],
                                     ic_data.loc['IC_mean2018'])
        condition_1 = func_same_sign(self.group_return_rank_coef['2015'], self.group_return_rank_coef['2016'],
                                     self.group_return_rank_coef['2017'], self.group_return_rank_coef['2018'])
        condition_2 = (abs(ic_data.loc['ICIR']) > abs_icir_threshold)
        condition_3 = (self.top_group_monthly_winning_pct_stat['MktMedian'] > winning_rate_against_mkt_median)
        condition_4 = ((year_return('2015') > mkt_median_return_threshold) and (year_return('2016') >
                                                                                mkt_median_return_threshold)) and (
            year_return('2017') > mkt_median_return_threshold) and (year_return('2018') > mkt_median_return_threshold)
        condition_5 = ((self.long_short_return_each_year['2015-LongShort'] > long_short_return_threshold) and (
            self.long_short_return_each_year['2016-LongShort'] > long_short_return_threshold) and (
                           self.long_short_return_each_year['2017-LongShort'] > long_short_return_threshold) and (
                           self.long_short_return_each_year['2018-LongShort'] > long_short_return_threshold))
        condition_6 = self.factor_avg_turnover_rate < avg_turnover_threshold
        condition_7 = self.corr_information['max_corr_value'][0] < max_correlation_threshold
        result = None

        def add_note(original, str_need_to_add):
            if original is None:
                return str_need_to_add
            else:
                return original + ' and ' + str_need_to_add

        if not condition_0:
            result = add_note(result, 'IC direction not same sign ')
        if not condition_1:
            result = add_note(result, 'Group rank not same sign ')
        if not condition_2:
            result = add_note(result, 'abs(ICIR) not greater than ' + str(abs_icir_threshold))
        if not condition_3:
            result = add_note(result, 'MonthlyWinningPct_MktMedian not greater than ' +
                              str(winning_rate_against_mkt_median))
        if not condition_4:
            result = add_note(result, str('Top group-MktMedian not greater than ' +
                                          str(mkt_median_return_threshold) + 'every year '))
        if not condition_5:
            result = add_note(result, 'Long-short return not greater than ' +
                              str(long_short_return_threshold) + 'every year ')
        if not condition_6:
            result = add_note(result, 'Average turnover rate greater than ' + str(avg_turnover_threshold))
        if not condition_7:
            result = add_note(result, 'Max correlation greater than ' + str(max_correlation_threshold))

        if result is None:
            result = 'No problem '
            self.__is_satisfied = True
        else:
            self.__is_satisfied = False
        return result

    def __fast_group_test(self, group_num=10, test_factor=..., position_window=1, stock_cost_ratio=0):
        """极速分层测试：不做行业中性，不对冲，不考虑买卖的可行性（涨停可以买、跌停或停牌都可以卖）"""
        test_factor2 = Dtk.convert_df_index_type(test_factor, 'timestamp', 'date_int')
        group_set = []
        for j, i_date in enumerate(list(test_factor2.index)):
            if j % position_window == 0:
                factor0 = test_factor2.loc[i_date]
                factor0 = factor0.sort_values()
                factor0 = factor0.dropna()
                num_stock = factor0.shape[0]
                stock_num_each_group = np.floor(num_stock / group_num)
                for i_group in range(group_num):
                    code_selected = list(factor0.index[int(stock_num_each_group * i_group):int(
                        stock_num_each_group * i_group + stock_num_each_group)])
                    if code_selected.__len__() > 0:
                        if group_set.__len__() < group_num:
                            group_set.append({i_date: code_selected})
                        else:
                            group_set[i_group].update({i_date: code_selected})
        # group_set是一个list, 长度等于group_num，其每个元素是一组的分组信息
        # group_set中的值（单组分组信息）是字典，字典的key是换仓日（例如20150105），value是对应日的股票列表
        deal_price_dict = {"coda": "twp_coda", "vwap": "vwap", "twap": "twap"}
        deal_price_type = deal_price_dict[self.label_type]
        deal_price_df = Dtk.get_panel_daily_pv_df(self.complete_stock_list, list(test_factor2.index)[0],
                                                  list(test_factor2.index)[-1],
                                                  deal_price_type, "FORWARD")  # 用这种价格撮合成交
        close_price_df = Dtk.get_panel_daily_pv_df(self.complete_stock_list, list(test_factor2.index)[0],
                                                   list(test_factor2.index)[-1], "close", "FORWARD")
        trading_day_list = list(test_factor2.index)
        for j, group in enumerate(group_set):
            temp_ans_list = equally_wt_fast_nav(group, trading_day_list, deal_price_df, close_price_df,
                                                stock_cost_ratio)
            i_group_nav, i_group_annualized_return, factor_turnover_rate = temp_ans_list
            self.group_nav_list.append(i_group_nav)
            self.factor_avg_turnover_rate = factor_turnover_rate
            if j <= 8:
                self.group_total_annualized_return_dict.update({"Group" + str(0) + str(j + 1):
                                                                i_group_annualized_return})
            else:
                self.group_total_annualized_return_dict.update({"Group" + str(j + 1): i_group_annualized_return})
        if self.group_nav_list[0].values[-1] > self.group_nav_list[-1].values[-1]:
            top_group_nav = self.group_nav_list[0]
            bottom_group_nav = self.group_nav_list[-1]
        else:
            top_group_nav = self.group_nav_list[-1]
            bottom_group_nav = self.group_nav_list[0]
        group_return_each_year_list = []
        # 将分组测试中，每组的nav计算每年的收益率
        for j, i_group_nav in enumerate(self.group_nav_list):
            if j <= 8:
                return_each_year = nav_series_annually_stat(i_group_nav, "Group" + "0" + str(j + 1))
            else:
                return_each_year = nav_series_annually_stat(i_group_nav, "Group" + str(j + 1))
            group_return_each_year_list.append(return_each_year)
        year_list = []
        for item in list(group_return_each_year_list[0].keys()):
            year_list.append(item[0:4])
        for i_year in year_list:
            temp_list = []
            for j_group in range(self.group_num):
                if j_group <= 8:
                    temp_list.append(group_return_each_year_list[j_group][i_year + "-Group0" + str(j_group + 1)])
                else:
                    temp_list.append(group_return_each_year_list[j_group][i_year + "-Group" + str(j_group + 1)])
            self.each_year_group_return_dict.update({i_year: temp_list})

        def calc_group_rank(list_input):
            rank_a = sorted(range(len(list_input)), key=list_input.__getitem__)
            rank_b = rank_a.copy()
            rank_b.sort()
            rank_coef = np.corrcoef(rank_a, rank_b)[0, 1]
            return rank_coef

        for year in self.each_year_group_return_dict.keys():
            group_rank = calc_group_rank(self.each_year_group_return_dict[year])
            self.group_return_rank_coef.update({year: group_rank})  # 计算每年分组测试、每组收益率排序的rank相关系数

        for hedge_index in self.benchmark_list:
            hedged_nav_series, hedge_index_annualized_return = \
                self.__fast_group_hedge_nav(top_group_nav, hedge_index)
            top_group_excess_return_each_year = nav_series_annually_stat(hedged_nav_series, hedge_index)
            self.top_group_excess_return_each_year.update(top_group_excess_return_each_year)
            self.top_group_hedge_nav_dict.update({hedge_index: hedged_nav_series})
            self.group_total_annualized_return_dict.update({hedge_index: hedge_index_annualized_return})
            daily_index = list(hedged_nav_series.index)
            monthly_index = Dtk.get_trading_day(daily_index[0], daily_index[-1], 'M')
            if daily_index[0] < monthly_index[0]:
                monthly_index_1 = [daily_index[0]]
                monthly_index_1.extend(monthly_index)
            else:
                monthly_index_1 = monthly_index
            hedged_nav_series_monthly = hedged_nav_series.reindex(monthly_index_1)
            hedged_return_each_month_series = hedged_nav_series_monthly - hedged_nav_series_monthly.shift(1)
            hedged_return_each_month_series.iloc[0] = hedged_nav_series_monthly.iloc[0] - 1
            # 将月度超额收益降序排列，一般列首>0、列尾<=0，逐个循环，可计算月度超额收益>0的月数
            hedged_monthly_return_descending_list = list(hedged_return_each_month_series)
            hedged_monthly_return_descending_list.sort(reverse=True)
            if hedged_monthly_return_descending_list[0] > 0 >= hedged_monthly_return_descending_list[-1]:
                for i in range(hedged_monthly_return_descending_list.__len__()):
                    if hedged_monthly_return_descending_list[i] * hedged_monthly_return_descending_list[i+1] <= 0:
                        break
                monthly_alpha_winning_pct = (i+1) / hedged_monthly_return_descending_list.__len__()
            elif hedged_monthly_return_descending_list[-1] > 0:
                monthly_alpha_winning_pct = 1
            else:
                monthly_alpha_winning_pct = 0
            self.top_group_monthly_winning_pct_stat.update({hedge_index: monthly_alpha_winning_pct})
            for date_return in hedged_return_each_month_series.iteritems():
                dict_key = str(date_return[0])[0:6] + "-" + str(hedge_index)
                self.top_group_excess_return_each_month.update({dict_key: date_return[1]})
        long_short_nav_series, long_short_annualized_return = fast_long_short_nav(top_group_nav, bottom_group_nav)
        long_short_return_each_year = nav_series_annually_stat(long_short_nav_series, "LongShort")
        self.long_short_nav = long_short_nav_series
        self.long_short_return_each_year = long_short_return_each_year

    def __fast_group_hedge_nav(self, nav_series, hedge_index):
        pct_chg_list = nav_series.diff()
        pct_chg_list = pct_chg_list.replace({np.nan: 0})
        if hedge_index == "MktMedian":
            self.daily_return_median = self.daily_return_median.reindex(nav_series.index)
            hedge_index_pct_chg = self.daily_return_median
        elif hedge_index == "MktMean":
            self.daily_return_mean = self.daily_return_mean.reindex(nav_series.index)
            hedge_index_pct_chg = self.daily_return_mean
        else:
            hedge_index_close = Dtk.get_panel_daily_pv_df([hedge_index], self.start_date, self.end_date, 'close')
            hedge_index_pct_chg = hedge_index_close / hedge_index_close.shift(1) - 1
            hedge_index_pct_chg = hedge_index_pct_chg[hedge_index]
        hedge_index_pct_chg.iloc[0] = 0.0
        daily_alpha = pct_chg_list - hedge_index_pct_chg
        daily_alpha.iloc[0] = 0.0
        hedged_nav = np.cumsum(daily_alpha) + 1
        hedged_nav = pd.Series(hedged_nav, index=nav_series.index)
        hedge_index_annualized_return = np.cumsum(hedge_index_pct_chg.values)[-1] / (
                                        hedge_index_pct_chg.__len__() / 244)
        return hedged_nav, hedge_index_annualized_return

    def __json_output(self):
        self.factor_stat_output.update({"Factor_name": self.factor_name, "Test_period_start": self.start_date_orignal,
                                        "Test_period_end": self.end_date, "Stock_universe": self.universe,
                                        "Date_count": Dtk.get_trading_day(self.start_date_orignal,
                                                                          self.end_date).__len__(),
                                        "Holding_period": self.holding_period,
                                        "Neutral_factors": list(self.neutral_factor_set),
                                        "Group_number": self.group_num, "Label_type": self.label_type,
                                        "Universe_coverage_mean": self.factor_neutralized_test_report[
                                            "Universe_coverage_mean"],
                                        "Universe_coverage_min": self.factor_neutralized_test_report[
                                            "Universe_coverage_min"],
                                        "Not_enough_coverage_dates": self.factor_neutralized_test_report[
                                            "Not_enough_coverage_dates"],
                                        "Stock_cost_rate": self.stock_cost_rate,
                                        "Avg_turnover_rate": self.factor_avg_turnover_rate,
                                        'Satisfied': self.__is_satisfied})
        for ic_stat_item in list(self.factor_neutralized_test_report['IC_stat'].index):
            self.factor_stat_output.update(
                {ic_stat_item: self.factor_neutralized_test_report['IC_stat'].loc[ic_stat_item][0]})
        for rank_year in list(self.group_return_rank_coef.keys()):
            self.factor_stat_output.update({'GroupRank_' + rank_year: self.group_return_rank_coef[rank_year]})
        for i_benchmark in self.top_group_monthly_winning_pct_stat.keys():
            self.factor_stat_output.update(
                {str('MonthlyWinningPct_' + i_benchmark): self.top_group_monthly_winning_pct_stat[i_benchmark]})
        self.factor_stat_output.update(self.top_group_excess_return_each_year)
        self.factor_stat_output.update(self.long_short_return_each_year)
        self.factor_stat_output.update(self.top_group_excess_return_each_month)
        self.factor_stat_output.update({'Worst_grouping_industries': self.worst_grouping_industries_list})
        self.factor_stat_output.update({'Poor_grouping_industries': self.poor_grouping_industries_list})
        self.factor_stat_output.update({'Max_corr_factors':  list(self.corr_information['max_corr_factor'])})
        self.factor_stat_output.update({'Max_corr_values': list(self.corr_information['max_corr_value'])})
        self.factor_stat_output.update(self.size_analysis_result)
        abs_address = report_address + '//factor_report//'
        json_file = open(abs_address + 'factor ' + self.factor_name + '_' + str(
            self.report_timestamp.strftime("%Y%m%d_%H%M%S")) + '.json', 'w')
        json_file.write(json.dumps(self.factor_stat_output))
        json_file.close()

    def launch_test(self):
        self.load_factor()
        self.stock_universe_df = Dtk.get_panel_daily_info(self.complete_stock_list, self.start_date, self.end_date,
                                                          info_type=self.universe, output_index_type='timestamp')
        # stock_universe_df是一个矩阵，值为1或0，如某只股票在某日在股票池内则值为1、否则为0
        self.label_data = self.load_label()
        self.label_data = self.label_data * self.stock_universe_df / self.stock_universe_df

        # 以下计算因子的IC
        # factor_data_df 乘以 stock_universe_df 再除以 stock_universe_df，就会把不在股票池内的因子值调整为nan
        factor_data = self.original_factor_data_df * self.stock_universe_df / self.stock_universe_df
        factor_isnan = np.isnan(factor_data)
        factor_coverage = factor_data.shape[1] - np.sum(factor_isnan, axis=1)
        universe_coverage_ratio = factor_coverage / np.sum(self.stock_universe_df, axis=1)  # 因子在universe的覆盖度
        universe_coverage_ratio = universe_coverage_ratio.replace(np.nan, 0)
        not_enough_coverage_series = universe_coverage_ratio[universe_coverage_ratio < 0.1]
        not_enough_coverage_date_list = []
        for date in list(not_enough_coverage_series.index):
            not_enough_coverage_date_list.append(int(dt.datetime.fromtimestamp(date).strftime("%Y%m%d")))
        self.factor_test_report.update({"Universe_coverage_series": universe_coverage_ratio})
        self.factor_test_report.update({"Universe_coverage_mean": np.mean(universe_coverage_ratio)})
        self.factor_test_report.update({"Universe_coverage_min": np.min(universe_coverage_ratio)})
        self.factor_test_report.update({"Not_enough_coverage_dates": not_enough_coverage_date_list})
        volume_df = Dtk.get_panel_daily_pv_df(self.complete_stock_list, self.start_date, self.end_date, "volume")
        volume_df = Dtk.convert_df_index_type(volume_df, 'date_int', 'timestamp')
        factor_data = factor_data * volume_df / volume_df  # 将停牌股票的因子值置为nan
        factor_data = outlier_filter(factor_data, self.outlier_filter_method)  # 因子去除极值
        factor_data = z_score_standardizer(factor_data)  # 因子标准化
        self.factor_test_report.update({'factor_data': factor_data})
        factor_distribution = factor_distribution_calc(factor_data)
        print(factor_distribution)  # 这些打印的部分，后续会改成输出到报告
        self.factor_test_report.update({'factor_distribution': factor_distribution})
        # 因为计算因子时会删去部分天（例如2016年1月熔断的2天），label也要将这些天删去
        self.label_data = self.label_data.reindex(factor_data.index)
        if self.is_day_factor:
            ic_series = factor_data.corrwith(self.label_data.shift(-1), axis=1)
        else:
            ic_series = factor_data.corrwith(self.label_data, axis=1)
        ic_stat = self.ic_stat_calc(ic_series, 'IC')
        print(ic_stat)
        ic_rolling_window = max([3, self.holding_period])
        ic_rolling = ic_series.rolling(ic_rolling_window).mean()
        ic_cumsum = ic_series.cumsum()
        self.factor_test_report.update({'IC_series': ic_series})
        self.factor_test_report.update({'IC_stat': ic_stat})
        self.factor_test_report.update({'IC_rolling': ic_rolling})
        self.factor_test_report.update({'IC_cumsum': ic_cumsum})

        # 因子中性化（将self.neutral_factor_set对原因子回归，以残差作为新的因子值）
        factor_data_neutralized = factor_neutralizer(factor_data, self.start_date, self.end_date,
                                                     self.neutral_factor_set)
        factor_data_neutralized_isnan = np.isnan(factor_data_neutralized)
        factor_neu_coverage = factor_data_neutralized.shape[1] - np.sum(factor_data_neutralized_isnan, axis=1)
        # 中性化因子在universe的覆盖度
        neu_universe_coverage_ratio = factor_neu_coverage / np.sum(self.stock_universe_df, axis=1)
        neu_universe_coverage_ratio = neu_universe_coverage_ratio.replace(np.nan, 0)
        neu_not_enough_coverage_series = neu_universe_coverage_ratio[neu_universe_coverage_ratio < 0.1]
        neu_not_enough_coverage_date_list = []
        for date in list(neu_not_enough_coverage_series.index):
            neu_not_enough_coverage_date_list.append(int(dt.datetime.fromtimestamp(date).strftime("%Y%m%d")))
        self.factor_neutralized_test_report.update({"Universe_coverage_series": neu_universe_coverage_ratio})
        self.factor_neutralized_test_report.update({"Universe_coverage_mean": np.mean(neu_universe_coverage_ratio)})
        self.factor_neutralized_test_report.update({"Universe_coverage_min": np.min(neu_universe_coverage_ratio)})
        self.factor_neutralized_test_report.update({"Not_enough_coverage_dates": neu_not_enough_coverage_date_list})
        self.factor_neutralized_test_report.update({"factor_data": factor_data_neutralized})
        factor_distribution = factor_distribution_calc(factor_data_neutralized)
        print(factor_distribution)
        self.factor_neutralized_test_report.update({'factor_distribution': factor_distribution})
        if self.is_day_factor:
            ic_series = factor_data_neutralized.corrwith(self.label_data.shift(-1), axis=1)
        else:
            ic_series = factor_data_neutralized.corrwith(self.label_data, axis=1)
        ic_stat = self.ic_stat_calc(ic_series, 'IC_of_factor_neutralized')
        print(ic_stat)
        ic_rolling = ic_series.rolling(ic_rolling_window).mean()
        ic_cumsum = ic_series.cumsum()
        self.factor_neutralized_test_report.update({'IC_series': ic_series})
        self.factor_neutralized_test_report.update({'IC_stat': ic_stat})
        self.factor_neutralized_test_report.update({'IC_rolling': ic_rolling})
        self.factor_neutralized_test_report.update({'IC_cumsum': ic_cumsum})

        # 将因子值在universe内按市值分为3块（小、中、大），在每块内再按因子值分self.group_num/2组，计算每块内
        # top组和bottom组的标签（收益率）在其相应块内的分位数
        self.size_analysis()

        # 将因子在行业内进行分析
        self.industry_group_analysis()

        # 在前面计算IC时，删去了部分全市场都为nan时的日期，但分组测试时要补回来
        group_test_factor_data = self.factor_neutralized_test_report["factor_data"].reindex(
            self.original_factor_data_df.index)
        group_test_factor_data = group_test_factor_data.shift(1).iloc[1:]

        print("Running fast group test...")
        self.__fast_group_test(group_num=self.group_num, test_factor=group_test_factor_data,
                               position_window=self.holding_period, stock_cost_ratio=self.stock_cost_rate)
        self.corr_old_factors()
        print("Fast group test finished. Generating PDF report...")
        self.__pdf_output()
        print("PDF report generated.")
        self.__json_output()
        print("Single factor test finished.")
