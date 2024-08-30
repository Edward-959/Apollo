"""
Created by 011672, 2019/3/18
该程序用于每次因子更新后
会输出一个csv，记录因子的更新情况和IC相关的基本信息
"""

import DataAPI.DataToolkit as Dtk
import pandas as pd
import numpy as np
import datetime as dt
import time
import platform, os


def outlier_filter(value_df, method="MAD"):
    if method == "3Std":
        factor_mean = value_df.mean(axis=1)
        factor_std = value_df.std(axis=1)
        upper_limit = factor_mean + 3 * factor_std
        lower_limit = factor_mean - 3 * factor_std
        upper_limit = upper_limit.fillna(method='ffill')
        lower_limit = lower_limit.fillna(method='ffill')
        value_df = value_df.clip_lower(lower_limit, axis='index')
        value_df = value_df.clip_upper(upper_limit, axis='index')
        return value_df
    else:
        # 在20160104和20160107这两天，全市场熔断，那么先删除全市场都是nan的数据
        factor_max = value_df.max(axis=1)
        factor_max = factor_max.dropna()
        value_df = value_df.reindex(factor_max.index)
        factor_median = value_df.median(axis=1)
        factor_deviation_from_median = value_df.sub(factor_median, axis=0)
        factor_mad = factor_deviation_from_median.abs().median(axis=1)
        lower_limit = factor_median - 3.14826 * factor_mad
        upper_limit = factor_median + 3.14826 * factor_mad
        lower_limit = lower_limit.fillna(method='ffill')
        upper_limit = upper_limit.fillna(method='ffill')
        value_df = value_df.clip_lower(lower_limit, axis='index')
        value_df = value_df.clip_upper(upper_limit, axis='index')
        return value_df


def z_score_standardizer(value_df):
    factor_mean = value_df.mean(axis=1)
    factor_std = value_df.std(axis=1)
    value_df = value_df.sub(factor_mean, axis=0)
    value_df = value_df.div(factor_std, axis=0)
    return value_df


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


def factor_neutralizer(factor_df, start_date, end_date, industry_df_copy, mkt_cap_ard_df_copy):
    def make_one_hot(input_data):
        max_value = np.max(input_data) + 1
        result = (np.arange(max_value) == input_data[:, None]).astype(np.int)
        return result

    valid_start_date = Dtk.get_n_days_off(start_date, -22)[0]
    stock_list = list(factor_df.columns)
    factor_date_list = list(factor_df.index)
    industry_df_copy = Dtk.convert_df_index_type(industry_df_copy, 'date_int', 'timestamp')
    mkt_cap_ard_df_copy = Dtk.convert_df_index_type(mkt_cap_ard_df_copy, 'date_int', 'timestamp')
    industry_df_copy = industry_df_copy.reindex(index=factor_df.index, columns=factor_df.columns)
    mkt_cap_ard_df_copy = mkt_cap_ard_df_copy.reindex(index=factor_df.index, columns=factor_df.columns)

    t1 = dt.datetime.now()
    factor_start_line = list(mkt_cap_ard_df_copy.index).index(factor_df.index[0])
    end_line = mkt_cap_ard_df_copy.__len__()
    repeat_lines_list = list(range(end_line))[factor_start_line: end_line]
    factor_array = factor_df.values
    industry_df2 = industry_df_copy.fillna(0)  # 将行业的缺失值替换为0，以方便后续用np创造one_hot矩阵
    industry_array = industry_df2.values
    mkt_cap_ard_array = mkt_cap_ard_df_copy.values
    stock_col_num = stock_list.__len__()
    residual_list = []
    residual_date_list = []
    for j, i_line in enumerate(repeat_lines_list):
        if j % 100 == 0:
            print("factor neutralizing, {} / {} days".format(j, list(factor_df.index).__len__()))
        y0 = factor_array[j]
        x1_0 = industry_array[i_line].astype(np.int)
        x1_0 = make_one_hot(x1_0)  # 这个one_hot函数
        x1_0 = x1_0[:, 1:]  # 去掉第0列，也就是去掉原来无行业的值
        x2_0 = mkt_cap_ard_array[i_line]
        x2_0 = x2_0.reshape([stock_col_num, 1])
        x0 = np.hstack([x1_0, x2_0])
        y0_isnan = np.isnan(y0)
        x0_isnan = np.isnan(np.max(x0, axis=1))
        y0_isvalid = 1 - y0_isnan
        x0_isvalid = 1 - x0_isnan
        valid_rows = x0_isvalid * y0_isvalid
        valid_stock_list = [stock_list[i] for i in range(stock_list.__len__()) if valid_rows[i] == 1]
        x0_valid = x0[valid_rows == 1]
        y0_valid = y0[valid_rows == 1]
        ind_check = np.sum(x0_valid, axis=0)
        empty_industry = []
        for i_col in range(x1_0.shape[1]):
            if ind_check[i_col] < 1:
                empty_industry.append(i_col)
        if empty_industry.__len__() > 0:
            x0_valid = np.delete(x0_valid, empty_industry, 1)
        if x0_valid.__len__() > 0:
            b = np.linalg.inv(x0_valid.T.dot(x0_valid)).dot(x0_valid.T).dot(y0_valid)
            residual = y0_valid - x0_valid.dot(b)  # 求残差
            residual_dict = dict(zip(valid_stock_list, residual))  # 将残差与股票代码关联起来
            residual_list.append(residual_dict)
            residual_date_list.append(factor_date_list[j])
    # 将残差list一次性转变为DataFrame
    neutralized_factor_df = pd.DataFrame(residual_list, index=residual_date_list)
    t2 = dt.datetime.now()
    print("neutralizing costs", t2 - t1)
    return neutralized_factor_df


def check_factor(factor_data, label, industry_df, mkt_cap_ard_df, volume_df):
    factor_dict = {}
    data = Dtk.convert_df_index_type(factor_data, 'timestamp', 'date_int')
    factor_start_date = list(data.index)[0]
    factor_end_date = list(data.index)[-1]
    industry_df_copy = industry_df.copy()
    mkt_cap_ard_df_copy = mkt_cap_ard_df.copy()

    factor_data = outlier_filter(factor_data, 'MAD')  # 因子去除极值
    factor_data = z_score_standardizer(factor_data)  # 因子标准化
    volume_df = volume_df.reindex(index=factor_data.index, columns=factor_data.columns)
    factor_data = factor_data * volume_df / volume_df  # 将停牌股票的因子值置为nan

    nuetralized_factor_df = factor_neutralizer(factor_data, factor_start_date, factor_end_date, industry_df_copy,
                                               mkt_cap_ard_df_copy)
    nuetralized_factor_df = Dtk.convert_df_index_type(nuetralized_factor_df, 'timestamp', 'date_int')
    complete_stock_list = Dtk.get_complete_stock_list()
    stock_universe = Dtk.get_panel_daily_info(complete_stock_list, factor_start_date, factor_end_date,
                                              info_type='alpha_universe')
    data = nuetralized_factor_df * stock_universe / stock_universe
    factor_isnan = np.isnan(data)
    factor_coverage = data.shape[1] - np.sum(factor_isnan, axis=1)
    universe_coverage_ratio = factor_coverage / np.sum(stock_universe, axis=1)  # 因子在universe的覆盖度
    universe_coverage_ratio_mean = np.mean(universe_coverage_ratio)
    coverage_ratio_last_line = universe_coverage_ratio[factor_end_date]
    trading_days_normal = len(Dtk.get_trading_day(factor_start_date, factor_end_date))
    trading_days_practice = len(list(data.index))
    trading_day_diff = trading_days_practice - trading_days_normal
    label = label.reindex(index=data.index)

    nuetralized_factor_df = Dtk.convert_df_index_type(nuetralized_factor_df, 'date_int', 'timestamp')
    label = Dtk.convert_df_index_type(label, 'date_int', 'timestamp')

    ic_series = nuetralized_factor_df.corrwith(label.shift(-1), axis=1)
    ic_stat = ic_stat_calc(ic_series, 'IC')
    factor_dict.update({'start_date': factor_start_date})
    factor_dict.update({'end_date': factor_end_date})
    factor_dict.update({'trading_day_diff': trading_day_diff})
    factor_dict.update({'universe_coverage_ratio_mean': universe_coverage_ratio_mean})
    factor_dict.update({'coverage_ratio_last_line': coverage_ratio_last_line})
    factor_dict.update({'IC_mean': ic_stat.at['IC_mean', 'IC']})
    ic_stat_index = list(ic_stat.index)
    if len(ic_stat_index) > 13:
        factor_dict.update({'IC_mean2014': ic_stat.at['IC_mean2014', 'IC']})
        factor_dict.update({'IC_mean2015': ic_stat.at['IC_mean2015', 'IC']})
        factor_dict.update({'IC_mean2016': ic_stat.at['IC_mean2016', 'IC']})
        factor_dict.update({'IC_mean2017': ic_stat.at['IC_mean2017', 'IC']})
        factor_dict.update({'IC_mean2018': ic_stat.at['IC_mean2018', 'IC']})
        factor_dict.update({'IC_mean2019': ic_stat.at['IC_mean2019', 'IC']})
    else:
        factor_dict.update({'IC_2014': 'unknown'})
        factor_dict.update({'IC_2015': 'unknown'})
        factor_dict.update({'IC_2016': 'unknown'})
        factor_dict.update({'IC_2017': 'unknown'})
        factor_dict.update({'IC_2018': 'unknown'})
        factor_dict.update({'IC_2019': 'unknown'})
    return factor_dict


def main():  # 输入更新的最后日期
    if platform.system() == "Windows":
        alpha_factor_root_path = "D:\\NewFactorData"
    elif os.system("nvidia-smi") == 0:
        alpha_factor_root_path = "/data/NewFactorData"
    else:
        user_id = os.environ['USER_ID']
        alpha_factor_root_path = "/app/data/" + user_id + "/Apollo/AlphaFactors"
    alpha_factor_root_path += '/AlphaFactors'
    factor_detail = {}
    label_data_path = '/app/data/' + user_id + '/Apollo/AlphaDataBase/Data_twap.h5'
    store = pd.HDFStore(label_data_path)
    label = store.select('/factor')
    store.close()
    stock_list = Dtk.get_complete_stock_list()
    today_int = int(time.strftime("%Y%m%d"))
    industry_df = Dtk.get_panel_daily_info(stock_list, 20100101, today_int, 'industry3', 'date_int')
    industry_df = industry_df.shift(1)  # 日级别信息要取前一天的
    mkt_cap_ard_df = Dtk.get_panel_daily_info(stock_list, 20100101, today_int, 'mkt_cap_ard', 'date_int')
    mkt_cap_ard_df = np.log(mkt_cap_ard_df)  # 对市值取对数
    mkt_cap_ard_df = mkt_cap_ard_df.shift(1)  # 日级别信息要取前一天的
    volume_df = Dtk.get_panel_daily_pv_df(stock_list, 20100101, today_int, "volume")
    volume_df = Dtk.convert_df_index_type(volume_df, 'date_int', 'timestamp')
    for root, dirs, files in os.walk(alpha_factor_root_path):
        for file in files:
            print(file)
            file_path = os.path.join(root, file)
            store = pd.HDFStore(file_path)
            data = store.select('/factor')
            store.close()
            factor_dict = check_factor(data, label, industry_df, mkt_cap_ard_df, volume_df)
            factor_detail.update({file[0:-3]: factor_dict})
    factor_detail = pd.DataFrame(factor_detail)
    factor_detail = factor_detail.T
    index = ['start_date', 'end_date', 'trading_day_diff', 'universe_coverage_ratio_mean', 'coverage_ratio_last_line',
             'IC_mean', 'IC_mean2014', 'IC_mean2015', 'IC_mean2016', 'IC_mean2017', 'IC_mean2018', 'IC_mean2019']
    factor_detail = factor_detail.reindex(columns=index)
    factor_detail.to_csv("/app/data/" + user_id + "/Apollo/AlphaFactors/FactorDetails.csv")
