# -*- coding: utf-8 -*-
"""
created on 2019/03/13
@author: 006566
用于计算和更新指数的行业暴露和Barra因子暴露
index_barra_exposure_updater -- 以指数成分股加权、或指数成分股市值加权，统计指数在各Barra风格因子的暴露，并输出4个文件:
        hs300_industry_weight.csv, hs300_mktstd_index_style_factor_exposure.csv,
        zz500_industry_weight.csv, zz500_mktstd_index_style_factor_exposure.csv
index_industry_exposure_updater -- 统计指数的行业暴露，输出2个文件：hs300_industry_weight.csv, zz500_industry_weight.csv
"""

import DataAPI.DataToolkit as Dtk
import platform
import os
import pandas as pd

update_start_date = 20190223
update_end_date = 20190301

if platform.system() == "Windows":
    save_path = "D:\ApolloTest\BarraRiskModel"
else:
    user_id = os.environ['USER_ID']
    save_path = "/app/data/" + user_id + "/Apollo/BarraRiskModel"


def index_barra_exposure_updater(start_date, end_date):
    # 以指数成分股加权、或指数成分股市值加权，统计指数在各Barra风格因子的暴露
    complete_stock_list = Dtk.get_complete_stock_list(end_date=end_date)

    index_weight_df_300 = Dtk.get_panel_daily_info(complete_stock_list, start_date, end_date, 'index_weight_hs300')
    index_weight_df_500 = Dtk.get_panel_daily_info(complete_stock_list, start_date, end_date, 'index_weight_zz500')
    mkt_cap_df = Dtk.get_panel_daily_info(complete_stock_list, start_date, end_date, 'mkt_cap_ard')

    barra_factor_list = ['Size', 'Beta', 'Momentum', 'ResidualVolatility', 'NonLinearSize', 'Value', 'Liquidity',
                         'EarningsYield', 'Growth', 'Leverage']

    df_index_barra_exposure_300 = pd.DataFrame(data=None, index=index_weight_df_300.index, columns=barra_factor_list)
    df_index_barra_exposure_500 = pd.DataFrame(data=None, index=index_weight_df_500.index, columns=barra_factor_list)
    df_index_barra_exposure_mkt_300 = pd.DataFrame(data=None, index=index_weight_df_300.index,
                                                   columns=barra_factor_list)
    df_index_barra_exposure_mkt_500 = pd.DataFrame(data=None, index=index_weight_df_500.index,
                                                   columns=barra_factor_list)

    for factor_name in barra_factor_list:
        df_barra_factor = Dtk.get_panel_daily_info(complete_stock_list, start_date, end_date, info_type=factor_name)
        if df_barra_factor.__len__() == 0:
            raise Exception("Barra style factor ", str(factor_name), " is empty; please check!")
        df_barra_factor = outlier_filter(df_barra_factor)
        df_barra_factor_mkt_cap = standardizer_mkt(df_barra_factor, mkt_cap_df)
        df_barra_factor_std = standardizer(df_barra_factor)

        temp_df_300 = df_barra_factor_std * index_weight_df_300
        temp_df_500 = df_barra_factor_std * index_weight_df_500
        temp_df_300_mkt = df_barra_factor_mkt_cap * index_weight_df_300
        temp_df_500_mkt = df_barra_factor_mkt_cap * index_weight_df_500

        df_index_barra_exposure_300[factor_name] = temp_df_300.sum(axis=1)
        df_index_barra_exposure_500[factor_name] = temp_df_500.sum(axis=1)
        df_index_barra_exposure_mkt_300[factor_name] = temp_df_300_mkt.sum(axis=1)
        df_index_barra_exposure_mkt_500[factor_name] = temp_df_500_mkt.sum(axis=1)
        # ----- 至此，指数的Barra加权Barra因子暴露和市值加权Barra因子暴露，已计算完毕 -----

    # 以下开始读取旧数据（如存在的话）
    index_barra_exposure_300_file_name = "hs300_index_style_factor_exposure"
    index_barra_exposure_500_file_name = "zz500_index_style_factor_exposure"
    index_barra_exposure_mkt_300_file_name = "hs300_mktstd_index_style_factor_exposure"
    index_barra_exposure_mkt_500_file_name = "zz500_mktstd_index_style_factor_exposure"

    index_barra_exposure_300_path = os.path.join(save_path, index_barra_exposure_300_file_name + ".csv")
    index_barra_exposure_500_path = os.path.join(save_path, index_barra_exposure_500_file_name + ".csv")
    index_barra_exposure_mkt_300_path = os.path.join(save_path, index_barra_exposure_mkt_300_file_name + ".csv")
    index_barra_exposure_mkt_500_path = os.path.join(save_path, index_barra_exposure_mkt_500_file_name + ".csv")

    # 如果相关数据已经有历史值，则读取，并合并之
    start_day_minus_1 = Dtk.get_n_days_off(start_date, -2)[0]

    if os.path.exists(index_barra_exposure_300_path):
        old_df_index_barra_exposure_300 = pd.read_csv(index_barra_exposure_300_path, index_col=0)
        print("index_barra_exposure_300's latest date is", old_df_index_barra_exposure_300.index[-1])
        if old_df_index_barra_exposure_300.index[-1] < start_day_minus_1:
            print("days are missing between original dates and new start date")
            exit()
        df_index_barra_exposure_300 = concat_df_fun(old_df_index_barra_exposure_300, df_index_barra_exposure_300)

    if os.path.exists(index_barra_exposure_500_path):
        old_df_index_barra_exposure_500 = pd.read_csv(index_barra_exposure_500_path, index_col=0)
        print("index_barra_exposure_500's latest date is", old_df_index_barra_exposure_500.index[-1])
        if old_df_index_barra_exposure_500.index[-1] < start_day_minus_1:
            print("days are missing between original dates and new start date")
            exit()
        df_index_barra_exposure_500 = concat_df_fun(old_df_index_barra_exposure_500, df_index_barra_exposure_500)

    if os.path.exists(index_barra_exposure_mkt_300_path):
        old_df_index_barra_exposure_mkt_300 = pd.read_csv(index_barra_exposure_mkt_300_path, index_col=0)
        print("index_barra_exposure_mkt_300's latest date is", old_df_index_barra_exposure_mkt_300.index[-1])
        if old_df_index_barra_exposure_mkt_300.index[-1] < start_day_minus_1:
            print("days are missing between original dates and new start date")
            exit()
        df_index_barra_exposure_mkt_300 = concat_df_fun(old_df_index_barra_exposure_mkt_300,
                                                        df_index_barra_exposure_mkt_300)

    if os.path.exists(index_barra_exposure_mkt_500_path):
        old_df_index_barra_exposure_mkt_500 = pd.read_csv(index_barra_exposure_mkt_500_path, index_col=0)
        print("index_barra_exposure_mkt_500's latest date is", old_df_index_barra_exposure_mkt_500.index[-1])
        if old_df_index_barra_exposure_mkt_500.index[-1] < start_day_minus_1:
            print("days are missing between original dates and new start date")
            exit()
        df_index_barra_exposure_mkt_500 = concat_df_fun(old_df_index_barra_exposure_mkt_500,
                                                        df_index_barra_exposure_mkt_500)

    # 输出
    df_index_barra_exposure_300.to_csv(index_barra_exposure_300_path)
    print("index_barra_exposure_300 was updated to", str(end_date))
    df_index_barra_exposure_500.to_csv(index_barra_exposure_500_path)
    print("index_barra_exposure_500 was updated to", str(end_date))
    df_index_barra_exposure_mkt_300.to_csv(index_barra_exposure_mkt_300_path)
    print("index_barra_exposure_mkt_300 was updated to", str(end_date))
    df_index_barra_exposure_mkt_500.to_csv(index_barra_exposure_mkt_500_path)
    print("index_barra_exposure_mkt_500 was updated to", str(end_date))


def index_industry_exposure_updater(start_date, end_date):
    # 以指数成分股加权统计指数的行业暴露
    complete_stock_list = Dtk.get_complete_stock_list(end_date=end_date)
    industry3_df = Dtk.get_panel_daily_info(complete_stock_list, start_date, end_date, 'industry3')

    def index_industry_calc(benchmark_index):
        if benchmark_index == "000300.SH":
            index_weight_df = Dtk.get_panel_daily_info(complete_stock_list, start_date, end_date, 'index_weight_hs300')
        else:
            index_weight_df = Dtk.get_panel_daily_info(complete_stock_list, start_date, end_date, 'index_weight_zz500')

        industry_column_list = ['industry' + str(i) for i in range(1, 32)]
        # index是日期，列是"industry1"-"industry31"，内容是指数成分股当天该行业的权重
        df_index_industry_weight = pd.DataFrame(data=None, index=index_weight_df.index, columns=industry_column_list)

        for t_day in index_weight_df.index:
            for i_industry in range(1, 32):
                temp_value = sum((industry3_df.loc[t_day] == i_industry) * index_weight_df.loc[t_day])
                df_index_industry_weight.at[t_day, "industry" + str(i_industry)] = temp_value
        return df_index_industry_weight

    index_industry_300_df = index_industry_calc('000300.SH')
    index_industry_500_df = index_industry_calc('000905.SH')
    # ----- 至此，指数的行业暴露已计算完毕 -----

    # 以下：如果相关数据已经有历史值，则读取，并合并之
    index_industry_300_file_name = "hs300_industry_weight"
    index_industry_500_file_name = "zz500_industry_weight"
    index_industry_300_path = os.path.join(save_path, index_industry_300_file_name + ".csv")
    index_industry_500_path = os.path.join(save_path, index_industry_500_file_name + ".csv")

    start_day_minus_1 = Dtk.get_n_days_off(start_date, -2)[0]

    if os.path.exists(index_industry_300_path):
        old_df_index_barra_exposure_300 = pd.read_csv(index_industry_300_path, index_col=0)
        print("index_industry_exposure_300's latest date is", old_df_index_barra_exposure_300.index[-1])
        if old_df_index_barra_exposure_300.index[-1] < start_day_minus_1:
            print("days are missing between original dates and new start date")
            exit()
        index_industry_300_df = concat_df_fun(old_df_index_barra_exposure_300, index_industry_300_df)

    if os.path.exists(index_industry_500_path):
        old_df_index_barra_exposure_500 = pd.read_csv(index_industry_500_path, index_col=0)
        print("index_industry_exposure_500's latest date is", old_df_index_barra_exposure_500.index[-1])
        if old_df_index_barra_exposure_500.index[-1] < start_day_minus_1:
            print("days are missing between original dates and new start date")
            exit()
        index_industry_500_df = concat_df_fun(old_df_index_barra_exposure_500, index_industry_500_df)

    # 输出到csv
    index_industry_300_df.to_csv(index_industry_300_path)
    print("index_industry_exposure_300 was updated to", str(end_date))
    index_industry_500_df.to_csv(index_industry_500_path)
    print("index_industry_exposure_500 was updated to", str(end_date))


def concat_df_fun(df0, df1):
    # 将df0 和 df1纵向concat起来；如两者索引有交集，以df1的覆盖df0的
    if df0.index[-1] < df1.index[0]:
        ans_df = pd.concat([df0, df1])
    else:
        ans_df = pd.concat(
            [df0.loc[:df1.index[0] - 1], df1])
    return ans_df


def outlier_filter(value_df):
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


def standardizer_mkt(value_df, mkt_cap):
    factor_mean = (value_df * mkt_cap).sum(axis=1) / mkt_cap.sum(axis=1)
    factor_std = value_df.std(axis=1)
    value_df = value_df.sub(factor_mean, axis=0)
    value_df = value_df.div(factor_std, axis=0)
    return value_df


def standardizer(value_df):
    factor_mean = value_df.mean(axis=1)
    factor_std = value_df.std(axis=1)
    value_df = value_df.sub(factor_mean, axis=0)
    value_df = value_df.div(factor_std, axis=0)
    return value_df


def main():
    index_barra_exposure_updater(update_start_date, update_end_date)
    index_industry_exposure_updater(update_start_date, update_end_date)

if __name__ == "__main__":
    main()
