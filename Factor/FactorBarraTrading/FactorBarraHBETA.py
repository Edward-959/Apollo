# -*- coding: utf-8 -*-
"""
@author: 006688
Created on 2019/01/22
Style: Beta
HBETA: Historical beta
𝑟𝑡−𝑟𝑓𝑡=𝛼+𝛽𝑠𝑅𝑡+𝑒𝑡
𝑟𝑖𝑛𝑑,𝑡−𝑟𝑓𝑡=𝛼+𝛽𝑖𝑛𝑑𝑅𝑡+𝑢𝑡
𝛽=(1−𝑤)𝛽𝑠+𝑤𝛽𝑖𝑛𝑑
𝑤=𝜎(𝛽𝑠)/(𝜎(𝛽𝑠)+𝜏𝜎(𝛽𝑖𝑛𝑑))
未考虑无风险收益率
"""

import DataAPI.DataToolkit as Dtk
import pandas as pd
import os
import datetime as dt
import platform
import numpy as np


def factor_generator(stock_list, start_date, end_date, factor_file_dir_path, factor_file_name):
    print("Start downloading raw data for calculating the factor")
    t1 = dt.datetime.now()
    ############################################
    # 以下是因子计算逻辑的部分，需要用户自定义 #
    # 计算因子时，最后应得到factor_data这个对象，类型应当是DataFrame，涵盖的时间段是start_date至end_date（前闭后闭）；
    # factor_data的每一列是相应的股票，每一行是每一天的因子值；
    # 最后factor_data的索引，建议与获得的原始行情的索引（index）一致，
    # 如通过reset_index撤销了原始行情的索引，那么不要删除'index'这一列，最后也不要设置别的索引。
    ############################################
    window_period = 252
    half_life = 63
    tau = 2
    complete_stock_list = Dtk.get_complete_stock_list()
    start_date_minus_lag_2 = Dtk.get_n_days_off(start_date, -window_period-2)[0]
    estimation_universe = Dtk.get_panel_daily_info(complete_stock_list, start_date_minus_lag_2, end_date,
                                                   info_type='risk_universe')
    mkt_cap = Dtk.get_panel_daily_info(complete_stock_list, start_date_minus_lag_2, end_date, info_type="mkt_cap_ard")
    industry = Dtk.get_panel_daily_info(complete_stock_list, start_date_minus_lag_2, end_date, info_type='industry3')
    stock_close = Dtk.get_panel_daily_pv_df(complete_stock_list, start_date_minus_lag_2, end_date, pv_type='close',
                                            adj_type='FORWARD')
    t2 = dt.datetime.now()
    print("Raw data was downloaded successfully and it cost", t2 - t1)
    # 计算因子值
    stock_return = stock_close / stock_close.shift(1) - 1
    universe_cap_return = (stock_return[estimation_universe == 1] * mkt_cap[estimation_universe == 1]).sum(
        axis=1) / mkt_cap[estimation_universe == 1].sum(axis=1)
    industry_cap_return = pd.DataFrame(index=stock_return.index)
    for i in range(1, 32):
        industry_cap_return[i] = (stock_return[industry == i] * mkt_cap[industry == i]).sum(axis=1) / \
                                 mkt_cap[industry == i].sum(axis=1)
    trading_days = Dtk.get_trading_day(start_date, end_date)
    alpha = 0.5**(1/half_life)
    weighted_window = np.logspace(window_period, 1, window_period, base=alpha)
    weight_mat = np.diag(weighted_window)
    stock_beta = pd.DataFrame(index=trading_days, columns=stock_return.columns)
    stock_beta_std = pd.DataFrame(index=trading_days, columns=stock_return.columns)
    industry_beta = pd.DataFrame(index=trading_days, columns=industry_cap_return.columns)
    industry_beta_std = pd.DataFrame(index=trading_days, columns=industry_cap_return.columns)
    for date in trading_days:
        i = stock_return.index.tolist().index(date)
        stock_return_i = stock_return.iloc[i-window_period+1:i+1]
        industry_return_i = industry_cap_return.iloc[i-window_period+1:i+1]
        universe_return_i = universe_cap_return.iloc[i-window_period+1:i+1]
        X = np.vstack((np.ones(universe_return_i.size), np.array(universe_return_i)))
        stock_reg = np.linalg.inv(X.dot(weight_mat).dot(X.T)).dot(X).dot(weight_mat).dot(np.array(stock_return_i))
        industry_reg = np.linalg.inv(X.dot(weight_mat).dot(X.T)).dot(X).dot(weight_mat).dot(np.array(industry_return_i))
        stock_res = stock_return_i - X.T.dot(stock_reg)
        industry_res = industry_return_i - X.T.dot(industry_reg)
        sxx = np.sqrt(np.sum(np.square(universe_return_i - universe_return_i.mean())))
        stock_beta.loc[date] = stock_reg[1, :]
        stock_beta_std.loc[date] = stock_res.std()/sxx
        industry_beta.loc[date] = industry_reg[1, :]
        industry_beta_std.loc[date] = industry_res.std() / sxx
    stock_industry_beta = stock_beta.copy()
    stock_industry_beta[:] = np.nan
    stock_industry_beta_std = stock_industry_beta.copy()
    for date in trading_days:
        for i in range(1, 32):
            stock_industry_beta.loc[date][industry.loc[date] == i] = industry_beta.loc[date, i]
            stock_industry_beta_std.loc[date][industry.loc[date] == i] = industry_beta_std.loc[date, i]
    weight = stock_beta_std.div(stock_beta_std.add(tau * stock_industry_beta_std))
    stock_beta_adjust = stock_beta.mul(1-weight) + stock_industry_beta.mul(weight)
    factor_data = stock_beta_adjust.astype(float)
    # 保留start_date至end_date（前闭后闭）期间的数据
    factor_data = factor_data.loc[start_date: end_date, stock_list].copy()
    ########################################
    # 因子计算逻辑到此为止，以下勿随意变更 #
    ########################################
    # 行情中获取原始的索引是20180829这种整形，保存因子文件时我们要转成timestamp；reset_index后，索引会变成普通的列'index'
    factor_data = factor_data.reset_index()
    date_list = Dtk.convert_date_or_time_int_to_datetime(factor_data['index'].tolist())
    timestamp_list = [i_date.timestamp() for i_date in date_list]
    factor_data['timestamp'] = timestamp_list
    # 将timestamp设为索引
    factor_data = factor_data.set_index(['timestamp'])
    # factor_data仅保留股票列表的列，删除其他无关的列
    factor_data = factor_data[stock_list].copy()
    if not factor_file_name[-3:] == ".h5":
        factor_file_name = factor_file_name + ".h5"
    file_full_path = os.path.join(factor_file_dir_path, factor_file_name)
    pd.set_option('io.hdf.default_format', 'table')
    store = pd.HDFStore(file_full_path)
    store.put("stock_list", pd.DataFrame(stock_list, columns=['code']))
    store.put("factor", factor_data, format="table")
    store.flush()
    store.close()
    print("Factor file", factor_file_name, "was created")


def main():
    stock_code_list = Dtk.get_complete_stock_list()
    if platform.system() == 'Windows':
        save_dir = "S:\\Apollo\\Factors\\"  # 保存于S盘的地址
    else:
        save_dir = "/app/data/006566/Apollo/Factors"  # 保存于XQuant的地址
    #################################################
    # 以下3行及产生因子的函数名需要自行改写 #
    #################################################
    istart_date_int = 20130104
    iend_date_int = 20180630
    factor_name = "F_B_HBETA"
    file_name = factor_name
    factor_generator(stock_code_list, istart_date_int, iend_date_int, save_dir, file_name)


if __name__ == '__main__':
    main()
