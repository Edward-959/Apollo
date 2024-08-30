#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019/2/21 14:28
# @Author  : 011673
# update_basedata 更新need_updated_list 中的非因子
# update non_factor 根据基础非因子更新上层非因子
# update factor 更新因子值
import DataAPI.DataToolkit as Dtk
from importlib import import_module
import os
import pandas as pd
import platform
import datetime as dt
import numpy as np
import json
import pickle

# -----------------------------------------------------------------------------
# 需要更新的因子名写在下列列表中，内容依次是模块名、参数列表、因子文件名
# 若是因子，以Factor开头；若是非因子（即因子原始值），以NonFactor开头
# 这部分可以写成类似AlgoConfig的形式
factors_need_updated_list = \
    [
        ['NonFactorDailyRSI_14',{},'NF_D_RSI_14.h5'],
        ['NonFactorDailyKDJ_D',{},'NF_D_KDJ_D.h5'],
        ['NonFactorDailyOBV',{},'NF_D_OBV.h5'],
        ['NonFactorDailyma',{'n':5},'NF_D_ma_5.h5'],
        ['NonFactorDailyma',{'n':10},'NF_D_ma_10.h5'],
        ['NonFactorDailyma', {'n': 20}, 'NF_D_ma_20.h5'],
        ['NonFactorDailyma',{'n':60},'NF_D_ma_60.h5'],
        ['NonFactorDailyMincorrindex500',{},'NF_D_Mincorrindex500.h5'],
        ['NonFactorDailyMinRSI',{},'NF_D_MinRSI.h5'],
        #
        ['NonFactorDailyget_day_low', {}, 'NF_D_get_day_low.h5'],
        ['NonFactorDailyget_day_ret', {}, 'NF_D_get_day_ret.h5'],
        ['NonFactorDailyget_day_high', {}, 'NF_D_get_day_high.h5'],
        ['NonFactorDailyget_n_day_ret', {'n': 5}, 'NF_D_get_5_day_ret.h5'],
        ['NonFactorDailyget_n_day_ret', {'n': 10}, 'NF_D_get_10_day_ret.h5'],
        ['NonFactorDailyget_n_day_ret', {'n': 15}, 'NF_D_get_15_day_ret.h5'],
        ['NonFactorDailyget_n_day_ret', {'n': 20}, 'NF_D_get_20_day_ret.h5'],
        ['NonFactorDailyamt_change', {}, 'NF_D_amt_change.h5'],
        ['NonFactorDailyamt_change_n', {'n': 5}, 'NF_D_amt_change_5.h5'],
        ['NonFactorDailyamt_change_n', {'n': 10}, 'NF_D_amt_change_10.h5'],
        ['NonFactorDailyMin_close_std', {}, 'NF_D_Min_close_std.h5'],
        ['NonFactorDailyMin_early_close_std', {}, 'NF_D_Min_early_close_std.h5'],
        ['NonFactorDailyMin_early_close_ret', {}, 'NF_D_Min_early_close_ret.h5'],
        ['NonFactorDailyMin_late_close_ret', {}, 'NF_D_Min_late_close_ret.h5'],
        ['NonFactorDailyMin_late_close_std', {}, 'NF_D_Min_late_close_std.h5'],
    ]

if platform.system() == "Windows":
    stock_list = Dtk.get_complete_stock_list()
else:
    stock_list = Dtk.get_complete_stock_list()

start_date = 20180101
end_date = 20180630

if platform.system() == "Windows":
    alpha_factor_root_path = "D:\\NewFactorData"
    name_dict_path = os.path.abspath('..') + '//GPFactorGenerater//factor_report'
    method_path = os.path.abspath('..') + '//GPFactorGenerater//Method'
elif os.system("nvidia-smi") == 0:
    alpha_factor_root_path = "/data/NewFactorData"
else:
    user_id = os.environ['USER_ID']
    alpha_factor_root_path = '/app/data/666889/Apollo/AlphaFactors/'
    # 存放因子代码和对应算法以及有效因子的文件的目录
    name_dict_path = '/app/data/011673/GPLearnReport'
    # 对应计算方法目录
    method_path = '/app/data/011673/GPLearnMethod'

# ------------需要设定的部分到此为止-----------------------------------------

factor_path_dir = os.path.join(alpha_factor_root_path, "AlphaFactors")
nonfactor_path_dir = os.path.join(alpha_factor_root_path, "AlphaNonFactors")

if not os.path.exists(alpha_factor_root_path):
    os.mkdir(alpha_factor_root_path)
if not os.path.exists(factor_path_dir):
    os.mkdir(factor_path_dir)
if not os.path.exists(nonfactor_path_dir):
    os.mkdir(nonfactor_path_dir)


def check_future_data(ans_date: pd.DataFrame):
    last_series = ans_date.iloc[-1]
    if np.isnan(last_series.astype(float)).all():
        print('WARNING!!! ALL DATA IN LAST DAY IS NAN. IT MAY INCLUDE FUTURE DATA')
    return


def update_non_factor(factor_index, factor_name, base_data: pd.DataFrame):
    ans_df = get_non_factor(factor_name, base_data)
    file_name = 'NF_D_{}.h5'.format(factor_index)
    output_file_path = os.path.join(nonfactor_path_dir, file_name)
    if not os.path.exists(output_file_path):
        pd.set_option('io.hdf.default_format', 'table')
        store = pd.HDFStore(output_file_path)
        store.put("stock_list", pd.DataFrame(stock_list, columns=['code']))
        store.put("factor", ans_df, format="table")
        store.flush()
        store.close()
        print("Factor file", file_name, "was created.")
    # 如已有因子文件，则更新之；如遇日期重叠的部分，以新计算的为准
    else:
        store = pd.HDFStore(output_file_path)
        original_data_df = store.select("/factor")
        original_data_df.sort_index(inplace=True)
        if original_data_df.index[-1] < ans_df.index[0]:
            ans_df2 = pd.concat([original_data_df, ans_df])
        else:
            ans_df2 = pd.concat([original_data_df.loc[:ans_df.index[0] - 1], ans_df])
        new_stock_list = list(ans_df2.columns)
        if new_stock_list.__len__() > list(original_data_df.columns).__len__():
            store.put("stock_list", pd.DataFrame(new_stock_list, columns=['code']))
        store.put("factor", ans_df2, format="table")
        store.flush()
        store.close()
        print("Factor_file", file_name, "was updated to", end_date, ".")


def update_factor(factor_index, factor_name, base_data: pd.DataFrame):
    ans_df = get_factor(factor_index, factor_name, base_data)
    file_name = 'F_D_{}.h5'.format(factor_index)
    output_file_path = os.path.join(factor_path_dir, file_name)
    if not os.path.exists(output_file_path):
        pd.set_option('io.hdf.default_format', 'table')
        store = pd.HDFStore(output_file_path)
        store.put("stock_list", pd.DataFrame(stock_list, columns=['code']))
        store.put("factor", ans_df, format="table")
        store.flush()
        store.close()
        print("Factor file", file_name, "was created.")
    # 如已有因子文件，则更新之；如遇日期重叠的部分，以新计算的为准
    else:
        store = pd.HDFStore(output_file_path)
        original_data_df = store.select("/factor")
        original_data_df.sort_index(inplace=True)
        if original_data_df.index[-1] < ans_df.index[0]:
            ans_df2 = pd.concat([original_data_df, ans_df])
        else:
            ans_df2 = pd.concat([original_data_df.loc[:ans_df.index[0] - 1], ans_df])
        new_stock_list = list(ans_df2.columns)
        if new_stock_list.__len__() > list(original_data_df.columns).__len__():
            store.put("stock_list", pd.DataFrame(new_stock_list, columns=['code']))
        store.put("factor", ans_df2, format="table")
        store.flush()
        store.close()
        print("Factor_file", file_name, "was updated to", end_date, ".")


def get_non_factor(factor_name, base_data: pd.DataFrame):
    with open(method_path + '//' + factor_name, 'rb') as f:
        method: dict = pickle.load(f)
    basedata = base_data.loc[:, method['base_data']]
    factor = method['generater'].transform(basedata)[:, method['index_number']]
    factor = pd.Series(factor, index=basedata.index)
    factor = factor.unstack()
    factor.sort_index(inplace=True)
    return factor


def get_factor(factor_index, factor_name, base_data):
    with open(method_path + '//' + factor_name, 'rb') as f:
        method: dict = pickle.load(f)
    corresponding_non_factor_file = 'NF_D_{}.h5'.format(factor_index)
    corresponding_non_factor_path = os.path.join(alpha_factor_root_path, "AlphaNonFactors",
                                                 corresponding_non_factor_file)
    if not os.path.exists(corresponding_non_factor_path):
        print("Error: the corresponding NonFactor file", corresponding_non_factor_path, "does not exist!")
        return None
    store = pd.HDFStore(corresponding_non_factor_path, mode="r")
    factor = store.select("/factor")
    store.close()
    if method['ema_number'] != 0:
        if method['ema_number'] is not None:
            factor: pd.DataFrame = factor.ewm(com=method['ema_number']).mean()
    factor.sort_index(inplace=True)
    return factor


def get_basedata(start_date, end_date):
    result = {}
    for inform in factors_need_updated_list:
        corresponding_non_factor_file = inform[2]
        corresponding_non_factor_path = os.path.join(alpha_factor_root_path, "AlphaNonFactors",
                                                     corresponding_non_factor_file)
        if not os.path.exists(corresponding_non_factor_path):
            print("Error: the corresponding NonFactor file", corresponding_non_factor_path, "does not exist!")
            return None
        store = pd.HDFStore(corresponding_non_factor_path, mode="r")
        start_date_stamp = dt.datetime(int(str(start_date)[0:4]), int(str(start_date)[4:6]),
                                       int(str(start_date)[6:8])).timestamp()
        end_date_stamp = dt.datetime(int(str(end_date)[0:4]), int(str(end_date)[4:6]),
                                     int(str(end_date)[6:8])).timestamp()
        factor_original_df = store.select("/factor", where=[
            "index >= {} and index <= {}".format(start_date_stamp, end_date_stamp)])
        store.close()
        df = factor_original_df
        df.replace(np.inf, np.nan, inplace=True)
        df.replace(-np.inf, np.nan, inplace=True)
        df.iloc[0] = df.iloc[0].fillna(df.iloc[0].mean())
        df.fillna(method='ffill', inplace=True)
        print(corresponding_non_factor_file)
        print(df.shape)
        df = df.stack(dropna=False)
        print(df.shape)
        result[corresponding_non_factor_file] = df
    result = pd.DataFrame(result)
    result.fillna(0, inplace=True)
    result_columns = list(result.columns)
    for n, non_name in enumerate(result_columns):
        name = non_name[5:-3]
        if name[0] == 'M':
            name = name[1:]
            name = 'm' + name
        result_columns[n] = name
    result.columns = result_columns
    return result


def update_basedata():
    # 将列表中待更新的因子逐个更新
    for i_factor in factors_need_updated_list:
        t1 = dt.datetime.now()
        file_name = i_factor[2]
        if i_factor[0][0:3] == "Fac":
            factor_module = import_module("Factor." + i_factor[0])
            output_file_path = os.path.join(factor_path_dir, file_name)
            valid_start_date = start_date
        else:
            factor_module = import_module("NonFactor." + i_factor[0])
            output_file_path = os.path.join(nonfactor_path_dir, file_name)
            if not os.path.exists(output_file_path):
                if 'forward_date' not in i_factor[1].keys():
                    date = 30
                else:
                    date = i_factor[1]['forward_date']
                # 如因子值文件不存在，则考虑到ema或ma的问题，将valid_start_date再往前推30个交易日
                valid_start_date = Dtk.get_n_days_off(start_date, -date)[0]
            else:
                valid_start_date = start_date
        class_name = getattr(factor_module, i_factor[0])
        # 初始化因子类
        factor_obj = class_name(alpha_factor_root_path, stock_list, valid_start_date, end_date, i_factor[1])
        # 计算因子
        ans_df = factor_obj.factor_calc()
        check_future_data(ans_df)
        # 如没有因子文件则创设之
        if not os.path.exists(output_file_path):
            pd.set_option('io.hdf.default_format', 'table')
            store = pd.HDFStore(output_file_path)
            store.put("stock_list", pd.DataFrame(stock_list, columns=['code']))
            store.put("factor", ans_df, format="table")
            store.flush()
            store.close()
            print("Factor file", file_name, "was created.")
        # 如已有因子文件，则更新之；如遇日期重叠的部分，以新计算的为准
        else:
            store = pd.HDFStore(output_file_path)
            original_data_df = store.select("/factor")
            original_data_df.sort_index(inplace=True)
            if original_data_df.index[-1] < ans_df.index[0]:
                ans_df2 = pd.concat([original_data_df, ans_df])
            else:
                ans_df2 = pd.concat([original_data_df.loc[:ans_df.index[0] - 1], ans_df])
            new_stock_list = list(ans_df2.columns)
            if new_stock_list.__len__() > list(original_data_df.columns).__len__():
                store.put("stock_list", pd.DataFrame(new_stock_list, columns=['code']))
            store.put("factor", ans_df2, format="table")
            store.flush()
            store.close()
            print("Factor_file", file_name, "was updated to", end_date, ".")
        t2 = dt.datetime.now()
        print(i_factor[0], t2 - t1)


def main():
    update_basedata()
    basedata = get_basedata(start_date, end_date)
    print('BASEDATA DONE')
    with open(name_dict_path + '/name_dict_backup.json', 'r') as f:
        name_dict: dict = json.load(f)
    for name_key in name_dict['selected_factor']:
        if 'GPY' in name_key:
            print(name_key)
            # if name_key not in ['GPY_184', 'GPY_208', 'GPY_265', 'GPY_283', 'GPY_269']: # 开发注释和更新无关
            update_non_factor(name_key, name_dict[name_key], basedata)
            update_factor(name_key, name_dict[name_key], basedata)


if __name__ == '__main__':
    main()
