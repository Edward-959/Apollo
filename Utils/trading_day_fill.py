# -*- coding: utf-8 -*-
"""
Created on 2018/11/19
@author: 011669
"""
import pickle
import pandas as pd
import numpy as np
import DataAPI.DataToolkit as Dtk
import datetime as dt
import os


def datetime2int(date_time):
    temp = str(date_time)
    date_int = int(temp[0:4]) * 10000 + int(temp[5:7]) * 100 + int(temp[8:10])
    return date_int


# 将财务数据按交易日向后填充到complete_stock_list的空表内
def back_fill(df_fill, df_qfa, df_ann, df_val_mv):
    """
        :param df_fill: 全样本股票列表及交易日的空表
        :param df_qfa: 财务数据表
        :param df_ann: 实际披露日期
        :param df_val_mv: 每日市值，如果为nan说明未上市/已退市
        :return: 将df_fill填充完成后的Dataframe
        """
    import math
    df_fill_new = df_fill.copy()
    trading_days = list(df_fill_new.index)
    start_date = trading_days[0]
    end_date = trading_days[-1]
    trading_days_np = np.array(trading_days)
    columns = df_fill_new.columns
    index = df_fill_new.index
    df_fill_np = df_fill_new.values  # 计算时涉及到循环，采用numpy计算提高速度
    for col_i, col in enumerate(columns):
        if col in df_ann and col in df_qfa:  # 如果个股在报表内
            s_ann = df_ann[col].values
            s_qfa = df_qfa[col].values
            s_ann_temp = s_ann[(s_ann <= end_date) & (s_ann >= start_date)]
            s_qfa_temp = s_qfa[(s_ann <= end_date) & (s_ann >= start_date)]
            for idate in range(len(s_ann_temp)):
                if not math.isnan(s_ann_temp[idate]) and not math.isnan(s_qfa_temp[idate]) and not math.isinf(
                        s_qfa_temp[idate]):  # 如果非nan且非inf，则填充到披露期那一天，如果披露期非交易日，则顺延到最近的一个交易日
                    ann_date = int(s_ann_temp[idate])
                    ann_date_id = np.where(trading_days_np >= ann_date)[0][0]
                    df_fill_np[ann_date_id, col_i] = s_qfa_temp[idate]
    df_fill_new = pd.DataFrame(df_fill_np, index=index, columns=columns)
    df_fill_new.sort_index(inplace=True)
    df_fill_new.fillna(method='ffill', inplace=True)  # 向后填充财务数据
    df_fill_new_2 = df_fill_new * df_val_mv / df_val_mv  # 通过总市值的dataframe来剔掉非上市日期
    trading_days_2 = Dtk.get_trading_day(20140101, 20180630)
    df_fill_new_2 = df_fill_new_2.loc[trading_days_2[0]: trading_days_2[-1], :]
    return df_fill_new_2


# 用于披露日期作为index，所需数据为col的情况，目前仅用于股东户数那张表(一般不需要用）
def back_fill2(df_fill, df_merge, df_val_mv):
    """
        :param df_fill: 全样本股票列表及交易日的空表
        :param df_merge: index即为实际披露日期的报表（df_qfa和df_ann在一张表上)
        :param df_val_mv: 每日市值，如果为nan说明未上市/已退市
        :return: 将df_fill填充完成后的Dataframe
        """
    df_fill_new = df_fill.copy()
    df_merge_reset = df_merge.reset_index()
    df_merge_reset['dt'] = df_merge_reset['dt'].apply(lambda x: datetime2int(x))  # 将dt转成int格式
    trading_days = list(df_fill_new.index)

    start_date = trading_days[0]
    end_date = trading_days[-1]
    trading_days_np = np.array(trading_days)
    columns = df_fill_new.columns
    index = df_fill_new.index
    df_fill_np = df_fill_new.values
    for col_i, col in enumerate(columns):
        if col in df_merge_reset:
            s_merge = df_merge_reset.loc[:, ['dt', col]].dropna().values
            s_merge_temp = s_merge[(s_merge[:, 0] <= end_date) & (s_merge[:, 0] >= start_date), :]
            for idate in range(s_merge_temp.__len__()):
                ann_date = int(s_merge_temp[idate, 0])
                ann_date_id = np.where(trading_days_np >= ann_date)[0][0]
                df_fill_np[ann_date_id, col_i] = int(s_merge_temp[idate, 1])
    df_fill_new = pd.DataFrame(df_fill_np, index=index, columns=columns)
    df_fill_new_2 = df_fill_new.fillna(method='ffill')
    df_fill_new_2 = df_fill_new_2 * df_val_mv / df_val_mv
    trading_days_2 = Dtk.get_trading_day(20140101, 20180630)
    df_fill_new_2 = df_fill_new_2.loc[trading_days_2[0]: trading_days_2[-1], :]
    return df_fill_new_2


# get_hdf_field是将对应表里的字段保存成pkl文件，此处需要注意
# 1) 绝大多数财务数据表的index均为报告期的timestamp，col里都有字段'ann_dt'，即财务数据披露期
# 2) consoli_date代表报表是否存在多种报表类型（合并或母公司报表等），如果存在则为True，此时默认取合并报表值，可以根据自身要求来取
# 3) 所有col均为其字段的大写
def get_hdf_field(path, field_list, trading_days, stock_code_list, consoli_state=False):
    store = pd.HDFStore(path + '.h5', mode='r')
    path_name = path
    all_pandas = store.select("/" + "WIND" + "_" + path)
    store.close()
    if consoli_state:
        all_pandas = all_pandas[all_pandas['STATEMENT_TYPE'] == 408001000]  # 取合并报表的值
    for list_i in field_list:
        LIST_I = list_i.upper()  # col名为字段名称大写
        if LIST_I in all_pandas:
            pd_field = all_pandas[LIST_I]  # pd_field为一个timestamp+股票名的双索引，需要unstack后方便处理
            if not pd_field.index.is_unique:   # 部分字段在退市或未上市新股里存在出现两个相同index的情况，此时无法直接unstack，因为对输出值没有影响，保留其中第一项即可
                pd_field = pd_field[~pd_field.index.duplicated(keep='first')]
            pd_field_unstack = pd_field.unstack().reset_index()
            pd_field_unstack['dt'] = pd_field_unstack['dt'].apply(lambda x:  datetime2int(x))  # dt字段格式转为int型日期，与目前数据统一
            pd_field_unstack.index = pd_field_unstack['dt']
            pd_field_unstack.sort_index(inplace=True)  # 原表内数据日期存在错乱，需要先排序
            col_all = pd_field_unstack.columns.tolist()
            stock_code_all = list(set(col_all).intersection(stock_code_list))  # 取complete_stock_list和表内所有股票的交集
            pd_field_unstack_needed = pd_field_unstack.loc[(pd_field_unstack.index >= trading_days[0]) & (
              pd_field_unstack.index <= trading_days[-1]), stock_code_all]
            if not os.path.exists(path_name):
                os.makedirs(path_name)
            with open(path_name + '/' + list_i + '.pkl', 'wb') as f:
                pickle.dump(pd_field_unstack_needed, f)
        else:
            print('missing ',  list_i)
    return


# 根据每季度的财务报表计算单季度财务数据(仅为可累加的数据，如营业收入，roe,etc.)
# 如果报告期为全年第一个数据，则该数据即为单季当期数据，否则用当期-上一期即得到单季数据
def qfa_adjust(path, field_list):
    for list_i in field_list:
        file_name = path + '/' + list_i + '.pkl'
        with open(file_name, 'rb') as f:
            pd_field = pickle.load(f)
        pd_raw: pd.DataFrame = pd_field.copy()
        pd_raw[:] = np.nan
        index_temp = pd_field.index.tolist()
        for i in range(1, len(index_temp)):
            if int(index_temp[i]/10000) == int(index_temp[i - 1]/10000):
                pd_raw.loc[index_temp[i], :] = pd_field.loc[index_temp[i], :] - pd_field.loc[index_temp[i - 1], :]
            else:
                pd_raw.loc[index_temp[i], :] = pd_field.loc[index_temp[i], :]
        with open(path + '/' + list_i + '_qfa.pkl', 'wb') as f:
            pickle.dump(pd_raw, f)
    return


# 计算单季财务数据增速
# 报告期数据/去年同一季度报告期数据即为单季增速
def qfa_yoy_growth(path, field_list):
    for list_i in field_list:
        file_name = path + '/' + list_i + '_qfa.pkl'
        with open(file_name, 'rb') as f:
            pd_field = pickle.load(f)
        pd_growth: pd.DataFrame = pd_field / pd_field.shift(4) - 1
        with open(path + '/' + list_i + '_qfa_yoy.pkl', 'wb') as f:
            pickle.dump(pd_growth, f)
    return


# 将财务数据填充到剩下所有交易日,方法默认填1
def trading_day_fill(path, field_list, appendix, pd_raw, pd_val_mv, method='1'):
    for list_i in field_list:
        file_name = path + '/' + list_i + appendix + '.pkl'
        with open(file_name, 'rb') as f:
            pd_field = pickle.load(f)
        if method == '1':
            ann_dt_name = path + '/' + 'ann_dt.pkl'
            with open(ann_dt_name, 'rb') as f:
                pd_ann_dt = pickle.load(f)
            pd_fill = back_fill(pd_raw, pd_field, pd_ann_dt, pd_val_mv)
        else:
            pd_fill = back_fill2(pd_raw, pd_field, pd_val_mv)
        with open(path + '/' + list_i + '_filled.pkl', 'wb') as f:
            pickle.dump(pd_fill, f)
    return

if __name__ == '__main__':
    trading_days = Dtk.get_trading_day(20120623, 20180630)  # 为取到所需的报表数据，多截1.5年的时间
    stock_code_list = Dtk.get_complete_stock_list()
    pd_raw = pd.DataFrame(data=None, index=trading_days, columns=stock_code_list)  # 先建一张空表

    with open('s_val_mv' + '.pkl', 'rb') as f:  # 总市值是一个当且仅当上市后和退市前才存在值的字段，取完数据后*总市值/总市值可以过滤掉非上市期间的数据
        s_val_mv = pickle.load(f)
    # h5字段参考'S:\Apollo\for 基本面因子\Wind资讯量化研究数据库V4.8--201305.pdf'里的说明
    # 字段所在表是120张落地数据库，目前还没有实现每日更新，保存在'http://eip.htsc.com.cn/xquant/ace/gpu/wdb_h5/WIND/'里

    get_hdf_field('AShareIncome', ['ann_dt', 'net_profit_after_ded_nr_lp'], trading_days, stock_code_list,  True)
    qfa_adjust('AShareIncome', ['net_profit_after_ded_nr_lp'])
    qfa_yoy_growth('AShareIncome', ['net_profit_after_ded_nr_lp'])
    trading_day_fill('AShareIncome', ['net_profit_after_ded_nr_lp'], '_qfa_yoy', pd_raw, s_val_mv)