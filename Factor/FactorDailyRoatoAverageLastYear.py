# -*- coding: utf-8 -*-
"""
@author: 011672, 2019/4/10
依据当年因子值与过去一年平均因子值之比的方式求取因子值
使用了落地库指标roa2
"""

import DataAPI.DataToolkit as Dtk
from Factor.DailyFactorBase import DailyFactorBase
import numpy as np
from xquant.multifactor.IO.IO import read_data
from copy import deepcopy
import pandas as pd
import os



def back_fill(df_fill, df_qfa, df_ann, fill_na=True):
    # 将财务数据按交易日向后填充到complete_stock_list的空表内
    """
    :param df_fill: 全样本股票列表及交易日的空表
    :param df_qfa: 财务数据表
    :param df_ann: 实际披露日期
    :param fill_na: 是否将nan值填充为0。目前万得数据库中一个报表数据不存在或值为0都会被填充为nan
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
    stock_listing_date = get_stock_listing_date()
    for col_i, col in enumerate(columns):
        # 如果个股在报表内
        if col in df_ann and col in df_qfa:
            listing_date = stock_listing_date.at[col, 'Listing_date']
            s_ann = df_ann[col].values
            s_ann = np.array([int(x) for x in s_ann])
            s_qfa = df_qfa[col].values
            # 将报告期list和数据list置于规定的开始日期和结束日期之内
            s_ann_temp = s_ann[(s_ann <= end_date) & ((s_ann >= start_date) & (s_ann > listing_date))]
            s_qfa_temp = s_qfa[(s_ann <= end_date) & ((s_ann >= start_date) & (s_ann > listing_date))]
            for idate in range(len(s_ann_temp)):
                # 如果非nan且非inf，则填充到披露期那一天，如果披露期非交易日，则顺延到最近的一个交易日
                if not math.isnan(s_ann_temp[idate]) and not math.isnan(s_qfa_temp[idate]) and not math.isinf(
                        s_qfa_temp[idate]):
                    ann_date = int(s_ann_temp[idate])
                    # 因为要填充到下一个发布日，所以首先判断是不是最后一个发布日
                    if idate < len(s_ann_temp) - 1:
                        # 下一个发布日的int
                        ann_date_next = int(s_ann_temp[idate + 1])
                        # 如果下一个发布日之后还有交易日的话（避免出现下一个发布日与最后一个交易日重叠的情况）
                        if np.where(trading_days_np > ann_date_next)[0].tolist() != []:
                            # 当前发布日在交易日list中的位置
                            ann_date_id = np.where(trading_days_np > ann_date)[0][0]
                            # 下一个发布日在交易日list中的位置
                            ann_date_next_id = np.where(trading_days_np > ann_date_next)[0][0]
                            # 将此两个位置之间的交易日填充为新值
                            df_fill_np[ann_date_id:ann_date_next_id, col_i] = s_qfa_temp[idate]
                        # 如果下一个交易日之后没有交易日了，也要进行填充，不然在下一步也不会填充了。
                        else:
                            # 有时年报和下一年一季报在同一天发布
                            if ann_date != ann_date_next:
                                # 当前发布日在交易日list中的位置
                                ann_date_id = np.where(trading_days_np > ann_date)[0][0]
                                # 将当前交易日之后的交易日填充
                                df_fill_np[ann_date_id:, col_i] = s_qfa_temp[idate]
                    # 如果到了最后一个发布日
                    else:
                        # 如果当前发布日之后还有交易日的话（避免出现当前发布日与最后一个交易日重叠的情况）
                        if np.where(trading_days_np > ann_date)[0].tolist() != []:
                            # 当前发布日在交易日list中的位置
                            ann_date_id = np.where(trading_days_np > ann_date)[0][0]
                            # 将当前交易日之后的交易日填充
                            df_fill_np[ann_date_id:, col_i] = s_qfa_temp[idate]
                        else:
                            pass
    df_fill_new = pd.DataFrame(df_fill_np, index=index, columns=columns)
    df_fill_new.sort_index(inplace=True)
    if fill_na:
        df_fill_new = df_fill_new.fillna(0)
    return df_fill_new


def start_date_backfill(start_date_int, back_years=0):
    # by 011672 - 向前回溯2个季度
    # revised by 006566 on 2019/4/4 - 添加back_years
    start_month = int(str(start_date_int)[4:6])
    start_year = int(str(start_date_int)[0:4])
    if 1 <= start_month <= 3:
        last_report_date = int(str(start_year - back_years - 1) + '0630')
    elif 4 <= start_month <= 6:
        last_report_date = int(str(start_year - back_years - 1) + '0930')
    elif 7 <= start_month <= 9:
        last_report_date = int(str(start_year - back_years - 1) + '1231')
    elif 10 <= start_month <= 12:
        last_report_date = int(str(start_year - back_years) + '0331')
    else:
        raise Exception('Start date error')
    last_report_date = Dtk.get_n_days_off(last_report_date, -1)[0]
    return last_report_date




class FactorDailyDivyieldtoAverageLastYear(DailyFactorBase):
    # 这个因子没有参数，但也须在初始化时预留一个"params"
    def __init__(self, alpha_factor_root_path, stock_list, start_date_int, end_date_int, params):
        super().__init__(alpha_factor_root_path)
        self.stock_list = stock_list
        self.start_date = start_date_int
        self.end_date = end_date_int

    def factor_calc(self):     
        info_type = "roa2"  
        factor_original_df = Dtk.get_panel_daily_info(self.stock_list, self.start_date, self.end_date, info_type)
        data_path = r"/app/data/wdb_h5/WIND/FDD_CHINA_STOCK_QUARTERLY_WIND/FDD_CHINA_STOCK_QUARTERLY_WIND.h5"
        store = pd.HDFStore(data_path, mode='r')
        data_df_info = store.select("/" + info_type)
        ann_df = store.select("/stm_issuingdate")
        store.close()
        ann_df = Dtk.unfold_df(ann_df)
        data_df_info = Dtk.unfold_df(data_df_info)
        ann_df = Dtk.convert_df_index_type(ann_df, 'timestamp2', 'date_int')
        data_df_info = Dtk.convert_df_index_type(data_df_info, 'timestamp2', 'date_int')
        ann_df = ann_df.fillna(0)
        data_df_info = data_df_info.reindex(columns=ann_df.columns)
        last_report_date = start_date_backfill(self.start_date, 2)
        trading_day_1 = Dtk.get_trading_day(last_report_date, self.end_date)
        data_df_raw = pd.DataFrame(index=trading_day_1, columns=self.stock_list)
        
        divyield_average = (data_df_info.shift(1) + data_df_info.shift(2) + data_df_info.shift(3) + data_df_info)/4
        divyield_average = Dtk.back_fill(data_df_raw, divyield_average, ann_df)
        divyield_average = divyield_average.reindex(index=factor_original_df.index, columns=factor_original_df.columns)
        ans_df = factor_original_df / divyield_average                                                                
        # ----以下勿改动----
        ans_df = ans_df.loc[self.start_date: self.end_date]
        ans_df = Dtk.convert_df_index_type(ans_df, 'date_int', 'timestamp')
        return ans_df
