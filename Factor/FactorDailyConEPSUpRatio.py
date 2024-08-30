# -*- coding: utf-8 -*-
"""
created on 2018/12/13
revised on 2019/2/27
revised on 2019/3/20: 更改朝阳永续数据路径
@author: 006566
(个股t日内业绩上调数 - 个股t日内业绩下调数) / (个股t日内业绩上调数 + 个股t日内业绩下调数)
t 只能等于7, 30或90
"""

import DataAPI.DataToolkit as Dtk
from Factor.DailyFactorBase import DailyFactorBase
import numpy as np
import pandas as pd
import os
import platform


class FactorDailyConEPSUpRatio(DailyFactorBase):
    # 这个因子没有参数，但也须在初始化时预留一个"params"
    def __init__(self, alpha_factor_root_path, stock_list, start_date_int, end_date_int, params):
        super().__init__(alpha_factor_root_path)
        self.stock_list = stock_list
        self.start_date = start_date_int
        self.end_date = end_date_int
        self.t_days = params["t_days"]

    def factor_calc(self):
        table_name = "stock_report_adjustment"
        if platform.system() == "Windows":
            gogoal_data_path = r"S:\xquant_data_backup\backup\fcd\DAILY\HTSC"
            table_path = os.path.join(gogoal_data_path, str(table_name + ".h5"))
        else:
            gogoal_data_path = "/app/data/wdb_h5/WIND/"
            table_path = os.path.join(gogoal_data_path, table_name, str(table_name + ".h5"))

        start_line = 0
        chunk_size = 100000
        start_date_timestamp = pd.Timestamp(str(self.start_date))
        end_date_timestamp = pd.Timestamp(str(self.end_date))
        store = pd.HDFStore(table_path, mode="r")

        store_info = store.info()  # 获取表信息
        rows_start = store_info.find("nrows->")
        rows_end = store_info.find(",ncols")
        df_total_rows = int(store_info[rows_start+7: rows_end])  # 得到表的总行数

        def get_a_chunk_of_df(store0, table_name0, chunk_size0, start_line0, key_name0):
            unique_date_list0 = []
            lines_per_day0 = 0
            for data_frame in store0.select(table_name0, chunksize=chunk_size0, start=start_line0, columns=[key_name0]):
                unique_date_list0 = list(set(data_frame.index.get_level_values(level=0)))
                unique_date_list0.sort()
                lines_per_day0 = data_frame.__len__() / unique_date_list0.__len__()
                break
            return unique_date_list0, lines_per_day0

        key_name_up = str("UP_NUMBER" + str(self.t_days))
        key_name_down = str("DOWN_NUMBER" + str(self.t_days))
        unique_date_list, lines_per_day = get_a_chunk_of_df(store, table_name, chunk_size, start_line, key_name_up)
        diff_days = (start_date_timestamp - unique_date_list[-1]).days
        backward_days = 20
        out_of_range = False
        while diff_days > 20:  # 逼近到前20天即可
            distance_in_lines = diff_days * lines_per_day
            if not out_of_range:
                start_line += int(distance_in_lines)
            else:
                start_line = int((df_total_rows + start_line) / 2)
            if start_line > df_total_rows:
                start_line = int(df_total_rows - backward_days * lines_per_day)
                out_of_range = True
            unique_date_list, lines_per_day = get_a_chunk_of_df(store, table_name, chunk_size, start_line, key_name_up)
            diff_days = (start_date_timestamp - unique_date_list[-1]).days
        if diff_days < 0:  # 若超过了，再返回一点
            while unique_date_list[0] > start_date_timestamp:
                start_line -= int(chunk_size)
                unique_date_list, lines_per_day = get_a_chunk_of_df(store, table_name, chunk_size, start_line,
                                                                    key_name_up)
        # 至此得到合适的start_line
        raw_data_df = pd.DataFrame()
        for df0 in store.select(table_name, chunksize=100000, start=start_line,
                                columns=['RPT_DATE', key_name_up, key_name_down]):
            if df0.index.get_level_values(level=0)[-1] < start_date_timestamp:
                continue
            if df0.index.get_level_values(level=0)[0] > end_date_timestamp:
                break
            df1 = df0.copy()
            df1['date_timestamp'] = df1.index.get_level_values(level=0)
            unique_date = list(set(list(df1['date_timestamp'])))
            report_year_dict = {}
            # 若在3月以后，则采用下一年的预期值，否则用当年预期值；另外，CON_TYPE选用1（加权计算）或2（手工计算）的
            for i_date in unique_date:
                if i_date <= pd.Timestamp(str(i_date.year) + "0228"):
                    report_year_dict.update({i_date: i_date.year})
                else:
                    report_year_dict.update({i_date: i_date.year + 1})
            date_time_stamp_list = df1['date_timestamp']
            report_year_list = [report_year_dict[i] for i in date_time_stamp_list]
            df1['report_year'] = report_year_list
            df1 = df1.loc[df1['RPT_DATE'] == df1['report_year']]
            df1 = df1[[key_name_up, key_name_down]]
            raw_data_df = raw_data_df.append(df1)
        store.close()

        def df_unfold_fun(input_df, column_stock_list):
            output_df = input_df.unstack()
            output_df = Dtk.convert_df_index_type(output_df, 'timestamp2', 'date_int')
            if set(column_stock_list).issubset(set(output_df.columns)):  # 如column_stock_list是input_df的子集
                output_df = output_df[column_stock_list]  # 使输出的列名顺序等于输入的顺序
            else:
                no_data_column_stock_list = list(set(column_stock_list) - set(output_df.columns))
                df_columns = list(output_df.columns)
                df_columns.extend(no_data_column_stock_list)
                value_array = output_df.values
                empty_array = np.empty((value_array.shape[0], no_data_column_stock_list.__len__(),))
                empty_array[:] = np.nan
                value_array = np.hstack([value_array, empty_array])
                output_df = pd.DataFrame(value_array, index=output_df.index, columns=df_columns)
                output_df = output_df[column_stock_list]
            return output_df

        rating_up_df = raw_data_df[key_name_up]
        rating_down_df = raw_data_df[key_name_down]
        rating_up_df1 = df_unfold_fun(rating_up_df, self.stock_list)
        rating_down_df1 = df_unfold_fun(rating_down_df, self.stock_list)

        factor_data = (rating_up_df1 - rating_down_df1) / (rating_up_df1 + rating_down_df1)
        factor_data = factor_data.replace(np.inf, np.nan)
        factor_data.index.name = 'index'
        ans_df = factor_data

        # ----以下勿改动----
        ans_df = ans_df.loc[self.start_date: self.end_date]
        ans_df = Dtk.convert_df_index_type(ans_df, 'date_int', 'timestamp')
        return ans_df
