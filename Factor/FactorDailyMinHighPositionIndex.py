# -*- coding: utf-8 -*-
"""
Created on 2018/11/15 19:23

@author: 006547
"""
from Factor.MinFactorBase import MinFactorBase
import pandas as pd
import numpy as np
import logging
from typing import List
import platform
import DataAPI.DataToolkit as Dtk


class FactorDailyMinHighPositionIndex(MinFactorBase):
    def __init__(self, codes: List[str] = ..., start_date_int: int = ..., end_date_int: int = ...,
                 save_path: str = ..., name: str = ..., lag: int = ...):
        super().__init__(codes, start_date_int, end_date_int, save_path, name)
        self.__lag = lag

    def single_stock_factor_generator(self, code: str = ..., start: int = ..., end: int = ...):
        ############################################
        # 以下是因子计算逻辑的部分，需要用户自定义 #
        ############################################
        stock_minute_data = Dtk.get_single_stock_minute_data(code, start, end, fill_nan=True, append_pre_close=False,
                                                             adj_type='None', drop_nan=False, full_length_padding=True)
        if stock_minute_data.columns.__len__() > 0:  # 如可正常取到行情DataFrame
            stock_minute_high = stock_minute_data['high'].unstack()
            index_max = np.argmax(stock_minute_high.values[:, 1:], 1)
            is_nan = np.isnan(np.array([stock_minute_high.values[i, index_max[i]] for i in range(index_max.__len__())]))
            is_nan = np.array(is_nan, dtype=int)
            is_nan = (is_nan - 1) * -1
            temp = pd.DataFrame(is_nan, index=list(stock_minute_high.index), columns=[code])
            factor_data = pd.DataFrame(index_max, index=list(stock_minute_high.index), columns=[code])
            factor_data = factor_data * temp / temp
        ########################################
        # 因子计算逻辑到此为止，以下勿随意变更 #
        ########################################
        else:  # 如取到的行情的DataFrame为空，则自造一个全为nan的DataFrame
            date_list = Dtk.get_trading_day(start, end)
            factor_data = pd.DataFrame(index=date_list)  # 新建一个空的DataFrame, 且先设好了索引
            temp_array = np.empty(shape=[date_list.__len__(), ])
            temp_array[:] = np.nan
            factor_data[code] = temp_array
        # 因子应当以timestamp作为索引
        factor_data = factor_data.reset_index()
        date_list = Dtk.convert_date_or_time_int_to_datetime(factor_data['index'].tolist())
        timestamp_list = [i_date.timestamp() for i_date in date_list]
        factor_data['index'] = timestamp_list
        factor_data = factor_data.set_index(['index'])
        factor_data = factor_data[[code]].copy()
        logging.info("finished calc {}".format(code))
        return factor_data


def main():
    logging.basicConfig(level=logging.INFO)
    stock_code_list = Dtk.get_complete_stock_list()
    if platform.system() == 'Windows':
        save_dir = "S:\\Apollo\\Factors\\"  # 保存于S盘的地址
    else:
        save_dir = "/app/data/006566/Apollo/Factors"  # 保存于XQuant的地址
    ###############################################
    # 以下3行及factor_generator的类名需要自行改写 #
    ###############################################
    istart_date_int = 20141201
    iend_date_int = 20180630
    for lag in [30]:
        factor_name = "F_D_MinHighPositionIndex"   # 这个因子名可以加各种后缀，用于和相近的因子做区分
        file_name = factor_name
        factor_generator = FactorDailyMinHighPositionIndex(codes=stock_code_list, start_date_int=istart_date_int,
                                                           end_date_int=iend_date_int, name=file_name, save_path=save_dir, lag=lag)
        factor_generator.launch()
    logging.info("program is stopped")


if __name__ == '__main__':
    main()
