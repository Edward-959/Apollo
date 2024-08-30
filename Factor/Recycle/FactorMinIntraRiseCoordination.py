# -*- coding: utf-8 -*-
# @Time    : 2018/9/3 18:00
# @Author  : 011673
# @File    : FactorMinIntraRiseCoordination.py

from Factor.MinFactorBase import MinFactorBase
import pandas as pd
import numpy as np
import logging
from typing import List
import platform
import DataAPI.DataToolkit as Dtk
import copy


class FactorMinRiseCor(MinFactorBase):
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
        N=self.__lag
        if stock_minute_data.columns.__len__() > 0:  # 如可正常取到行情DataFrame
            def cor_func(serie):
                sorted_value = copy.deepcopy(serie)
                sorted_value.sort()
                return np.corrcoef(serie, sorted_value)[0][1]

            close = stock_minute_data['close']
            close = close.unstack().T
            close_list = close.rolling(window=N, min_periods=10)
            cor_value = close_list.apply(func=cor_func)
            result=cor_value.fillna(0)
            result=result.T.stack()
            factor_data = pd.DataFrame(result, index=result.index, columns=[code])

        ########################################
        # 因子计算逻辑到此为止，以下勿随意变更 #
        ########################################
        else:  # 如取到的行情的DataFrame为空，则自造一个全为nan的DataFrame
            date_list = Dtk.get_trading_day(start, end)
            complete_minute_list = Dtk.get_complete_minute_list()  # 每天242根完整的分钟Bar线的分钟值
            i_stock_minute_data_full_length = date_list.__len__() * 242
            index_tuple = [np.repeat(date_list, len(complete_minute_list)), complete_minute_list * len(date_list)]
            mi_index = pd.MultiIndex.from_tuples(list(zip(*index_tuple)), names=['dt', 'minute'])
            factor_data = pd.DataFrame(index=mi_index)  # 新建一个空的DataFrame, 且先设好了索引
            temp_array = np.empty(shape=[i_stock_minute_data_full_length, ])
            temp_array[:] = np.nan
            factor_data[code] = temp_array
        if factor_data.index.names[0] == 'dt':  # 将index变为普通的列，以便得到日期'dt'和分钟'minute'用于后续计算
            factor_data = factor_data.reset_index()
        # 拼接计算14位数的datetime, 格式例如20180802145500
        factor_data['datetime'] = factor_data['dt'] * 1000000 + factor_data['minute'] * 100
        # 将14位数的datetime转为dt.datetime
        date_time_dt = Dtk.convert_date_or_time_int_to_datetime(factor_data['datetime'].tolist())
        # 将dt.datetime转为timestamp
        timestamp_list = [i_date_time.timestamp() for i_date_time in date_time_dt]
        factor_data['timestamp'] = timestamp_list
        # 将timestamp设为索引
        factor_data = factor_data.set_index(['timestamp'])
        # DataFrame仅保留因子值一列，作为多进程任务的返回值
        factor_data = factor_data[[code]].copy()
        logging.info("finished calc {}".format(code))
        return factor_data

def test():
    code = '000002.SZ'
    start = 20170601
    end = 20170630
    factor_name = "F_M_Pvt"
    file_name = str(factor_name + '_' + str(start) + "_" + str(end))
    if platform.system() == 'Windows':
        save_dir = "S:\\Apollo\\Factors\\"  # 保存于S盘的地址
    else:
        save_dir = "/app/data/006566/Apollo/Factors"  # 保存于XQuant的地址
    factor_generator = FactorMinRiseCor(codes=[code], start_date_int=start,
                                         end_date_int=end, name=file_name, save_path=save_dir, lag=20)
    factor_generator.single_stock_factor_generator(code, start, end)

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
    istart_date_int = 20150701
    iend_date_int = 20180630
    for lag in [20,30,40]:
        factor_name = "F_M_IntraRiseCor" + str(lag)  # 这个因子名可以加各种后缀，用于和相近的因子做区分
        file_name = factor_name
        factor_generator = FactorMinRiseCor(codes=stock_code_list, start_date_int=istart_date_int,
                                             end_date_int=iend_date_int, name=file_name, save_path=save_dir, lag=lag)
        factor_generator.launch()
    logging.info("program is stopped")


if __name__ == '__main__':
    # main()
    test()