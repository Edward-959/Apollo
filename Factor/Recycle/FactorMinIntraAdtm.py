# -*- coding: utf-8 -*-
"""
@author: 006688
ADTM（动态买卖气指标，超买超卖）是指动态买卖气指标，用开盘价的向上波动幅度和向下波动幅度的距离差值来描述人气高低的指标。
DTM=IF(OPEN<=REF(OPEN,1),0,MAX((HIGH-OPEN),(OPEN-REF(OPEN,1))))
DBM=IF(OPEN>=REF(OPEN,1),0,MAX((OPEN-LOW),(REF(OPEN,1)-OPEN)))
STM=SUM(DTM,P)
SBM=SUM(DBM,P)
ADTM=IF(STM>SBM,(STM-SBM)/STM,IF(STM=SBM,0,(STM-SBM)/SBM))
"""

from Factor.MinFactorBase import MinFactorBase
import pandas as pd
import numpy as np
import logging
from typing import List
import platform
import DataAPI.DataToolkit as Dtk


class FactorMinIntraAdtm(MinFactorBase):
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
            stock_minute_open = stock_minute_data['open'].unstack()
            stock_minute_high = stock_minute_data['high'].unstack()
            stock_minute_low = stock_minute_data['low'].unstack()
            stock_minute_diff_open = stock_minute_open - stock_minute_open.shift(1, axis=1)
            stock_minute_dtm = np.maximum(stock_minute_high - stock_minute_open, abs(stock_minute_diff_open))
            stock_minute_dtm[stock_minute_diff_open <= 0] = 0
            stock_minute_dbm = np.maximum(stock_minute_open - stock_minute_low, abs(stock_minute_diff_open))
            stock_minute_dbm[stock_minute_diff_open >= 0] = 0
            stock_minute_stm = stock_minute_dtm.rolling(self.__lag, min_periods=1, axis=1).sum()
            stock_minute_sbm = stock_minute_dbm.rolling(self.__lag, min_periods=1, axis=1).sum()
            stock_minute_adtm = (stock_minute_stm - stock_minute_sbm) / np.maximum(stock_minute_stm, stock_minute_sbm)
            stock_minute_adtm[stock_minute_stm == stock_minute_sbm] = 0
            stock_minute_data[code] = stock_minute_adtm.stack()
            factor_data = stock_minute_data.loc[start: end][code]
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
    istart_date_int = 20180601
    iend_date_int = 20180630
    for lag in [30]:
        factor_name = "F_M_IntraAdtm" + str(lag)  # 这个因子名可以加各种后缀，用于和相近的因子做区分
        file_name = factor_name
        factor_generator = FactorMinIntraAdtm(codes=stock_code_list, start_date_int=istart_date_int,
                                              end_date_int=iend_date_int, name=file_name, save_path=save_dir, lag=lag)
        factor_generator.launch()
    logging.info("program is stopped")


if __name__ == '__main__':
    main()
