# -*- coding: utf-8 -*-
"""
Created on 2018/11/27
@author: 006688
一致卖出交易因子：下跌的实体K线成交量除以当日总成交量得到当日因子值，再求移动平均
"""
from Factor.MinFactorBase import MinFactorBase
import pandas as pd
import numpy as np
import logging
from typing import List
import platform
import DataAPI.DataToolkit as Dtk
import os


class FactorDailyMinNegativeConsistentVolume(MinFactorBase):
    def __init__(self, codes: List[str] = ..., start_date_int: int = ..., end_date_int: int = ...,
                 save_path: str = ..., name: str = ..., n: int = ...):
        super().__init__(codes, start_date_int, end_date_int, save_path, name)
        self.__n = n
        # 所有要用到的日级别信息，应在此获取
        self.__complete_minute_list = Dtk.get_complete_minute_list()
        self.__start_date_minus_n_2 = Dtk.get_n_days_off(start_date_int, -(n + 2))[0]

    def single_stock_factor_generator(self, code: str = ..., start: int = ..., end: int = ...):
        stock_minute_data = Dtk.get_single_stock_minute_data(code, self.__start_date_minus_n_2, end, fill_nan=True,
                                                             append_pre_close=False, adj_type='None', drop_nan=False,
                                                             full_length_padding=True)
        data_check_df = stock_minute_data.dropna()  # 用于检查行情的完整性
        if stock_minute_data.columns.__len__() > 0 and data_check_df.__len__() > 0:  # 如可正常取到行情DataFrame
            ############################################
            # 以下是数据计算逻辑的部分，需要用户自定义 #
            # 计算数据时，最后应得到factor_data这个对象，类型应当是DataFrame，涵盖的时间段是start至end（前闭后闭）；
            # factor_data的因子值一列，应当以股票代码为列名；
            # 最后factor_data的索引，应当从原始分钟数据中获得的dt，即start至end，内容的格式是20180904这种8位数字
            ############################################
            stock_minute_open = stock_minute_data['open'].unstack()
            stock_minute_close = stock_minute_data['close'].unstack()
            stock_minute_high = stock_minute_data['high'].unstack()
            stock_minute_low = stock_minute_data['low'].unstack()
            stock_minute_volume = stock_minute_data['volume'].unstack()
            stock_minute_open_5 = pd.DataFrame([], index=list(stock_minute_open.index))
            stock_minute_close_5 = pd.DataFrame([], index=list(stock_minute_close.index))
            stock_minute_high_5 = pd.DataFrame([], index=list(stock_minute_high.index))
            stock_minute_low_5 = pd.DataFrame([], index=list(stock_minute_low.index))
            stock_minute_volume_5 = pd.DataFrame([], index=list(stock_minute_volume.index))
            for i, minute in enumerate(self.__complete_minute_list):
                if minute % 5 == 0 and 930 <= minute < 1500:
                    stock_minute_open_5[minute] = stock_minute_open.iloc[:, i]
                    stock_minute_close_5[minute] = stock_minute_close.iloc[:, i+4]
                    stock_minute_high_5[minute] = stock_minute_high.iloc[:, i:i+5].max(axis=1)
                    stock_minute_low_5[minute] = stock_minute_low.iloc[:, i:i+5].min(axis=1)
                    stock_minute_volume_5[minute] = stock_minute_volume.iloc[:, i:i+5].sum(axis=1)
            consistent_bar = (stock_minute_open_5 - stock_minute_close_5) >= 0.95 * (stock_minute_high_5 - stock_minute_low_5)
            total_consistent_volume = stock_minute_volume_5[consistent_bar].sum(axis=1) / stock_minute_volume.sum(axis=1)
            factor_data = total_consistent_volume.rolling(self.__n, min_periods=1).mean()
            factor_data = pd.DataFrame(factor_data, index=Dtk.get_trading_day(start, end), columns=[code])
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
        save_dir = "S:\\Apollo\\Factors\\"    # 保存于云桌面的地址
    else:
        user_id = os.environ['USER_ID']
        save_dir = "/app/data/" + user_id + "/AlphaFactors"  # 保存于XQuant的地址
    if not os.path.exists(save_dir):
        os.mkdir(save_dir)
    ###############################################
    # 以下3行及factor_generator的类名需要自行改写 #
    ###############################################
    istart_date_int = 20141201
    iend_date_int = 20180630
    for i in [10]:
        factor_name = "F_D_MinNegativeConsistentVolume_" + str(i)   # 这个因子名可以加各种后缀，用于和相近的因子做区分
        file_name = factor_name
        factor_generator = FactorDailyMinNegativeConsistentVolume(codes=stock_code_list, start_date_int=istart_date_int,
                                                                  end_date_int=iend_date_int, name=file_name,
                                                                  save_path=save_dir, n=i)
        factor_generator.launch()
    logging.info("program is stopped")


if __name__ == '__main__':
    main()
