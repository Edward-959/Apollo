# -*- coding: utf-8 -*-
"""
@author: 006688
不同期间累计成交量方差比
用5 分钟成交量均线的方差与10 分钟成交量均线的方差构造了变异数比率，
用于反映日内成交量的趋势性
"""


from Factor.MinFactorBase import MinFactorBase
import pandas as pd
import numpy as np
import logging
from typing import List
import platform
import DataAPI.DataToolkit as Dtk


class DailyMinVarianceRatio(MinFactorBase):
    def __init__(self, codes: List[str] = ..., start_date_int: int = ..., end_date_int: int = ..., save_path: str = ...,
                 name: str = ..., n: int = ...):
        super().__init__(codes, start_date_int, end_date_int, save_path, name)
        self.__n = n
        self.__start_date_minus_n_2 = Dtk.get_n_days_off(start_date_int, -(self.__n + 2))[0]
        self.__data_volume: pd.DataFrame = Dtk.get_panel_daily_pv_df(codes, self.__start_date_minus_n_2, end_date_int,
                                                                     pv_type='volume')

    def single_stock_factor_generator(self, code: str = ..., start: int = ..., end: int = ...):
        ############################################
        # 以下是数据计算逻辑的部分，需要用户自定义 #
        # 计算数据时，最后应得到factor_data这个对象，类型应当是DataFrame，涵盖的时间段是start至end（前闭后闭）；
        # factor_data的因子值一列，应当以股票代码为列名；
        # 最后factor_data的索引，应当从原始分钟数据中获得的dt，即start至end期间的交易日，内容的格式是20180904这种8位数字
        ############################################
        stock_minute_data = Dtk.get_single_stock_minute_data(code, self.__start_date_minus_n_2, end, fill_nan=True,
                                                             append_pre_close=False, adj_type='NONE', drop_nan=False,
                                                             full_length_padding=True)
        if stock_minute_data.columns.__len__() > 0 and stock_minute_data.__len__() > 0:
            stock_minute_volume = stock_minute_data['volume'].unstack()
            stock_volume_sum_5 = stock_minute_volume.rolling(5, axis=1).sum()
            stock_volume_sum_10 = stock_minute_volume.rolling(10, axis=1).sum()
            stock_variance_ratio = (stock_volume_sum_5.var(axis=1) / 5) / (stock_volume_sum_10.var(axis=1) / 10)
            factor_data = stock_variance_ratio.rolling(self.__n, min_periods=1).mean()
            factor_data = factor_data * self.__data_volume.loc[:, code] / self.__data_volume.loc[:, code]
            factor_data = factor_data.to_frame(code)
            factor_data = factor_data.loc[start: end].copy()
        ########################################
        # 数据计算逻辑到此为止，以下勿随意变更 #
        ########################################
        else:  # 如取到的行情的DataFrame为空，则自造一个全为nan的DataFrame
            date_list = Dtk.get_trading_day(start, end)
            factor_data = pd.DataFrame(index=date_list)  # 新建一个空的DataFrame, 且先设好了索引
            temp_array = np.empty(shape=[date_list.__len__(), ])
            temp_array[:] = np.nan
            factor_data[code] = temp_array
        # 因子应当以timestamp作为索引
        date_list = Dtk.convert_date_or_time_int_to_datetime(list(factor_data.index))
        timestamp_list = [i_date.timestamp() for i_date in date_list]
        factor_data.index = timestamp_list
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
    for n in [10]:
        factor_name = "F_D_MinVarianceRatio_" + str(n)
        file_name = factor_name
        factor_generator = DailyMinVarianceRatio(codes=stock_code_list, start_date_int=istart_date_int,
                                                 end_date_int=iend_date_int, name=file_name, save_path=save_dir, n=n)
        factor_generator.launch()
    logging.info("program is stopped")


if __name__ == '__main__':
    main()
