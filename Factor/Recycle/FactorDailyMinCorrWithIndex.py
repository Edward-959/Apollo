# -*- coding: utf-8 -*-
"""
@author: 006688
计算每日个股与指数的历史相关系数，计算方式为：
取过去N日个股与指数的分钟K线，计算其收益率的相关性
"""


from Factor.MinFactorBase import MinFactorBase
import pandas as pd
import numpy as np
import logging
from typing import List
import platform
import DataAPI.DataToolkit as Dtk


class CorrWithIndex(MinFactorBase):
    def __init__(self, codes: List[str] = ..., start_date_int: int = ..., end_date_int: int = ..., save_path: str = ...,
                 name: str = ..., lag: int = ..., index_code: str = ...):
        super().__init__(codes, start_date_int, end_date_int, save_path, name)
        self.__lag = lag
        self.__index_code = index_code
        start_date_minus_k_2 = Dtk.get_n_days_off(start_date_int, -(self.__lag + 2))[0]
        index_minute_data = Dtk.get_single_stock_minute_data(index_code, start_date_minus_k_2, end_date_int,
                                                             fill_nan=True, append_pre_close=False, adj_type='NONE',
                                                             drop_nan=False, full_length_padding=True)
        self.__index_minute_ret = index_minute_data['close'].diff() / index_minute_data['close'].shift(1)
        self.__trading_days = Dtk.get_trading_day(start_date_int, end_date_int)
        self.__refer_days = self.__index_minute_ret.index.unique(0).tolist()

    def single_stock_factor_generator(self, code: str = ..., start: int = ..., end: int = ...):
        ############################################
        # 以下是数据计算逻辑的部分，需要用户自定义 #
        # 计算数据时，最后应得到factor_data这个对象，类型应当是DataFrame，涵盖的时间段是start至end（前闭后闭）；
        # factor_data的因子值一列，应当以股票代码为列名；
        # 最后factor_data的索引，应当从原始分钟数据中获得的dt，即start至end期间的交易日，内容的格式是20180904这种8位数字
        ############################################
        start_date_minus_k_2 = Dtk.get_n_days_off(start, -(self.__lag + 2))[0]
        stock_minute_data = Dtk.get_single_stock_minute_data(code, start_date_minus_k_2, end, fill_nan=True,
                                                             append_pre_close=False, adj_type='FORWARD', drop_nan=False,
                                                             full_length_padding=True)
        stock_minute_ret = stock_minute_data['close'].diff() / stock_minute_data['close'].shift(1)
        if stock_minute_data.columns.__len__() > 0 and stock_minute_data.__len__() > 0:  # 如可正常取到行情DataFrame
            corr_list = []
            # 逐日取前lag日的个股与指数的分钟收益率，计算相关系数
            for date in self.__trading_days:
                i = self.__refer_days.index(date)
                index_minute_ret_i = self.__index_minute_ret.loc[self.__refer_days[i - self.__lag:i]]
                stock_minute_ret_i = stock_minute_ret.loc[self.__refer_days[i - self.__lag:i]]
                corr_list.append(np.corrcoef(stock_minute_ret_i, index_minute_ret_i)[0][1])
            factor_data = pd.DataFrame(corr_list, index=self.__trading_days, columns=[code])
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
    istart_date_int = 20180101
    iend_date_int = 20180630
    lag = 5
    index_code = '000300.SH'
    factor_name = "F_D_MinCorrWithIndex_" + str(lag) + "_" + index_code[0:6]
    file_name = factor_name
    factor_generator = CorrWithIndex(codes=stock_code_list, start_date_int=istart_date_int,
                                     end_date_int=iend_date_int, name=file_name, save_path=save_dir,
                                     lag=lag, index_code=index_code)
    factor_generator.launch()
    logging.info("program is stopped")


if __name__ == '__main__':
    main()
