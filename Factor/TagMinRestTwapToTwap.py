# -*- coding: utf-8 -*-
"""
Created on 2018/8/27 16:13

@author: 006547
"""
from Factor.MinFactorBase import MinFactorBase
import pandas as pd
import numpy as np
import logging
from typing import List
import platform
import DataAPI.DataToolkit as Dtk


class TagMinRestTwapToTwap(MinFactorBase):
    def __init__(self, codes: List[str] = ..., start_date_int: int = ..., end_date_int: int = ...,
                 save_path: str = ..., name: str = ..., lag: int = ...):
        super().__init__(codes, start_date_int, end_date_int, save_path, name)
        self.__lag = lag

    def single_stock_factor_generator(self, code: str = ..., start: int = ..., end: int = ...):
        ############################################
        # 以下是因子计算逻辑的部分，需要用户自定义 #
        # 计算因子时，最后应得到factor_data这个对象，类型应当是DataFrame，涵盖的时间段是start至end（前闭后闭）；
        # factor_data的因子值一列，应当以股票代码为列名；
        # 最后factor_data的索引，建议与从get_single_stock_minute_data获得的原始行情的索引（index）一致，
        # 如通过reset_index撤销了原始行情的索引，那么不要删除'dt'或'minute'这两列，也不要设别的索引。
        ############################################
        end_date_plus_k = Dtk.get_n_days_off(end, self.__lag + 1)[-1]
        minute_data = Dtk.get_single_stock_minute_data(code, start, end_date_plus_k, fill_nan=True,
                                                       append_pre_close=False, adj_type='FORWARD',
                                                       drop_nan=False, full_length_padding=True)
        # 将非交易日的行情（分钟数据接口有个机制，如果全天无交易，那么行情全为na）drop掉，用于后续检查是否行情全为nan
        minute_data2 = minute_data.loc[start: end].dropna()
        if minute_data2.columns.__len__() > 0 and minute_data2.__len__() > 0:  # 如可正常取到行情DataFrame
            minute_close = minute_data['close'].unstack()

            minute_total_close = pd.DataFrame(np.tile(minute_close.sum(1).values, (242, 1)).transpose(),
                                              columns=minute_close.columns, index=minute_close.index)
            minute_acc_close = minute_close.cumsum(1)

            minute_one = pd.DataFrame(np.ones([minute_close.shape[0], minute_close.shape[1]]),
                                      columns=minute_close.columns, index=minute_close.index)
            minute_total_one = pd.DataFrame(np.tile(minute_one.sum(1).values, (242, 1)).transpose(),
                                            columns=minute_close.columns, index=minute_close.index)
            minute_acc_one = minute_one.cumsum(1)

            minute_TWAP_remain = (minute_total_close - minute_acc_close) / (minute_total_one - minute_acc_one)

            twap_remain_925 = minute_TWAP_remain.values[:, 0]  # 925的TWAP_remain其实就是全天TWAP
            minute_TWAP_allday = pd.DataFrame(np.tile(twap_remain_925, (242, 1)).transpose(),
                                              columns=minute_close.columns, index=minute_close.index)

            stock_minute_data = minute_TWAP_allday.shift(-self.__lag) / minute_TWAP_remain - 1
            stock_minute_data = stock_minute_data.stack()

            # 使不同股票的行数一致
            stock_minute_data = stock_minute_data.reindex(index=minute_data.index)

            # 将start_date至end_date期间的因子值提取出来
            factor_data = stock_minute_data.loc[start: end]
            # 将factor_data改为DataFrame, 且列名设为code
            factor_data = factor_data.to_frame()
            factor_data.columns = [code]

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
    istart_date_int = 20141201
    iend_date_int = 20180630
    for i_lag in [1, 2, 3, 4, 5]:
        k = i_lag
        factor_name = "T_D_RestTwapToTwap_" + str(k)
        file_name = factor_name
        factor_generator = TagMinRestTwapToTwap(codes=stock_code_list, start_date_int=istart_date_int,
                                                end_date_int=iend_date_int, name=file_name, save_path=save_dir,
                                                lag=i_lag)
        factor_generator.launch()
    logging.info("program is stopped")


if __name__ == '__main__':
    main()
