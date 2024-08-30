# -*- coding: utf-8 -*-
"""
Created on 2018/8/27
@author: 006566

这是一个范例，用于计算每分钟收盘价到当天VWAP的距离
这个因子只用到当天日内的行情，不会用到跨日数据

本代码用于：
1）生成新的因子文件（一般只用一次）
2）将因子文件根据行情更新到最新日期（添加行），如有新股上市、则也需更新因子文件（添加列）
    （会被反复调用） —— 还未写好
"""


from Factor.MinFactorBase import MinFactorBase
import pandas as pd
import numpy as np
import logging
from typing import List
import platform
import DataAPI.DataToolkit as Dtk


class FactorMinIntradayDistanceToVWAP(MinFactorBase):
    def __init__(self, codes: List[str] = ..., start_date_int: int = ..., end_date_int: int = ...,
                 save_path: str = ..., name: str = ...):
        super().__init__(codes, start_date_int, end_date_int, save_path, name)

    def single_stock_factor_generator(self, code: str = ..., start: int = ..., end: int = ...):
        ############################################
        # 以下是因子计算逻辑的部分，需要用户自定义 #
        # 计算因子时，最后应得到factor_data这个对象，类型应当是DataFrame，涵盖的时间段是start至end（前闭后闭）；
        # factor_data的因子值一列，应当以股票代码为列名；
        # 最后factor_data的索引，建议与从get_single_stock_minute_data获得的原始行情的索引（index）一致，
        # 如通过reset_index撤销了原始行情的索引，那么不要删除'dt'或'minute'这两列，也不要设别的索引。
        ############################################
        stock_minute_data = Dtk.get_single_stock_minute_data(code, start, end, fill_nan=True,
                                                             append_pre_close=False, adj_type='NONE',
                                                             drop_nan=False, full_length_padding=True)
        if stock_minute_data.columns.__len__() > 0:  # 如可正常取到行情DataFrame
            # 将index变为普通列，主要是要得到'dt'（日期），用于后续计算日内的vwap
            stock_minute_data = stock_minute_data.reset_index()
            # 计算每日日内的累计成交额和累计成交量
            stock_minute_data['intraday_volume_cumsum'] = stock_minute_data.groupby('dt')['volume'].cumsum()
            stock_minute_data['intraday_amt_cumsum'] = stock_minute_data.groupby('dt')['amt'].cumsum()
            # 计算日内vwap（黄线）
            stock_minute_data['intraday_vwap'] = stock_minute_data['intraday_amt_cumsum'] / stock_minute_data[
                'intraday_volume_cumsum']
            # 计算到黄线的距离，因为最后保存到因子文件时需以股票代码作为列名，所以这里提前设置了
            stock_minute_data[code] = stock_minute_data['close'] / stock_minute_data[
                'intraday_vwap'] - 1
            factor_data = stock_minute_data
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
    istart_date_int = 20150701
    iend_date_int = 20180630
    factor_name = "F_M_IntradayDistanceToVWAP"  # 这个因子名可以加各种后缀，用于和相近的因子做区分
    file_name = factor_name
    factor_generator = FactorMinIntradayDistanceToVWAP(codes=stock_code_list, start_date_int=istart_date_int,
                                                       end_date_int=iend_date_int, name=file_name, save_path=save_dir)
    factor_generator.launch()
    logging.info("program is stopped")


if __name__ == '__main__':
    main()
