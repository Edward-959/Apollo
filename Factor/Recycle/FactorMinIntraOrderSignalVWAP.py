# -*- coding: utf-8 -*-
"""
@author: 006688
根据打败VWAP的逻辑，输出日内开平仓信号
FactorMinIntraOrderSignalVWAPBuy输出值为1的是买入信号
FactorMinIntraOrderSignalVWAPSell输出值为1的是卖出信号
"""

from Factor.MinFactorBase import MinFactorBase
import pandas as pd
import numpy as np
import logging
from typing import List
import platform
import DataAPI.DataToolkit as Dtk
from copy import deepcopy


class FactorMinIntraOrderSignalVWAPBuy(MinFactorBase):
    def __init__(self, codes: List[str] = ..., start_date_int: int = ..., end_date_int: int = ...,
                 save_path: str = ..., name: str = ..., lag: int = ...):
        super().__init__(codes, start_date_int, end_date_int, save_path, name)
        self.__lag = lag

    def single_stock_factor_generator(self, code: str = ..., start: int = ..., end: int = ...):
        ############################################
        # 以下是因子计算逻辑的部分，需要用户自定义 #
        ############################################
        open_para_start = 0.002  # 开仓阈值起始值
        open_para_end = -0.004  # 开仓阈值终值
        open_para = np.array([0.0] * 242)
        open_para[30: 237] = np.linspace(open_para_start, open_para_end, 207)
        start_date_minus_n_2 = Dtk.get_n_days_off(start, -(self.__lag + 2))[0]
        minute_data_raw = Dtk.get_single_stock_minute_data(code, start_date_minus_n_2, end, fill_nan=True,
                                                           append_pre_close=False, adj_type='None', drop_nan=False,
                                                           full_length_padding=True)
        minute_data = deepcopy(minute_data_raw.dropna())
        if divmod(minute_data.__len__(), 242)[1] > 0:  # 如果minute_data的行数不是242的整数倍，说明可能漏了部分行（某些分钟）
            minute_data = minute_data.reset_index()
            date_list = list(np.unique(minute_data['dt']))
            complete_minute_list = Dtk.get_complete_minute_list()
            index_tuple = [np.repeat(date_list, len(complete_minute_list)), complete_minute_list * len(date_list)]
            # 构建一个逐日、逐分钟的双重索引
            mi_index = pd.MultiIndex.from_tuples(list(zip(*index_tuple)), names=['dt', 'minute'])
            # 对数据重建索引，如不在索引中的，默认就是NaN
            minute_data = minute_data.reindex(index=mi_index)
            for column in minute_data.columns:
                if column in ['amt', 'volume']:
                    minute_data[column] = minute_data[column].fillna(0)
                else:
                    minute_data[column] = minute_data[column].fillna(method='ffill')
        if minute_data.columns.__len__() > 0 and minute_data.__len__() > 0:  # 如可正常取到行情DataFrame
            minute_close = minute_data['close'].unstack()
            minute_volume = minute_data['volume'].unstack()
            minute_amt = minute_data['amt'].unstack()
            minute_acc_volume = minute_volume.cumsum(1)
            minute_acc_volume_percent = minute_acc_volume.div(minute_acc_volume[1500], axis=0)  # 累计成交量当日占比
            minute_acc_volume_percent_est = minute_acc_volume_percent.rolling(self.__lag, min_periods=1).mean().shift(1)  # 估计当日成交量占比
            minute_acc_amt = minute_amt.cumsum(1)
            minute_VWAP = minute_acc_amt / minute_acc_volume  # 当前VWAP
            minute_close_ma = minute_close.rolling(30, axis=1).mean()
            # 计算上穿布林带下轨
            minute_close_std = minute_close.rolling(30, axis=1).std()
            # minute_close_boll_down = minute_close_ma - 2 * minute_close_std
            # minute_close_boll_signal = minute_close - minute_close_boll_down
            # minute_close_boll_signal_sign_buy = np.sign(minute_close_boll_signal)
            # minute_close_boll_signal_sign_buy = minute_close_boll_signal_sign_buy.rolling(2, axis=1).sum() + minute_close_boll_signal_sign_buy
            # 剩余时间
            min_remaining = 242 - np.arange(0, 242)
            min_remaining = np.tile(min_remaining, (minute_close.shape[0], 1))
            min_remaining = pd.DataFrame(min_remaining, columns=minute_close.columns, index=minute_close.index)
            # 以过去30分钟价格波动率进行估计
            minute_price_down = minute_close_ma - np.sqrt(min_remaining / 30) * minute_close_std
            # 估计VWAP区间
            minute_VWAP_down = minute_VWAP * minute_acc_volume_percent_est + minute_price_down * (1 - minute_acc_volume_percent_est)

            minute_signal_buy = minute_close / minute_VWAP_down - 1
            signal_buy = minute_signal_buy.apply(lambda x: x < -open_para, axis=1).astype('int')
            # minute_signal_buy = minute_signal_buy.apply(lambda x: x < -open_para, axis=1).stack()
            # minute_close_boll_signal_sign_buy = minute_close_boll_signal_sign_buy.apply(lambda x: x == 1, axis=1).stack()
            # signal_buy = pd.DataFrame([minute_close_boll_signal_sign_buy, minute_signal_buy]).all(0).astype('int')
            # signal_buy = signal_buy.unstack()
            signal_buy.iloc[:, 0:30] = 0
            signal_buy.iloc[:, 237:] = 0
            minute_data[code] = signal_buy.stack()
            minute_data = minute_data.reindex(index=minute_data_raw.index)
            factor_data = minute_data.loc[start: end][code]
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


class FactorMinIntraOrderSignalVWAPSell(MinFactorBase):
    def __init__(self, codes: List[str] = ..., start_date_int: int = ..., end_date_int: int = ...,
                 save_path: str = ..., name: str = ..., lag: int = ...):
        super().__init__(codes, start_date_int, end_date_int, save_path, name)
        self.__lag = lag

    def single_stock_factor_generator(self, code: str = ..., start: int = ..., end: int = ...):
        ############################################
        # 以下是因子计算逻辑的部分，需要用户自定义 #
        ############################################
        open_para_start = 0.002  # 开仓阈值起始值
        open_para_end = -0.004  # 开仓阈值终值
        open_para = np.array([0.0] * 242)
        open_para[30: 237] = np.linspace(open_para_start, open_para_end, 207)
        start_date_minus_n_2 = Dtk.get_n_days_off(start, -(self.__lag + 2))[0]
        minute_data_raw = Dtk.get_single_stock_minute_data(code, start_date_minus_n_2, end, fill_nan=True,
                                                           append_pre_close=False, adj_type='None', drop_nan=False,
                                                           full_length_padding=True)
        minute_data = deepcopy(minute_data_raw.dropna())
        if divmod(minute_data.__len__(), 242)[1] > 0:  # 如果minute_data的行数不是242的整数倍，说明可能漏了部分行（某些分钟）
            minute_data = minute_data.reset_index()
            date_list = list(np.unique(minute_data['dt']))
            complete_minute_list = Dtk.get_complete_minute_list()
            index_tuple = [np.repeat(date_list, len(complete_minute_list)), complete_minute_list * len(date_list)]
            # 构建一个逐日、逐分钟的双重索引
            mi_index = pd.MultiIndex.from_tuples(list(zip(*index_tuple)), names=['dt', 'minute'])
            # 对数据重建索引，如不在索引中的，默认就是NaN
            minute_data = minute_data.reindex(index=mi_index)
            for column in minute_data.columns:
                if column in ['amt', 'volume']:
                    minute_data[column] = minute_data[column].fillna(0)
                else:
                    minute_data[column] = minute_data[column].fillna(method='ffill')
        if minute_data.columns.__len__() > 0 and minute_data.__len__() > 0:  # 如可正常取到行情DataFrame
            minute_close = minute_data['close'].unstack()
            minute_volume = minute_data['volume'].unstack()
            minute_amt = minute_data['amt'].unstack()
            minute_acc_volume = minute_volume.cumsum(1)
            minute_acc_volume_percent = minute_acc_volume.div(minute_acc_volume[1500], axis=0)  # 累计成交量当日占比
            minute_acc_volume_percent_est = minute_acc_volume_percent.rolling(self.__lag, min_periods=1).mean().shift(1)  # 估计当日成交量占比
            minute_acc_amt = minute_amt.cumsum(1)
            minute_VWAP = minute_acc_amt / minute_acc_volume  # 当前VWAP
            minute_close_ma = minute_close.rolling(30, axis=1).mean()
            # 计算下穿布林带上轨
            minute_close_std = minute_close.rolling(30, axis=1).std()
            # minute_close_boll_up = minute_close_ma + 2 * minute_close_std
            # minute_close_boll_signal = minute_close_boll_up - minute_close
            # minute_close_boll_signal_sign_sell = np.sign(minute_close_boll_signal)
            # minute_close_boll_signal_sign_sell = minute_close_boll_signal_sign_sell.rolling(2, axis=1).sum() + minute_close_boll_signal_sign_sell
            # # 剩余时间
            min_remaining = 242 - np.arange(0, 242)
            min_remaining = np.tile(min_remaining, (minute_close.shape[0], 1))
            min_remaining = pd.DataFrame(min_remaining, columns=minute_close.columns, index=minute_close.index)
            # 以过去30分钟价格波动率进行估计
            minute_price_up = minute_close_ma + np.sqrt(min_remaining / 30) * minute_close_std
            # 估计VWAP区间
            minute_VWAP_up = minute_VWAP * minute_acc_volume_percent_est + minute_price_up * (1 - minute_acc_volume_percent_est)

            minute_signal_sell = minute_close / minute_VWAP_up - 1
            signal_sell = minute_signal_sell.apply(lambda x: x > open_para, axis=1).astype('int')
            # minute_signal_sell = minute_signal_sell.apply(lambda x: x > open_para, axis=1).stack()
            # minute_close_boll_signal_sign_sell = minute_close_boll_signal_sign_sell.apply(lambda x: x == 1, axis=1).stack()
            # signal_sell = pd.DataFrame([minute_close_boll_signal_sign_sell, minute_signal_sell]).all(0).astype('int')
            # signal_sell = signal_sell.unstack()
            signal_sell.iloc[:, 0:30] = 0
            signal_sell.iloc[:, 237:] = 0
            minute_data[code] = signal_sell.stack()
            minute_data = minute_data.reindex(index=minute_data_raw.index)
            factor_data = minute_data.loc[start: end][code]
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
    istart_date_int = 20170701
    iend_date_int = 20171231
    for lag in [5]:
        # 计算买入信号
        factor_name = "F_M_IntraOrderSignalVWAPBuy0.02to-0.04"  # 这个因子名可以加各种后缀，用于和相近的因子做区分
        file_name = factor_name
        factor_generator = FactorMinIntraOrderSignalVWAPBuy(codes=stock_code_list, start_date_int=istart_date_int,
                                                            end_date_int=iend_date_int, name=file_name,
                                                            save_path=save_dir, lag=lag)
        factor_generator.launch()
        # 计算卖出信号
        factor_name = "F_M_IntraOrderSignalVWAPSell0.02to-0.04"  # 这个因子名可以加各种后缀，用于和相近的因子做区分
        file_name = factor_name
        factor_generator = FactorMinIntraOrderSignalVWAPSell(codes=stock_code_list, start_date_int=istart_date_int,
                                                             end_date_int=iend_date_int, name=file_name,
                                                             save_path=save_dir, lag=lag)
        factor_generator.launch()
    logging.info("program is stopped")


if __name__ == '__main__':
    main()
