# -*- coding: utf-8 -*-
# @Time    : 2018/10/15 20:53
# @Author  : 011673
# @File    : MinDataUpdater.py.py
# revised by 006566, 2019/2/20
# latest updated on 2019/2/21 新增：指数模式
# latest updated on 2019/2/26 修复了max_day_span交界日重复获取数据的bug
import datetime as dt
from xquant.marketdata import MarketData
import pandas as pd
import os


max_day_span = 28

def minute_data_updater(code, start, end, mode='stock'):
    if mode == 'stock':
        dir_path = '/app/data/006566/UnadjustedStockMinData/MINUTE/stock/'
        file_name = "UnAdjstedStockMinute_" + code[0:6] + ".pkl"
    elif mode == 'index':
        dir_path = '/app/data/006566/UnadjustedStockMinData/MINUTE/index/'
        file_name = "indexMinute_" + code[0:6] + ".pkl"
    else:
        raise TypeError
    if os.path.exists(os.path.join(dir_path, file_name)):
        original_minute_data = pd.read_pickle(os.path.join(dir_path, file_name), compression='gzip')
        update_minute_data = __read_xquant_data(code, start, end)
        update_minute_data2 = update_minute_data.dropna()
        if update_minute_data2.__len__() > 0:  # 若更新期间股票无交易信息，则不会更新
            def get_new_index(df: pd.DataFrame):
                df = df.reset_index()
                df['unq_index'] = df['dt'] * 10000 + df['minute']
                df = df.set_index(['dt', 'Ticker'])
                return df

            original_minute_data = get_new_index(original_minute_data)
            original_minute_data = original_minute_data[(original_minute_data['unq_index'] < (start * 10000)) | (
            original_minute_data['unq_index'] > (end * 10000 + 2359))]
            update_minute_data = get_new_index(update_minute_data)
            result = original_minute_data.append(update_minute_data)
            result = result.sort_values(by='unq_index')
            result.pop('unq_index')
            pd.to_pickle(result, os.path.join(dir_path, file_name), compression='gzip')
            return result
    else:
        minute_data = __read_xquant_data(code, start, end)
        pd.to_pickle(minute_data, os.path.join(dir_path, file_name), compression='gzip')
        return


def __read_xquant_data(code, start_date, end_date):
    start_date_dt = dt.datetime(int(str(start_date)[0:4]), int(str(start_date)[4:6]), int(str(start_date)[6:8]), 0, 0,
                                0)
    end_date_dt = dt.datetime(int(str(end_date)[0:4]), int(str(end_date)[4:6]), int(str(end_date)[6:8]), 0, 0,
                              0) + dt.timedelta(days=1)
    result = None
    while start_date_dt < end_date_dt:
        start = int(start_date_dt.strftime('%Y%m%d'))
        end = min(end_date_dt, (start_date_dt + dt.timedelta(days=max_day_span)))
        end = int(end.strftime('%Y%m%d'))
        if result is None:
            result = __read_xqaunt_data_month(code, start, end)
        else:
            temp = __read_xqaunt_data_month(code, start, end)
            if temp is not None:
                result = result.append(temp)
        start_date_dt = min((start_date_dt + dt.timedelta(days=(max_day_span+1))), end_date_dt)
    return result


def __read_xqaunt_data_month(code, start_date, end_date):
    ma = MarketData()
    start_date = str(start_date * 1000000)
    end_date = str(end_date * 1000000 + 235959)
    xqt_data = ma.getKLine4ZTDataFrame(code, start_date, end_date, 10, 20, True)
    xqt_data.rename(columns={'MDDate': 'dt',
                             'MDTime': 'minute',
                             'OpenPx': 'open',
                             'ClosePx': 'close',
                             'HighPx': 'high',
                             'LowPx': 'low',
                             'TotalVolumeTrade': 'volume',
                             'TotalValueTrade': 'amt'}, inplace=True)

    def code_name_to_number(code_name):
        return int(code_name[0:-3])

    xqt_data['Ticker'] = code_name_to_number(code)
    xqt_data['Ticker'] = xqt_data['Ticker'].astype(int)
    xqt_data['dt'] = xqt_data['dt'].astype(int)
    xqt_data['minute'] = (xqt_data['minute'].astype(int) / 100000).astype(int)
    xqt_data['open'] = xqt_data['open'].astype(float)
    xqt_data['high'] = xqt_data['high'].astype(float)
    xqt_data['low'] = xqt_data['low'].astype(float)
    xqt_data['close'] = xqt_data['close'].astype(float)
    xqt_data['volume'] = xqt_data['volume'].astype(float)
    xqt_data['amt'] = xqt_data['amt'].astype(float)
    xqt_data = xqt_data[['dt', 'Ticker', 'minute', 'open', 'high', 'low', 'close', 'volume', 'amt']]
    xqt_data.set_index(['dt', 'Ticker'], inplace=True)
    return xqt_data
