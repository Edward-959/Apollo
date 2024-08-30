# -*- coding: utf-8 -*-
import pandas as pd
import datetime as dt
import scipy.io as sio
from os import path
from .utils.timetool import get_month_list
import pickle


def read_from_local(src_dir: str, stock_code: str, start_date_time: dt.datetime,
                    end_date_time: dt.datetime) ->pd.DataFrame:
    """
    getData这个函数是根据输入的股票代码、开始日期时间、
    终止日期时间及交易所日历中的交易日(exchangeTradingDayList，输出所选的股票数据
    输出的数据的格式是list, 其长度等于开始日期和终止日期之间的交易所日历交易日天数
    输出的list的内容是dict, 若某天股票停牌，则当天为空

    注意，该函数的startDateTime和endDateTime都是Strategy定义的时间，而exchangeTradingDayList是切分后的日期list，
    例如，startDateTime=2017/10/10/09/30/00，endDateTime=2017/10/16/15/00/00，
    exchangeTradingDayList=[2017/10/12, 2017/10/13]
    """
    year_month_list = get_month_list(start_date_time, end_date_time)
    data_frame = None
    for month_id in year_month_list:
        # data_frame.append(read_single_month_file(srcDir, stockCode, month_id))
        if data_frame is None:
            data_frame = read_single_month_file(src_dir, stock_code, month_id)
        else:
            data_frame = data_frame.append(read_single_month_file(src_dir, stock_code, month_id), ignore_index=True)

    final_data = data_filter(stock_code, data_frame, start_date_time, end_date_time)
    return final_data


def data_filter(stock_code: str, stock_data: pd.DataFrame, start_date_time: dt.datetime,
                end_date_time: dt.datetime, time_mode: int=1):
    start_date_time_stamp = start_date_time.timestamp()
    end_date_time_stamp = end_date_time.timestamp()
    start_time8digit = start_date_time.hour * 10000000
    start_time8digit += start_date_time.minute * 100000 + start_date_time.second * 10000
    end_time8digit = end_date_time.hour * 10000000 + end_date_time.minute * 100000 + end_date_time.second * 10000
    valid_time1 = (stock_data['TimeStamp'] >= start_date_time_stamp) & (stock_data['TimeStamp'] <= end_date_time_stamp)
    stock_data = stock_data.loc[valid_time1]  # 仅保留startDateTime和endDateTime之间的数据
    if time_mode == 1:
        if stock_code[-1] == 'H':  # 上交所的股票或指数
            time_filter = ((stock_data['Time'] >= start_time8digit) & (stock_data['Time'] < 113000000)) | (
                    (stock_data['Time'] >= 130000000) &
                    (stock_data['Time'] < 145957000) & (stock_data['Time'] < end_time8digit))
        else:
            time_filter = ((stock_data['Time'] >= start_time8digit) & (stock_data['Time'] < 113000000)) | (
                    (stock_data['Time'] >= 130000000) &
                    (stock_data['Time'] < 145657000) & (stock_data['Time'] < end_time8digit))
    else:
        if stock_code[-1] == 'H':  # 上交所的股票或指数
            time_filter = ((stock_data['Time'] >= 93000000) & (stock_data['Time'] < 113000000)) | (
                    (stock_data['Time'] >= 130000000) & (stock_data['Time'] < 145957000))
        else:  # 深交所的股票或指数
            time_filter = ((stock_data['Time'] >= 93000000) & (stock_data['Time'] < 113000000)) | (
                    (stock_data['Time'] >= 130000000) & (stock_data['Time'] < 145657000))
    stock_data = stock_data[time_filter]  # 仅保留在连续竞价期间的数据
    return stock_data


def calc_diff(val):
    if len(val) > 1:
        return val[-1] - val[-2]
    else:
        return 0


def clean_dataframe(data_frame: pd.DataFrame) -> pd.DataFrame:
    for day in data_frame['Date'].unique():
        df = data_frame[data_frame['Date'] == day]
        if len(df[(df['BidV1'] == 0) | (df['AskV1'] == 0)]) > 0:
            print("%s tick file hit limit high/low, drop it" % str(day))
            data_frame = data_frame[data_frame.Date != day]
        elif df.duplicated("TimeStamp").sum() > 0:
            print("%s tick file has duplicated entries, drop it" % str(day))
            data_frame = data_frame[data_frame.Date != day]
        else:
            is_bad = False
            for index, row in df.iterrows():
                for level in range(1, 10):
                    if row['AskP' + str(level)] >= row['AskP' + str(level+1)]:
                        print("%s tick file %d has unordered ask prices, drop it" % (str(day), row['Time']))
                        data_frame = data_frame[data_frame.Date != day]
                        is_bad = True
                        break
                    if row['BidP' + str(level)] <= row['BidP' + str(level+1)]:
                        print("%s tick file %d has unordered bid prices, drop it" % (str(day), row['Time']))
                        data_frame = data_frame[data_frame.Date != day]
                        is_bad = True
                        break
                if is_bad:
                    break
    # clean volume is 0 rows
    result = data_frame[data_frame['AccVolume'] > 0]
    if len(result) == 0:
        return None
    else:
        return result


def read_single_day_pickle_file(root_path: str, stock_code: str, day: str) -> pd.DataFrame:
    tick_fn = path.join(root_path, stock_code[:6] + "_" + day + ".pickle")
    if not path.exists(tick_fn):
        print("{} {} stock file {} not exist".format(stock_code, day, tick_fn))
        return None
    data = pickle.load(open(tick_fn, "rb"))[0]
    result = pd.DataFrame(data)
    return result


def read_single_day_file(root_path: str, stock_code:str, day:str, is_br_data=False) -> pd.DataFrame:
    tick_fn = path.join(root_path, stock_code[:6] + "_" + day + ".csv")
    if is_br_data:
        if stock_code[6:] == ".SH":
            tick_fn = path.join(root_path, "SSE", "SHA", day, "tick", stock_code[:6] + ".csv")
        else:
            tick_fn = path.join(root_path, "SZE", "SZA", day, "tick", stock_code[:6] + ".csv")

    if not path.exists(tick_fn):
        print("{} {} stock file {} not exist".format(stock_code, day, tick_fn))
        return None

    result = pd.read_csv(tick_fn, skipinitialspace=True)

    if len(result) < 500:
        print("{} {} stock file {} too short".format(stock_code, day, tick_fn))
        return None

    if "S1" in result.columns:
        rename_columns = {"ContractCode": "Code", "Date": "Date", "TotalTurnover": "AccTurover",
                          "TotalVolume": "AccVolume", "HighPrice": "High", "LowPrice": "Low",
                          "OpenPrice": "Open", "PreClosePrice": "PreClose", "LastPrice": "Price"}

        for index in range(1, 11):
            rename_columns["B" + str(index)] = "BidP" + str(index)
            rename_columns["S" + str(index)] = "AskP" + str(index)
            rename_columns["BV" + str(index)] = "BidV" + str(index)
            rename_columns["SV" + str(index)] = "AskV" + str(index)
        result.rename(columns=rename_columns, inplace=True)
        result["Code"] = stock_code
        result["TimeStamp"] = result.apply(axis=1, reduce=True,
                                           func=lambda x: int(dt.datetime.
                                                              strptime(str(x["Date"]) + " " + str(x["Time"]),
                                                                       "%Y%m%d %H%M%S000").timestamp()))
        result["Turover"] = result["AccTurover"].rolling(2).apply(calc_diff).fillna(0)
        result["Volume"] = result["AccVolume"].rolling(2).apply(calc_diff).fillna(0)

        del result["NumberTrades"]
        del result["TotalBidQty"]
        if 'IOPV' in result.columns:
            del result["IOPV"]
        del result["WavgBidPrice"]
        del result["TotalOfferQty"]
        del result["WavgOfferPrice"]
        if 'YTM' in result.columns:
            del result["YTM"]

        if stock_code[-1] == 'H':  # 上交所的股票或指数
            time_filter = ((result['Time'] >= 93000000) & (result['Time'] < 113000000)) | (
                    (result['Time'] >= 130000000) & (result['Time'] < 145957000))
        else:  # 深交所的股票或指数
            time_filter = ((result['Time'] >= 93000000) & (result['Time'] < 113000000)) | (
                    (result['Time'] >= 130000000) & (result['Time'] < 145657000))

        return clean_dataframe(result[time_filter])
    elif "contractCode" in result.columns:
        rename_columns = {"contractCode": "Code", "date": "Date", "totalTurnover": "AccTurover",
                          "totalVolume": "AccVolume", "highPrice": "High", "lowPrice": "Low",
                          "openPrice": "Open", "preClosePrice": "PreClose", "lastPrice": "Price",
                          "time": "Time"}

        for index in range(1, 11):
            rename_columns["bidPrice" + str(index)] = "BidP" + str(index)
            rename_columns["askPrice" + str(index)] = "AskP" + str(index)
            rename_columns["bidVolume" + str(index)] = "BidV" + str(index)
            rename_columns["askVolume" + str(index)] = "AskV" + str(index)
        result.rename(columns=rename_columns, inplace=True)
        result["Code"] = stock_code
        result["TimeStamp"] = result.apply(axis=1, reduce=True, func=lambda x: int(
            dt.datetime.strptime(str(x["Date"]) + " " + str(x["Time"]), "%Y%m%d %H%M%S000").timestamp()))
        result["Turover"] = result["AccTurover"].rolling(2).apply(calc_diff).fillna(0)
        result["Volume"] = result["AccVolume"].rolling(2).apply(calc_diff).fillna(0)

        del result["marketCode"]
        del result["upperLimitPrice"]
        del result["lowerLimitPrice"]
        del result["curVolume"]
        del result["curTurnover"]
        del result["curNumberTrades"]
        del result["totalNumberTrades"]
        del result["totalBidVolume"]
        del result["weightAvgBidPrice"]
        del result["weightAvgAskPrice"]
        del result["totalAskVolume"]
        del result["peratio1"]
        del result["peratio2"]
        del result["iopv"]
        del result["ytm"]

        if stock_code[-1] == 'H':  # 上交所的股票或指数
            time_filter = ((result['Time'] >= 93000000) & (result['Time'] < 113000000)) | (
                    (result['Time'] >= 130000000) & (result['Time'] < 145957000))
        else:  # 深交所的股票或指数
            time_filter = ((result['Time'] >= 93000000) & (result['Time'] < 113000000)) | (
                    (result['Time'] >= 130000000) & (result['Time'] < 145657000))

        return clean_dataframe(result[time_filter])
    else:
        rename_columns = {"InstrumentID": "Code", "TradingDay": "Date", "Turnover": "AccTurover",
                          "Volume": "AccVolume", "HighestPrice": "High", "LowestPrice": "Low",
                          "OpenPrice": "Open", "PreClosePrice": "PreClose", "LastPrice": "Price",
                          "Time": "Time"}

        for index in range(1, 11):
            rename_columns["BidPrice" + str(index)] = "BidP" + str(index)
            rename_columns["AskPrice" + str(index)] = "AskP" + str(index)
            rename_columns["BidVolume" + str(index)] = "BidV" + str(index)
            rename_columns["AskVolume" + str(index)] = "AskV" + str(index)
        result.rename(columns=rename_columns, inplace=True)
        result["Code"] = stock_code
        result["TimeStamp"] = result.apply(axis=1, reduce=True, func=lambda x: int(
            dt.datetime.strptime(str(x["Date"]) + " " + str(x["Time"]), "%Y%m%d %H:%M:%S").timestamp()))
        result["Time"] = \
            result["Time"].apply(lambda x: int(x[:2]) * 10000000 + int(x[3:5]) * 100000 + int(x[6:]) * 1000)
        result["Turover"] = result["AccTurover"].rolling(2).apply(calc_diff).fillna(0)
        result["Volume"] = result["AccVolume"].rolling(2).apply(calc_diff).fillna(0)

        if stock_code[-1] == 'H':  # 上交所的股票或指数
            time_filter = ((result['Time'] >= 93000000) & (result['Time'] < 113000000)) | (
                    (result['Time'] >= 130000000) & (result['Time'] < 145957000))
        else:  # 深交所的股票或指数
            time_filter = ((result['Time'] >= 93000000) & (result['Time'] < 113000000)) | (
                    (result['Time'] >= 130000000) & (result['Time'] < 145657000))

        return clean_dataframe(result[time_filter])


def read_single_month_file(root_path: str, stock_code: str, month: str) ->pd.DataFrame:
    """
    market data are stored as one month file
    :param root_path: root path that market data stored
    :param stock_code: symbol eg.000001.SH
    :param month: string eg.'2018-10'
    :return:
    """
    if (stock_code[0] == "0" and stock_code[-1] == "H") or (stock_code[:2] == "39" and stock_code[-1] == "Z"):
        # 如果是指数，则在指数文件夹下读取文档
        matfn = path.join(root_path, 'IndexTickData', stock_code, 'TickInfo_' + stock_code + '_' +
                          month + '.mat')
    else:
        matfn = path.join(root_path, 'StockTickData', stock_code, 'TickInfo_' + stock_code + '_' +
                          month + '.mat')
    matdata = sio.loadmat(open(matfn, 'rb'))  # 将matlab数据读进来，得到一个叫matdata的字典
    if len(matdata['TimeStamp']) == 0:  # 若股票整个月都停牌，则跳过
        print('{} {} 无数据'.format(stock_code, month))
        return None
    date_time_index = [i for i in range(len(matdata['TimeStamp']))]
    # for iTime in range(0, len(matdata['TimeStamp'])):
    #     DateTimeIndex[iTime] = dt.datetime.fromtimestamp(matdata['TimeStamp'][iTime])
    del matdata['__header__']  # 读入mat文件会生成这3个无用的数据，删去
    del matdata['__version__']
    del matdata['__globals__']
    temp_stock_data = pd.DataFrame(index=date_time_index)
    for key in matdata:
        temp_stock_data[key] = matdata[key]
    temp_stock_data['Code'] = stock_code
    return temp_stock_data


def read_single_day_file_transaction(root_path: str, stock_code:str, day:str) -> pd.DataFrame:
    tickfn = path.join(root_path, stock_code[:6] + "_" + day + ".csv")

    if not path.exists(tickfn):
        print("{} {} stock file {} not exist".format(stock_code, day, tickfn))
        return None

    result = pd.read_csv(tickfn, skipinitialspace=True)

    if len(result) < 500:
        print("{} {} stock file {} too short".format(stock_code, day, tickfn))
        return None

    if "Date" in result.columns:
        rename_columns = {"ContractCode": "Code", "TradePrice": "Price",
                          "TradeQty": "Volume"}

        result.rename(columns=rename_columns, inplace=True)
        result["TimeStamp"] = result.apply(axis=1, reduce=True,
                                           func=lambda x: int(dt.datetime.strptime(f'{x["Date"]} {x["Time"]:0>9}',
                                                                                   "%Y%m%d %H%M%S%f").timestamp()))
        result = result[result['Price'] > 0.00001]
        result["BsFlag"] = result.apply(axis=1, reduce=True, func=lambda x: 1 if x['BuyOrderRecNO'] > x['SellOrderRecNO'] else -1)
        result['Code'] = stock_code

        # del result["Date"]
        del result["SetNO"]
        del result["RecNO"]
        del result["BuyOrderRecNO"]
        del result["SellOrderRecNO"]
        del result["OrderKind"]
        del result["FunctionCode"]

        if stock_code[-1] == 'H':  # 上交所的股票或指数
            time_filter = ((result['Time'] >= 93000000) & (result['Time'] < 113000000)) | (
                (result['Time'] >= 130000000) & (result['Time'] < 145957000))
        else:  # 深交所的股票或指数
            time_filter = ((result['Time'] >= 93000000) & (result['Time'] < 113000000)) | (
                (result['Time'] >= 130000000) & (result['Time'] < 145657000))

        return result[time_filter]
    elif "contractCode" in result.columns:
        rename_columns = {"price": "Price", "volume": "Volume", "time": "Time", 'date': 'Date'}

        result.rename(columns=rename_columns, inplace=True)
        result["Code"] = stock_code
        result["TimeStamp"] = result.apply(axis=1, reduce=True,
                                           func=lambda x: int(dt.datetime.strptime(f'{x["Date"]} {x["Time"]:0>9}',
                                                                                   "%Y%m%d %H%M%S%f").timestamp()))
        result = result[result['Price'] > 0.00001]
        result["BsFlag"] = result.apply(axis=1, reduce=True, func=lambda x: 1 if x['bidOrderNo'] > x['askOrderNo'] else -1)

        del result["contractCode"]
        del result["marketCode"]
        # del result["Date"]
        del result["tradeIndex"]
        del result["tradeChannel"]
        del result["turnover"]
        del result["orderKind"]
        del result["functionCode"]
        del result["askOrderNo"]
        del result["bidOrderNo"]

        if stock_code[-1] == 'H':  # 上交所的股票或指数
            time_filter = ((result['Time'] >= 93000000) & (result['Time'] < 113000000)) | (
                (result['Time'] >= 130000000) & (result['Time'] < 145957000))
        else:  # 深交所的股票或指数
            time_filter = ((result['Time'] >= 93000000) & (result['Time'] < 113000000)) | (
                (result['Time'] >= 130000000) & (result['Time'] < 145657000))

        return result[time_filter]
