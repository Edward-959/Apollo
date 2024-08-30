import os
from datetime import datetime, timedelta
import pandas as pd
from DataAPI.DataToolkit import get_single_stock_minute_data, get_panel_daily_info, get_complete_stock_list
from os import environ


def merge(code_list, start_date, end_date, output_name):
    user_id = environ['USER_ID']
    temp_store_dir = "/app/data/" + user_id + "/log/"
    if not os.path.exists(temp_store_dir):
        os.mkdir(temp_store_dir)
    start_date_int = start_date
    end_date_int = end_date
    start_str = '{} 09:24:00'.format(start_date_int)
    end_str = '{} 15:01:00'.format(end_date_int)
    start_datetime = datetime.strptime(start_str, '%Y%m%d %H:%M:%S')
    end_datetime = datetime.strptime(end_str, '%Y%m%d %H:%M:%S')
    start = start_datetime
    store = pd.HDFStore(output_name)
    while start < end_datetime:
        start_date = start.year * 10000 + start.month * 100 + start.day
        end = start + timedelta(days=90)
        end = min(end, end_datetime)
        if end.year > start.year:
            temp_end_str = '{}{} 15:01:00'.format(start.year, 1231)
            end = datetime.strptime(temp_end_str, '%Y%m%d %H:%M:%S')
        end_date = end.year * 10000 + end.month * 100 + end.day
        print(start_date, end_date)
        stock_num = code_list.__len__()
        i = 1
        temp_store = pd.HDFStore(temp_store_dir + "temp.h5")
        is_data_appended = False  # 为了应对可能每年最后一段日期是非交易日的情况
        for symbol in code_list:
            cell: pd.DataFrame = get_single_stock_minute_data(symbol, start_date, end_date, fill_nan=False,
                                                              append_pre_close=False, adj_type='NONE', drop_nan=True,
                                                              full_length_padding=False)
            if cell.__len__() == 0:
                continue
            cell["symbol"] = symbol
            cell = cell.reset_index()
            temp_store.append('data', cell, format='table', data_columns=True)
            is_data_appended = True
            print("{} {} / {} loaded from-to {} {}".format(symbol, i, stock_num, start_date, end_date))
            i += 1
        if is_data_appended:
            block = temp_store.select("data")
            block["dt"] = block["dt"].astype('int')
            block["minute"] = block["minute"].astype('int')
            block = block.sort_values(by=["dt", "minute"])
            store.append('data/y{}'.format(start.year), block, format="table", data_columns=True)
            store.flush()
        temp_store.close()
        os.remove(temp_store_dir + "temp.h5")
        start = end + timedelta(days=1)
    store.close()


def main():
    # ======================= 以上是读取所有股票的代码，如要merge自己的股票池、请另传代码=======================
    file_directory = "/app/data/006566/UnadjustedStockMinData/MINUTE/stock"
    filename_list = os.listdir(file_directory)

    file_code_list = []
    for code in filename_list:
        xcode = code[21:27]
        if xcode.startswith('6'):
            xcode += ".SH"
        else:
            xcode += ".SZ"
        file_code_list.append(xcode)

    file_directory = "/app/data/006566/UnadjustedStockMinData/MINUTE/index"
    filename_list = os.listdir(file_directory)
    for code in filename_list:
        xcode = code[12:18]
        xcode += ".SH"
        file_code_list.append(xcode)
    # ======================= 以上是读取所有股票的代码，如要merge自己的股票池、请另传代码=======================

    complete_stock_list = get_complete_stock_list()
    df4 = get_panel_daily_info(complete_stock_list, 20150101, 20180630, 'index_500')
    df4_max = df4.max()
    df4_code_list = list(df4_max[df4_max == 1].index)
    df4_code_list.append("000905.SH")  # 请务必记得把指数也一起加进来

    # ======================= 以上是读20150101至20180630期中证500成分股的代码，如要merge自己的股票池、请另传代码=======================

    user_id = environ['USER_ID']
    output_file_dir = "/app/data/" + user_id + "/log/"

    file_code_list = file_code_list
    output_file_name = "merged_market_data_index500_20150101to20180630.h5"  # 注意改文件名字
    output_path = output_file_dir + output_file_name
    merge_start_date = 20150101
    merge_end_date = 20180630
    merge(file_code_list, merge_start_date, merge_end_date, output_path)


if __name__ == "__main__":
    main()
