import pandas as pd
from DataAPI.DataToolkit import get_single_stock_minute_data, get_panel_daily_info, get_complete_stock_list
from datetime import datetime, timedelta
import argparse
import os

parser = argparse.ArgumentParser(description="")
parser.add_argument('--start', '-s', action='store_true', help='start date', default=20160101)
parser.add_argument('--end', '-e', action='store_true', help='end date', default=20180810)
parser.add_argument('--index', '-i', action='store_true', help='index need to merge', default='000300.SH')
parser.add_argument('--output', '-o', action='store_true', help='output directory',
                    default='S:/Apollo/merged_MarketData')
args = parser.parse_args()

index_weight_dict = {
    "000300.SH": 'index_weight_hs300'
}
time_start = datetime.now()
start_date_int = args.start
end_date_int = args.end
start_str = '{} 09:30:00'.format(start_date_int)
end_str = '{} 15:01:00'.format(end_date_int)
start_datetime = datetime.strptime(start_str, '%Y%m%d %H:%M:%S')
end_datetime = datetime.strptime(end_str, '%Y%m%d %H:%M:%S')
start = start_datetime
store = pd.HDFStore("{}/{}_v3.h5".format(args.output.strip("/"), args.index))

###################################################################
# 计算这段时间所有的指数的组合的合集
complete_stock_list = get_complete_stock_list()
stock_weight = get_panel_daily_info(complete_stock_list, start_date_int, end_date_int, index_weight_dict.get(args.index))
dailyStockPool = {}
for i_date in stock_weight.index:
    dailyStockPool.update({str(i_date): stock_weight.columns[stock_weight.loc[i_date, :] > 0]})
all_stock = set([])
for day_info in dailyStockPool.values():
    all_stock = all_stock.union(set(day_info))
all_stock = list(all_stock)
stock_num = all_stock.__len__()
####################################################################################
while start < end_datetime:
    start_date = start.year * 10000 + start.month * 100 + start.day
    end = start + timedelta(days=180)
    end = min(end, end_datetime)
    end_date = end.year * 10000 + end.month * 100 + end.day
    print(start_date, end_date)
    cell: pd.DataFrame = get_single_stock_minute_data(args.index, start_date, end_date, fill_nan=False,
                                                       append_pre_close=False, adj_type='NONE', drop_nan=True,
                                                       full_length_padding=False)
    cell["symbol"] = args.index
    cell = cell.reset_index()
    temp_store = pd.HDFStore("temp.h5")
    temp_store.append('data', cell, format="table")
    # block['symbol'] = args.index
    # block = block.reset_index()

    i = 1
    for code in all_stock:
        t2 = datetime.now()
        cell: pd.DataFrame = get_single_stock_minute_data(code, start_date, end_date, fill_nan=False,
                                                          append_pre_close=False, adj_type='NONE', drop_nan=True,
                                                          full_length_padding=False)
        cell["symbol"] = code
        cell = cell.reset_index()
        # block = block.append(cell)
        if cell.size == 0:
            i += 1
            continue
        temp_store.append('data', cell, format="table")
        print("{} {} / {} loaded from-to {} {} {}".format(code, i, stock_num, start_date, end_date, datetime.now()-t2))
        i += 1

    t3 = datetime.now()
    block = temp_store.select("data")
    block["dt"] = block["dt"].astype('int')
    block["minute"] = block["minute"].astype('int')
    block = block.sort_values(by=["dt", "minute"])
    store.append('data', block, format="table")
    store.flush()
    temp_store.close()
    os.remove("temp.h5")
    print("merge take {}".format(datetime.now()-t3))
    start = end + timedelta(days=1)
store.close()
print("take {} time".format(datetime.now() - time_start))

