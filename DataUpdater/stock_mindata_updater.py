import DataAPI.DataToolkit as Dtk
from DataUpdater.MinDataUpdater import minute_data_updater
import time

stock_list = Dtk.get_complete_stock_list(drop_delisted_stocks=True)

today_int = int(time.strftime("%Y%m%d"))
day_before_yesterday = Dtk.get_n_days_off(today_int, -3)[0]
yesterday = Dtk.get_n_days_off(today_int, -3)[1]
start_date = day_before_yesterday
end_date = yesterday

for j, stock in enumerate(stock_list):
    print(stock)
    minute_data_updater(stock, start_date, end_date)
    if j == 1 or j % 50 == 0:
        print("{}/{} stocks updated.".format(j, stock_list.__len__()))
