from DataUpdater.MinDataUpdater import minute_data_updater
import time
import DataAPI.DataToolkit
import DataAPI.DataToolkit as Dtk

index_list = ["000001.SH", "000016.SH", "000300.SH", "000905.SH", "000906.SH", "399001.SZ", "399006.SZ"]

today_int = int(time.strftime("%Y%m%d"))
day_before_yesterday = Dtk.get_n_days_off(today_int, -3)[0]
yesterday = Dtk.get_n_days_off(today_int, -3)[1]
start_date = day_before_yesterday
end_date = yesterday

for index in index_list:
    print(index)
    minute_data_updater(index, start_date, end_date, mode='index')
