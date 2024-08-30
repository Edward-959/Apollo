"""
Created on 2018/8/11
@author: 006566
"""

import csv
import time
from DataAPI import DataToolkit as Dtk
import os


def update_complete_stock_list():
    """
    用于将新股自动更新到CompleteStockList.csv这一文件中；退市股票比较罕见，例子还没来得及写，后续再写吧。
    by 006566, 2018/8/11
    """
    today_int = int(time.strftime("%Y%m%d"))
    nearest_trade_date = Dtk.get_n_days_off(today_int, -1)[0]
    listed_stock = Dtk.get_listed_stocks(nearest_trade_date)  # “目前”处于上市状态的股票列表

    complete_stock_list_path = "S:\\Apollo\\CompleteStockList.csv"
    complete_stock_list = []
    if os.path.exists(complete_stock_list_path):
        file = open(complete_stock_list_path, 'r')
        stock_list_reader = csv.reader(file)
        for i,  line in enumerate(stock_list_reader):
            if i > 0:  # 因首行是表头，从第2行开始读起
                complete_stock_list.append(line[0])  # 近年来的股票列表全集（含退市股票）
        file.close()
        # 在listed_stock中有、而complete_stock_list中没有的，就是新上市的股票列表
        new_stock_list = list(set(listed_stock).difference(set(complete_stock_list)))
        if new_stock_list.__len__() > 0:
            for new_stock_code in new_stock_list:
                listing_date = Dtk.get_stock_latest_info(new_stock_code, 'Listing_date')
                # 'a' 表示从最后一行起开始追加
                with open(complete_stock_list_path, 'a', newline='') as f:
                    writer = csv.writer(f)
                    writer.writerow([new_stock_code, "NaN", listing_date])
                    print("New record added to CompleteStockList.csv:", new_stock_code, "which was listed on",
                          listing_date, ".")
                    f.close()
        else:
            print("No new stocks.")
    else:
        print("Error: cannot find the CompleteStockList file.")


if __name__ == "__main__":
    update_complete_stock_list()
