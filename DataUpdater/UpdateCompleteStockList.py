"""
Created on 2019/2/25
Author: 006566
revised on 2019/2/28 删去额外添加的000022.SZ
revised on 2019/3/20 上市两日之内的股票不再列入
"""
import pandas as pd
import os
import DataAPI.DataToolkit as Dtk
import time

# 保留latest_delisting_date之后退市的股票
latest_delisting_date = 20081231
output_file_path = '/app/data/666889/Apollo/AlphaDataBase/CompleteStockList.csv'


def main():
    sheet_name = 'AShareDescription'
    field_list = ['s_info_listdate', 's_info_delistdate']
    # 将字段变成大写（因为h5文件中的字段是大写）
    field_list = [i.upper() for i in field_list]
    address = '/app/data/wdb_h5/WIND/' + sheet_name + '/' + sheet_name + '.h5'
    store = pd.HDFStore(address, mode='r')
    list_delist_date_df = store.select("/" + sheet_name)
    store.close()
    # 仅保留上市日期、退市日期两个字段
    list_delist_date_df = list_delist_date_df[field_list]
    # 原始的DataFrame是双重索引(dt, Ticker)，这两将索引解除、变成普通的列，并删去'dt'这一列
    list_delist_date_df = list_delist_date_df.reset_index()
    list_delist_date_df = list_delist_date_df.drop('dt', axis=1)
    # 修改三列的字段名
    list_delist_date_df.columns = ['Stock_code', 'Listing_date', 'Delisting_date']
    # 将上市日期为NaN的记录删除，然后用0填充NaN
    list_delist_date_df = list_delist_date_df.dropna(subset=['Listing_date'])
    list_delist_date_df = list_delist_date_df.fillna(0)
    # 将数据类型强制转化为int
    list_delist_date_df['Listing_date'] = list_delist_date_df['Listing_date'].astype('int')
    list_delist_date_df['Delisting_date'] = list_delist_date_df['Delisting_date'].astype('int')
    # 保留退市日期晚于latest_delisting_date的股票以及未退市的股票
    list_delist_date_df = list_delist_date_df[(list_delist_date_df.Delisting_date > latest_delisting_date) |
                                              (list_delist_date_df.Delisting_date == 0)]
    stock_code_list = list(list_delist_date_df['Stock_code'])
    list_delist_date_df = list_delist_date_df.set_index('Stock_code')
    # 删去股票代码以A或T开头的
    invalid_stock_code_list = [code for code in stock_code_list if (code[0] == 'A' or code[0] == 'T')]
    list_delist_date_df = list_delist_date_df.drop(invalid_stock_code_list, axis=0)
    # 删去上市不足2日的股票
    today_int = int(time.strftime("%Y%m%d"))
    last_2_traing_day = Dtk.get_n_days_off(today_int, -2)
    invalid_stock_code_list = [code for code in stock_code_list if
                               list_delist_date_df.at[code, 'Listing_date'] in last_2_traing_day]
    list_delist_date_df = list_delist_date_df.drop(invalid_stock_code_list, axis=0)
    # # 000022.SZ 深赤湾在Wind库中改了代码，历史上这只股票的数据都移给了001872，这里单列处理
    # 2019/2/28 发现日级别数据库中所有000022.SZ的数据都不存在了，所以也不额外添加这只股票
    # list_delist_date_df.at[('000022.SZ', 'Listing_date')] = list_delist_date_df.at[('001872.SZ', 'Listing_date')]
    # list_delist_date_df.at[('000022.SZ', 'Delisting_date')] = 20181226
    # 将数据类型强制转化为int
    list_delist_date_df['Listing_date'] = list_delist_date_df['Listing_date'].astype('int')
    list_delist_date_df['Delisting_date'] = list_delist_date_df['Delisting_date'].astype('int')

    if os.path.exists(output_file_path):
        os.remove(output_file_path)
    list_delist_date_df.to_csv(output_file_path)


if __name__ == '__main__':
    main()
