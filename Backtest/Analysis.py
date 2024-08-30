# -*- coding: utf-8 -*-
"""
Created on 2018/11/1 15:50

@author: 006547
"""
from datetime import datetime
import csv
import pickle
file_name = 'D:\\Apollo\\Backtest\\alphapositon2016-2017.csv'
with open(file_name) as f:
    reader = csv.reader(f)
    daily_stock_pool = {}
    row1 = []
    import numpy as np
    for row in reader:
        if reader.line_num == 1:
            row1 = row
        else:
            temp_date = row[0]
            temp_date = datetime.strptime(temp_date, '%Y/%m/%d')
            temp_row = np.array(row)
            row_code = np.array(row1)[np.argwhere(temp_row != '').flatten()]
            daily_stock_pool.update({temp_date.strftime('%Y%m%d'): row_code[1:].tolist()})

file_name2 = 'D:\\Apollo\\Backtest\\position_20181102_132918_jinrong.csv'
with open(file_name2) as f:
    reader2 = csv.reader(f)
    daily_stock_pool2 = {}
    row1 = []
    import numpy as np
    temp_date_old = ''
    row_code = []
    for row in reader2:
        if reader2.line_num == 1:
            row1 = row
        else:
            temp_date = row[0]
            if temp_date == temp_date_old or temp_date_old == '':
                temp_code = row[1].strip()
                if int(row[2]) >= 100:
                    row_code.append(temp_code)
                temp_date_old = temp_date
            else:
                row_code.sort()
                daily_stock_pool2.update({temp_date_old: row_code})
                row_code = []
                temp_code = row[1].strip()
                row_code.append(temp_code)
                temp_date_old = temp_date

result = {}
for key in list(daily_stock_pool.keys()):
    result_row = [0, 0, 0, 0]
    if key in daily_stock_pool2.keys():
        pool1_len = daily_stock_pool[key].__len__()
        pool2_len = daily_stock_pool2[key].__len__()
        copy_pool1 = daily_stock_pool[key].copy()
        copy_pool1.extend(daily_stock_pool2[key])
        set_len = set(copy_pool1).__len__()
        result_row[0] = pool1_len
        result_row[1] = pool1_len + pool2_len - set_len
        result_row[2] = set_len - pool1_len
        result_row[3] = set_len - pool2_len
        result.update({key: result_row})
pass
# with open('daily_stock_pool_jingong500.pickle', 'wb') as f:
#     pickle.dump(daily_stock_pool, f)