# -*- coding: utf-8 -*-
# @Time    : 2018/12/10 19:27
# @Author  : 011673
# @File    : FactorTestloader.py
import datetime
from typing import List
import pandas as pd
import os
import numpy as np


def load_factor(factor_name: str = ..., stock_list: List[str] = ..., start_date: datetime = None,
                end_date: datetime = None, path: str = "S:\\Apollo\\AlphaFactors\\") -> pd.DataFrame:
    """
    获取单个因子数据的矩阵， 要求数据存储为一个因子一个文件，
    命名方式为因子名.h5, 文件中行为时间，列为股票
    :param factor_name: 因子名
    :param stock_list: 股票列表
    :param start_date: 开始日期 类型 datetime
    :param end_date:  结束日期 类型 datetime
    :return: 返回值为DataFrame
    """
    file_name = "{}/{}.h5".format(path, factor_name)
    if not os.path.isfile(file_name):
        print("could not find Factor file: {}".format(factor_name))
        #  因子不存在
        return np.nan
    data = pd.HDFStore(file_name, mode='r')
    # 存的因子文件是以timestamp为index的，所以直接以index来筛选；这里result获得的是因子文件中所有股票指定时间内的数据
    result = data.select("/factor", "index >= start_date.timestamp() and index <= end_date.timestamp()")
    result2 = result.reindex(columns=stock_list)
    data.close()
    return result2
