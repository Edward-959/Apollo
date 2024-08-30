import pandas as pd
import numpy as np
from datetime import datetime
from typing import List
import os.path
import platform
import xarray

if platform.system() == "Windows":  # 云桌面环境运行是Windows
    __ROOT_PATH__ = "S:\\Apollo\\Factors"
else:
    __ROOT_PATH__ = "/app/data/006566/Apollo/Factors/"


def load(factor_list: List[str], stock_list: List[str] = ...,
         start_date: datetime = None, end_date: datetime = None):
    """
    获取因子矩阵接口
    :param factor_list: 因子列表
    :param stock_list: 股票列表
    :param start_date: 开始日期 类型 datetime default=None 表示不设条件
    :param end_date:  结束日期 类型 datetime default=None 表示不设条件
    :return: 返回值为三维numpy数组， 0：日期， 1：因子名称 2：股票
    其中因子的顺序为输入列表的顺序， 股票顺序为输入列表的顺序
    """
    time_index = list(filter(lambda x: start_date.timestamp() <= x <= end_date.timestamp(),
                             get_index_of_factor(factor_list[0])))

    factors = []
    for factor in factor_list:
        factors.append(load_factor(factor_name=factor, stock_list=stock_list, start_date=start_date,
                                   end_date=end_date).as_matrix())
    data = np.array(factors).swapaxes(0, 1)
    return xarray.DataArray(data, coords=[time_index, factor_list, stock_list],
                            dims=["time_index", "factor_list", "stock_list"])


def load_factor(factor_name: str = ...,  stock_list: List[str]=...,
                start_date: datetime = None, end_date: datetime = None) -> pd.DataFrame:
    """
    获取单个因子数据的矩阵， 要求数据存储为一个因子一个文件，
    命名方式为因子名.h5, 文件中行为时间，列为股票
    :param factor_name: 因子名
    :param stock_list: 股票列表
    :param start_date: 开始日期 类型 datetime
    :param end_date:  结束日期 类型 datetime
    :return: 返回值为DataFrame
    """
    file_name = "{}/{}.h5".format(__ROOT_PATH__, factor_name)
    if not os.path.isfile(file_name):
        print("could not find Factor file: {}".format(factor_name))
        #  因子不存在
        return np.nan
    data = pd.HDFStore(file_name, mode='r')
    # 存的因子文件是以timestamp为index的，所以直接以index来筛选；这里result获得的是因子文件中所有股票指定时间内的数据
    result = data.select("/factor", "index >= start_date.timestamp() and index <= end_date.timestamp()")
    result2 = result[stock_list]
    data.close()
    return result2


def get_stock_list_of_factor(factor_name: str = ...) -> List[str]:
    """
    获取因子文件中所有股票代码
    :param factor_name: 因子名称
    :return: 股票列表
    """
    file_name = "{}/{}.h5".format(__ROOT_PATH__, factor_name)
    if not os.path.isfile(file_name):
        #  因子不存在
        return []
    data = pd.HDFStore(file_name, mode='r')
    index = data.select("/stock_list")
    index_list = list(index["code"])
    data.close()
    return index_list


def get_index_of_factor(factor_name: str = ...) -> List[str]:
    """
    获取因子文件中index,其数值为timestamp
    :param factor_name: 因子名称
    :return: list
    """
    file_name = "{}/{}.h5".format(__ROOT_PATH__, factor_name)
    if not os.path.isfile(file_name):
        #  因子不存在
        return []
    data = pd.HDFStore(file_name, mode='r')
    index = data.select_column("factor", "index")
    data.close()
    return index


def get_info_of_factor(factor_name: str = ...) -> tuple:
    """
    获取因子文件中股票列表和index,其数值为timestamp
    :param factor_name: 因子名称
    :return: (股票列表，索引)
    """
    file_name = "{}/{}.h5".format(__ROOT_PATH__, factor_name)
    if not os.path.isfile(file_name):
        #  因子不存在
        return ()
    data = pd.HDFStore(file_name, 'r')
    index = data.select_column("factor", "index")
    stock_list = data.select("/stock_list")
    stock_list = list(stock_list["code"])
    data.close()
    return stock_list, index


def restore_factor(factor_name: str = ...) -> None:
    file_name = "{}/{}.h5".format(__ROOT_PATH__, factor_name)
    if not os.path.isfile(file_name):
        print("Factor does't exist")
        return
    file_name_faster = "{}/{}_fast.h5".format(__ROOT_PATH__, factor_name)

    if os.path.isfile(file_name_faster):
        # 文件已经存在
        return
    store_ori = pd.HDFStore(file_name)
    store_new = pd.HDFStore(file_name_faster)
    keys = store_ori.keys()
    keys_df = pd.DataFrame(keys, columns=["code"])
    store_new.put(key="stock_list", value=keys_df)
    index = store_ori.select_column(keys[0], "index")
    left_rows = index.__len__()
    for chk in store_ori.select_as_multiple(keys, chunksize=1000):
        store_new.append("factor", chk, format="table")
        store_new.flush()
        left_rows = left_rows - chk.__len__()
        print("remains {} rows".format(left_rows))

    store_new.close()
    store_ori.close()
