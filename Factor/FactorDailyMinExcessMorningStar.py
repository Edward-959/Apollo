# -*- coding: utf-8 -*-
# @Time    : 2018/11/30 13:53
# @Author  : 011673
# @File    : FactorDailyMinExcessMorningStar.py
# （1）以股票第 T-1 日的收盘价 P_(T-1,Close)为基准，计算股票第 T
# 日中截止至第 t 分钟的累积收益率：Rs_(T,t) = P_(T,t)/P_(T-1,Close)-1；
# （2）以指数第 T-1 日的收盘价 Q_(T-1,Close)为基准，计算指数第 T
# 日中截止至第 t 分钟的累积收益率：Ri_(T,t) = Q_(T,t)/Q_(T-1,Close)-1；
# （3）基于上述两个变量，计算股票第 T 日中第 t 分钟的累积超
# 额收益：Re_(T,t) = Rs_(T,t)-Ri_(T,t)；
# （4）取第 T 日累积超额收益的开盘值、最高值、最低值、收盘
# 值，构成第 T 日的超额蜡烛图。
# 在超额蜡烛图的基础上，我们可以仿照传统十字星的参数化定
# 义，以参数(h=0.1%,a=3,b=3)为标准识别超额十字星。
from Factor.MinFactorBase import MinFactorBase
import pandas as pd
import numpy as np
import logging
from typing import List
import platform
import DataAPI.DataToolkit as Dtk
from scipy import stats


def max_value(df1:pd.Series,df2:pd.Series):
    condition=df1>df2
    result=pd.Series(np.nan,index=df1.index)
    result[condition==True]=df1
    result[condition==False]=df2
    return result

def min_value(df1:pd.Series,df2:pd.Series):
    condition=df1>df2
    result=pd.Series(np.nan,index=df1.index)
    result[condition==True]=df2
    result[condition==False]=df1
    return result

def get_value(data:bool):
    return float(data)

class DailyMin(MinFactorBase):
    def __init__(self, codes: List[str] = ..., start_date_int: int = ..., end_date_int: int = ..., save_path: str = ...,
                 name: str = ..., index_code: str = ...):
        super().__init__(codes, start_date_int, end_date_int, save_path, name)
        self.__index_code = index_code
        index_minute_data = Dtk.get_single_stock_minute_data(index_code, start_date_int, end_date_int,
                                                             fill_nan=True, append_pre_close=True, adj_type='NONE',
                                                             drop_nan=False, full_length_padding=True)
        index_close = index_minute_data['close'].unstack()
        self.__index_ret = index_close / index_minute_data['pre_close'].unstack() - 1
        # self.__trading_days = self.__index_minute_ret.index.tolist()

    def single_stock_factor_generator(self, code: str = ..., start: int = ..., end: int = ...):
        ############################################
        # 以下是数据计算逻辑的部分，需要用户自定义 #
        # 计算数据时，最后应得到factor_data这个对象，类型应当是DataFrame，涵盖的时间段是start至end（前闭后闭）；
        # factor_data的因子值一列，应当以股票代码为列名；
        # 最后factor_data的索引，应当从原始分钟数据中获得的dt，即start至end期间的交易日，内容的格式是20180904这种8位数字
        ############################################
        stock_minute_data = Dtk.get_single_stock_minute_data(code, start, end, fill_nan=True, append_pre_close=True,
                                                             adj_type='NONE', drop_nan=False, full_length_padding=True)
        data_check_df = stock_minute_data.dropna()
        if stock_minute_data.columns.__len__() > 0 and data_check_df.__len__() > 0:
            close = stock_minute_data['close'].unstack()
            ret = close / stock_minute_data['pre_close'].unstack() - 1
            excess:pd.DataFrame=ret-self.__index_ret
            open_=excess.iloc[:,0]
            close=excess.iloc[:,-1]
            high=excess.max(axis=1)
            low=excess.min(axis=1)
            h=abs(open_-close)
            condition_1=(h<0.001)
            condition_2=(high-max_value(open_,close))>3*h
            condition_3= (min_value(open_,close)-low)>3*h
            factor_data:pd.DataFrame=condition_1 & condition_2 &condition_3
            factor_data=factor_data.apply(get_value)
            factor_data = pd.DataFrame(factor_data, index=close.index, columns=[code])
            factor_data.index.name='index'
        ########################################
        # 数据计算逻辑到此为止，以下勿随意变更 #
        ########################################
        else:  # 如取到的行情的DataFrame为空，则自造一个全为nan的DataFrame
            date_list = Dtk.get_trading_day(start, end)
            factor_data = pd.DataFrame(index=date_list)  # 新建一个空的DataFrame, 且先设好了索引
            temp_array = np.empty(shape=[date_list.__len__(), ])
            temp_array[:] = np.nan
            factor_data[code] = temp_array
        # 因子应当以timestamp作为索引
        factor_data = factor_data.reset_index()
        date_list = Dtk.convert_date_or_time_int_to_datetime(factor_data['index'].tolist())
        timestamp_list = [i_date.timestamp() for i_date in date_list]
        factor_data['index'] = timestamp_list
        factor_data = factor_data.set_index(['index'])
        factor_data = factor_data[[code]].copy()
        # print(factor_data.sum())
        logging.info("finished calc {}".format(code))
        return factor_data


def main():
    logging.basicConfig(level=logging.INFO)
    stock_code_list = Dtk.get_complete_stock_list()
    if platform.system() == 'Windows':
        save_dir = "S:\\Apollo\\AlphaFactors\\"  # 保存于S盘的地址
    else:
        save_dir = "/app/data/006566/Apollo/Factors"  # 保存于XQuant的地址
    ###############################################
    # 以下3行及factor_generator的类名需要自行改写 #
    ###############################################
    istart_date_int = 20141201
    iend_date_int = 20180630
    index_code = '000300.SH'
    factor_name = "F_D_MinExcessMorning_" + index_code[0:6]
    file_name = factor_name
    factor_generator = DailyMin(codes=stock_code_list, start_date_int=istart_date_int,
                                    end_date_int=iend_date_int, name=file_name, save_path=save_dir,
                                    index_code=index_code)
    factor_generator.launch()
    logging.info("program is stopped")


if __name__ == '__main__':
    main()
