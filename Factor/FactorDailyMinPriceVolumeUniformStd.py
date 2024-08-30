# -*- coding: utf-8 -*-
# @Time    : 2018/12/25 8:40
# @Author  : 011673
# @File    : FactorDailyMinPriceVolumeUniformStd.py
#   计算5分钟价量相关性指标 ret/abs(ret)*abs(ret)*volume 在每一日时间序列的方差，并按10日周期取ema
from Factor.MinFactorBase import MinFactorBase
import pandas as pd
import numpy as np
import logging
from typing import List
import platform
import DataAPI.DataToolkit as Dtk
import os
import copy


class FactorDailyMinVolumeUniformStd(MinFactorBase):
    def __init__(self, codes: List[str] = ..., start_date_int: int = ..., end_date_int: int = ...,
                 save_path: str = ..., name: str = ...):
        super().__init__(codes, start_date_int, end_date_int, save_path, name)
        self._ema = True if name[-3:] == 'ema' else False
        # 所有要用到的日级别信息，应在此获取

    def single_stock_factor_generator(self, code: str = ..., start: int = ..., end: int = ...):
        stock_minute_data = Dtk.get_single_stock_minute_data(code, start, end, fill_nan=True, append_pre_close=False,
                                                             adj_type='None', drop_nan=False, full_length_padding=True)
        data_check_df = stock_minute_data.dropna()  # 用于检查行情的完整性
        if stock_minute_data.columns.__len__() > 0 and data_check_df.__len__() > 0:  # 如可正常取到行情DataFrame
            ############################################
            # 以下是数据计算逻辑的部分，需要用户自定义 #
            # 计算数据时，最后应得到factor_data这个对象，类型应当是DataFrame，涵盖的时间段是start至end（前闭后闭）；
            # factor_data的因子值一列，应当以股票代码为列名；
            # 最后factor_data的索引，应当从原始分钟数据中获得的dt，即start至end，内容的格式是20180904这种8位数字
            ############################################
            volume = stock_minute_data['volume'].unstack().T
            close=stock_minute_data['close'].unstack().T
            volume=volume.iloc[1:,:]
            close=close.iloc[1:,:]
            volume_list=[]
            ret_list=[]
            timer=0
            while timer < len(volume.index):
                if timer+10>len(volume.index):
                    ret_list.append(close.iloc[-1,:]/close.iloc[timer,:]-1)
                    volume_list.append(volume.iloc[timer:, :].sum())
                    break
                else:
                    ret_list.append(close.iloc[timer+5,:]/close.iloc[timer,:]-1)
                    volume_list.append(volume.iloc[timer:timer+5,:].sum())
                timer+=5
            ret_5min=pd.DataFrame(ret_list)
            volume_5min=pd.DataFrame(volume_list)
            del volume_list,ret_list
            direction=copy.deepcopy(ret_5min)
            direction[direction>0]=1
            direction[direction<0]=-1
            ret_5min_abs=abs(ret_5min)
            volume_5min=volume_5min*ret_5min_abs
            volume_5min.replace(0,1,inplace=True)
            volume_5min=pd.DataFrame(np.log(volume_5min.values),index=volume_5min.index,columns=volume_5min.columns)
            volume_5min.replace(np.inf,np.nan,inplace=True)
            volume_5min=volume_5min*direction
            factor_data=volume_5min.std()
            factor_data = pd.DataFrame(factor_data, index=list(volume.T.index), columns=[code])
            factor_data = factor_data.ewm(com=10).mean()
        ########################################
        # 因子计算逻辑到此为止，以下勿随意变更 #
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
        logging.info("finished calc {}".format(code))
        # print(factor_data.head)
        return factor_data


def main():
    logging.basicConfig(level=logging.INFO)
    stock_code_list = Dtk.get_complete_stock_list()
    if platform.system() == 'Windows':
        # save_dir = "S:\\Apollo\\AlphaFactors\\"  # 保存于云桌面的地址
        save_dir = "D:\\AlphaFactors\\"  # 保存于云桌面的地址
    else:
        user_id = os.environ['USER_ID']
        save_dir = "/app/data/" + user_id + "/AlphaFactors"  # 保存于XQuant的地址
    if not os.path.exists(save_dir):
        os.mkdir(save_dir)
    ###############################################
    # 以下3行及factor_generator的类名需要自行改写 #
    ###############################################
    istart_date_int = 20141201
    iend_date_int = 20180630
    factor_name = "F_D_MinPriceVolumeUniformStd_ema"  # 这个因子名可以加各种后缀，用于和相近的因子做区分
    file_name = factor_name
    factor_generator = FactorDailyMinVolumeUniformStd(codes=stock_code_list, start_date_int=istart_date_int,
                                                 end_date_int=iend_date_int, name=file_name,
                                                 save_path=save_dir)
    factor_generator.launch()
    logging.info("program is stopped")


if __name__ == '__main__':
    main()
