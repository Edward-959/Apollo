# -*- coding: utf-8 -*-
# @Author  : 006566
from Factor.DailyMinFactorBase import DailyMinFactorBase
import DataAPI.DataToolkit as Dtk
import pandas as pd


class NonFactorDailyMinBCVP(DailyMinFactorBase):
    def __init__(self, alpha_factor_root_path, stock_list, start_date_int, end_date_int, params):
        super().__init__(alpha_factor_root_path, stock_list, start_date_int, end_date_int)

    def single_stock_factor_generator(self, code):
        ############################################
        # 以下是数据计算逻辑的部分，需要用户自定义 #
        # 计算数据时，最后应得到factor_data这个DataFrame，涵盖的时间段是self.start_date至self.end_date（前闭后闭）；
        # factor_data的因子值一列，应当以股票代码为列名；
        # 最后factor_data的索引，应当是8位整数的交易日；
        # 获取分钟数据应当用get_single_stock_minute_data2这个函数。
        ############################################
        stock_minute_data = Dtk.get_single_stock_minute_data2(code, self.start_date, self.end_date,
                                                              fill_nan=True, append_pre_close=False, adj_type='NONE',
                                                              drop_nan=False, full_length_padding=True)
        data_check_df = stock_minute_data.dropna()  # 用于检查行情的完整性
        if stock_minute_data.columns.__len__() > 0 and data_check_df.__len__() > 0:
            stock_minute_volume = stock_minute_data['volume'].unstack()
            stock_minute_volume_before_closing = stock_minute_volume.loc[:, 1456:].sum(axis=1)
            stock_minute_volume_all_day = stock_minute_volume.sum(axis=1)
            factor_value = stock_minute_volume_before_closing / stock_minute_volume_all_day
            factor_data = pd.DataFrame(factor_value, index=list(stock_minute_volume.index), columns=[code])
        ########################################
        # 因子计算逻辑到此为止，以下勿随意变更 #
        ########################################
        else:  # 如取到的行情的DataFrame为空，则自造一个全为nan的DataFrame
            factor_data = self.generate_empty_df(code)
        return factor_data