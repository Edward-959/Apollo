# -*- coding: utf-8 -*-
"""
@author: 006688
Created on 2018/11/29
revised on 2019/02/25
局部流动性因子：i取值0，1，2，3，4，分别表示隔夜、日内第1~4个小时的换手率，
将过去n日对应时段换手率求和取对数(该日内因子的计算到此为止，剩下部分在FactorDailyTurnPartPure中计算)，
并参考日级别因子TurnPure的计算方法，在横截面上关于对数流通市值回归取残差，再将残差关于TurnPure回归取残差作为因子值
"""
from Factor.DailyMinFactorBase import DailyMinFactorBase
import DataAPI.DataToolkit as Dtk


class NonFactorDailyMinTurnPart(DailyMinFactorBase):
    def __init__(self, alpha_factor_root_path, stock_list, start_date_int, end_date_int, params):
        super().__init__(alpha_factor_root_path, stock_list, start_date_int, end_date_int)
        self.period = params['period']
        self.__stock_free_float_shares = Dtk.get_panel_daily_info(stock_list, start_date_int, end_date_int,
                                                                  info_type='free_float_shares')

    def single_stock_factor_generator(self, code):
        ############################################
        # 以下是数据计算逻辑的部分，需要用户自定义 #
        # 计算数据时，最后应得到factor_data这个DataFrame，涵盖的时间段是self.start_date至self.end_date（前闭后闭）；
        # factor_data的因子值一列，应当以股票代码为列名；
        # 最后factor_data的索引，应当是8位整数的交易日；
        # 获取分钟数据应当用get_single_stock_minute_data2这个函数
        ############################################
        stock_minute_data = Dtk.get_single_stock_minute_data2(code, self.start_date, self.end_date,
                                                              fill_nan=True, append_pre_close=False, adj_type='NONE',
                                                              drop_nan=False, full_length_padding=True)
        data_check_df = stock_minute_data.dropna()  # 用于检查行情的完整性
        if stock_minute_data.columns.__len__() > 0 and data_check_df.__len__() > 0:
            stock_minute_volume = stock_minute_data['volume'].unstack()
            if self.period == 0:
                stock_turn = stock_minute_volume.loc[:, 925] / self.__stock_free_float_shares.loc[:, code] / 100
            elif self.period == 1:
                stock_turn = stock_minute_volume.loc[:, 930:1029].sum(axis=1) / \
                             self.__stock_free_float_shares.loc[:, code] / 100
            elif self.period == 2:
                stock_turn = stock_minute_volume.loc[:, 1030:1129].sum(axis=1) / \
                             self.__stock_free_float_shares.loc[:, code] / 100
            elif self.period == 3:
                stock_turn = stock_minute_volume.loc[:, 1300:1359].sum(axis=1) / \
                             self.__stock_free_float_shares.loc[:, code] / 100
            elif self.period == 4:
                stock_turn = stock_minute_volume.loc[:, 1400:].sum(axis=1) / \
                             self.__stock_free_float_shares.loc[:, code] / 100
            factor_data = stock_turn.to_frame(code)
            factor_data = factor_data.loc[self.start_date: self.end_date]
        ########################################
        # 因子计算逻辑到此为止，以下勿随意变更 #
        ########################################
        else:  # 如取到的行情的DataFrame为空，则自造一个全为nan的DataFrame
            factor_data = self.generate_empty_df(code)
        return factor_data
