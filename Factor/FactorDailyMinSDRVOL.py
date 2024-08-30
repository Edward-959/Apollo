from Factor.DailyMinFactorBase import DailyMinFactorBase
import DataAPI.DataToolkit as Dtk
import numpy as np
import pandas as pd


class FactorDailyMinSDRVOL(DailyMinFactorBase):
    def __init__(self, alpha_factor_root_path, stock_list, start_date_int, end_date_int, params):
        super().__init__(alpha_factor_root_path, stock_list, start_date_int, end_date_int)
        self.__n = params["n"]
        self.__start_date_minus_n_2 = Dtk.get_n_days_off(start_date_int, -(self.__n + 2))[0]
        self.__data_volume = Dtk.get_panel_daily_pv_df(stock_list, self.__start_date_minus_n_2, end_date_int,
                                                       pv_type='volume')

    def single_stock_factor_generator(self, code):
        # 注意，这里取数据请尽量使用get_single_stock_minute_data2，这个函数
        stock_minute_data = Dtk.get_single_stock_minute_data2(code, self.__start_date_minus_n_2, self.end_date,
                                                              fill_nan=True, append_pre_close=False, adj_type='NONE',
                                                              drop_nan=False, full_length_padding=True)
        data_check_df = stock_minute_data.dropna()  # 用于检查行情的完整性
        if stock_minute_data.columns.__len__() > 0 and data_check_df.__len__() > 0:
            stock_minute_open = stock_minute_data['open'].unstack()
            stock_minute_close = stock_minute_data['close'].unstack()
            stock_minute_ret = stock_minute_close / (stock_minute_open.shift(5, axis=1)) - 1

            complete_min_list = Dtk.get_complete_minute_list()
            temp_min_list = [imin for imin in complete_min_list if imin % 5 == 0]
            temp_min_list = temp_min_list[2:]

            stock_minute_5ret = stock_minute_ret[temp_min_list]
            stock_minute_RVOL = stock_minute_5ret.std(ddof=0, axis=1)

            stock_minute_cov1 = stock_minute_RVOL.ewm(halflife=self.__n).cov(stock_minute_RVOL.shift(1))
            stock_minute_cov2 = stock_minute_RVOL.ewm(halflife=self.__n).cov(stock_minute_RVOL.shift(2))
            # stock_minute_cov3 = stock_minute_RVOL.ewm(halflife=self.__n).cov(stock_minute_RVOL.shift(3))

            stock_StdEW = stock_minute_RVOL.ewm(halflife=self.__n).std() + 2 * (
                        2 / 3 * stock_minute_cov1 + 1 / 3 * stock_minute_cov2)
            factor_data = stock_StdEW

            factor_data = factor_data * self.__data_volume.loc[:, code] / self.__data_volume.loc[:, code]
            factor_data = factor_data.to_frame(code)
            factor_data = factor_data.loc[self.start_date: self.end_date].copy()
        ########################################
        # 因子计算逻辑到此为止，以下勿随意变更 #
        ########################################
        else:  # 如取到的行情的DataFrame为空，则自造一个全为nan的DataFrame
            factor_data = self.generate_empty_df(code)
        return factor_data
