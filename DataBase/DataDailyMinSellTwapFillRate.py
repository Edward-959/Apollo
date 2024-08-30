"""
created on 2019/03/12
计算按照twap进行交易能够实现的平均价格以及比例
相比与原先的twap，主要考虑了盘中涨跌停的情况
在卖出过程中，如果当前分钟K线的收盘价等于跌停价，则无法成交，能够实现的成交比例相应减少
"""
from Factor.DailyMinFactorBase import DailyMinFactorBase
import DataAPI.DataToolkit as Dtk
import numpy as np


class DataDailyMinSellTwapFillRate(DailyMinFactorBase):
    def __init__(self, alpha_factor_root_path, stock_list, start_date, end_date, params):
        super().__init__(alpha_factor_root_path, stock_list, start_date, end_date)
        self.start_date = start_date
        self.end_date = end_date
        self.pre_close = Dtk.get_panel_daily_pv_df(stock_list, start_date, end_date, 'pre_close')

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
            pre_close = self.pre_close[code]
            # 由于计算精度的问题，出现小数点后第3为5的情况，可能向下取，故加0.01确保不低于真实的跌停价
            limit_down_price = round(pre_close * 0.9, 2) + 0.01
            stock_minute_data_close = stock_minute_data['close'].unstack()
            # 如果分钟K线收盘价小于或等于跌停价，为0，否则为1
            if_not_limit_down = np.sign(np.round(stock_minute_data_close.sub(limit_down_price, axis=0), 2)).clip(lower=0)
            stock_twap_all_day = if_not_limit_down.sum(axis=1) / 242
            stock_minute_data_amt = stock_minute_data['amt'].unstack()
            stock_amt_all_day = stock_minute_data_amt.mean(axis=1)
            # 先乘以成交额，再除以成交额，如果这段时间的成交额为0，则价格会变成nan
            stock_twap_all_day = stock_twap_all_day * stock_amt_all_day / stock_amt_all_day
            # 将stock_twap_all_day转为DataFrame, 列名是股票代码
            factor_data = stock_twap_all_day.to_frame(code)
        ########################################
        # 因子计算逻辑到此为止，以下勿随意变更 #
        ########################################
        else:  # 如取到的行情的DataFrame为空，则自造一个全为nan的DataFrame
            factor_data = self.generate_empty_df(code)
        return factor_data
