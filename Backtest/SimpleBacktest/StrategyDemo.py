"""
相对快速进行策略的回测，以单股票为粒度，进行并行计算
支持成交量的撮合、每日收益情况统计
输出包括委托列表、成交列表、每日收益、交易分析
"""
import DataAPI.DataToolkit as Dtk
from pandas import Series
import multiprocessing
import datetime as dt
from Backtest.SimpleBacktest.OrderProcessor import Order, OrderProcessor, analyze_and_export_result


class Strategy:
    def __init__(self, start_date, end_date, daily_stock_pool, hedge_index):
        self.__start_date = start_date
        self.__end_date = end_date
        self.__trading_days = Dtk.get_trading_day(self.__start_date, self.__end_date)
        self.__daily_stock_pool = daily_stock_pool
        self.__total_stock_list = set([])
        for date in self.__daily_stock_pool.values():
            self.__total_stock_list = self.__total_stock_list.union(set(date))
        self.__total_stock_list = list(self.__total_stock_list)
        self.__hedge_index = hedge_index
        self.__lag = 20
        self.__cost = 0.0012
        self.__prepare_daily_data()
        self.__op = OrderProcessor()

    # 准备日度数据，进行日度因子的计算
    def __prepare_daily_data(self):
        start_date_minus_lag_2 = Dtk.get_n_days_off(self.__start_date, -(self.__lag + 2))[0]
        self.__adj_factor_df = Dtk.get_panel_daily_info(self.__total_stock_list, start_date_minus_lag_2,
                                                        self.__end_date, info_type="adjfactor")
        self.__stock_daily_close_none = Dtk.get_panel_daily_pv_df(self.__total_stock_list, start_date_minus_lag_2,
                                                                  self.__end_date, pv_type='close', adj_type='NONE')
        self.__stock_daily_close = Dtk.get_panel_daily_pv_df(self.__total_stock_list, start_date_minus_lag_2,
                                                             self.__end_date, pv_type='close', adj_type='FORWARD')
        self.__stock_daily_high = Dtk.get_panel_daily_pv_df(self.__total_stock_list, start_date_minus_lag_2,
                                                            self.__end_date, pv_type='high', adj_type='FORWARD')
        self.__stock_daily_low = Dtk.get_panel_daily_pv_df(self.__total_stock_list, start_date_minus_lag_2,
                                                           self.__end_date, pv_type='low', adj_type='FORWARD')
        self.__stock_daily_pre_close = Dtk.get_panel_daily_pv_df(self.__total_stock_list, start_date_minus_lag_2,
                                                                 self.__end_date, pv_type='pre_close', adj_type='NONE')
        self.__stock_daily_pre_high = self.__stock_daily_high.shift(1)
        self.__stock_daily_high_max = self.__stock_daily_high.rolling(self.__lag).max().shift(1)
        self.__stock_daily_low_min = self.__stock_daily_low.rolling(self.__lag).min().shift(1)
        self.__stock_daily_high_idxmax = self.__stock_daily_high.rolling(self.__lag).apply(
            lambda x: Series(x).idxmax(), raw=True).shift(1)
        self.__stock_daily_low_idxmin = self.__stock_daily_low.rolling(self.__lag).apply(
            lambda x: Series(x).idxmin(), raw=True).shift(1)
        self.__hedge_index_min_data = Dtk.get_single_stock_minute_data(self.__hedge_index, self.__start_date,
                                                                       self.__end_date, fill_nan=True,
                                                                       append_pre_close=False, adj_type='NONE',
                                                                       drop_nan=False, full_length_padding=True)

    # 对单股票进行交易的回测和记录
    def single_stock_trading(self, code):
        stock_min_data = Dtk.get_single_stock_minute_data(code, self.__start_date, self.__end_date, fill_nan=True,
                                                          append_pre_close=False, adj_type='NONE', drop_nan=False,
                                                          full_length_padding=True)
        self.__op.initiate_data(code, stock_min_data, self.__stock_daily_close_none.loc[:, code])
        cut_loss_price = None
        cut_profit_price = None
        for date_i, date in enumerate(self.__trading_days):
            # 每日盘前数据处理
            if date_i > 0:
                yesterday = self.__trading_days[date_i-1]
                today = date
                self.__op.on_new_day(code, yesterday, today)
            if code not in self.__daily_stock_pool[date] and self.__op.position() == 0:
                continue
            high_max = self.__stock_daily_high_max.at[date, code]
            low_min = self.__stock_daily_low_min.at[date, code]
            high_idxmax = self.__stock_daily_high_idxmax.at[date, code]
            low_idxmin = self.__stock_daily_low_idxmin.at[date, code]
            open_signal = low_idxmin - high_idxmax > 10 and low_idxmin > 15 and low_min / high_max - 1 < -0.2
            if not open_signal and self.__op.position() == 0:
                continue
            min_data_close = stock_min_data.loc[date, 'close']
            min_data_high = stock_min_data.loc[date, 'high']
            min_data_low = stock_min_data.loc[date, 'low']
            min_data_time = stock_min_data.loc[date].index.tolist()
            adj_factor = self.__adj_factor_df.at[date, code]
            inday_high = None
            inday_low = None
            # 播放日内分钟数据
            for i in range(min_data_time.__len__()):
                if inday_high is None or min_data_high.iloc[i] > inday_high:
                    inday_high = min_data_high.iloc[i]
                if inday_low is None or min_data_low.iloc[i] < inday_low:
                    inday_low = min_data_low.iloc[i]
                if 959 <= min_data_time[i] <= 1454:
                    if self.__op.avail_sell() > 0:
                        if min_data_close.iloc[i] * adj_factor < cut_loss_price or \
                                (min_data_close.iloc[i] * adj_factor > cut_profit_price and
                                 min_data_close.iloc[i] < 2/3 * inday_high + 1/3 * inday_low):
                            close_order = Order(code, date, min_data_time[i], min_data_close.iloc[i], self.__op.avail_sell(), 'sell')
                            self.__op.insert_order(close_order)
                    if self.__op.avail_sell() == 0 and open_signal and min_data_close.iloc[i] > 1/3 * inday_high + 2/3 * inday_low:
                        single_order_amt = 500000
                        max_amt = 2000000
                        avail_amt = max(max_amt - self.__op.position() * min_data_close.iloc[i], 0)
                        order_volume = round(min(single_order_amt, avail_amt) / min_data_close.iloc[i], -2)
                        if order_volume > 100:
                            open_order = Order(code, date, min_data_time[i], min_data_close.iloc[i], order_volume, 'buy')
                            self.__op.insert_order(open_order)
                            cut_loss_price = min(low_min, inday_low * adj_factor) * 0.98
                            cut_profit_price = 1/3 * high_max + 2/3 * low_min
        order_list, trade_list, daily_info = self.__op.get_single_result()
        return order_list, trade_list, daily_info

    def get_total_stock_list(self):
        return self.__total_stock_list


def main():
    t1 = dt.datetime.now()
    start_date = 20150101
    end_date = 20151231
    complete_stock_list = Dtk.get_complete_stock_list()
    df1 = Dtk.get_panel_daily_info(complete_stock_list, start_date, end_date, 'index_500')
    daily_stock_pool = {}
    trading_days = Dtk.get_trading_day(start_date, end_date)
    for date in trading_days:
        df2 = df1.loc[date]
        daily_stock_pool.update({date: df2[df2 == 1].index.tolist()})
    strategy = Strategy(start_date, end_date, daily_stock_pool, '000905.SH')
    total_stock_list = strategy.get_total_stock_list()
    result_list = []
    total_order_list = []
    total_trade_list = []
    total_daily_info = []
    pool = multiprocessing.Pool(processes=3)
    for i in range(total_stock_list.__len__()):
        result = pool.apply_async(strategy.single_stock_trading, (total_stock_list[i],))
        result_list.append(result)
    pool.close()
    pool.join()
    for result in result_list:
        total_order_list += result.get()[0]
        total_trade_list += result.get()[1]
        total_daily_info += result.get()[2]
    file_name = "result"
    analyze_and_export_result(total_order_list, total_trade_list, total_daily_info, trading_days, file_name)
    t2 = dt.datetime.now()
    print("it cost", t2 - t1)


if __name__ == "__main__":
    main()
