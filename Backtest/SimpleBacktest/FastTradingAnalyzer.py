"""
快速进行策略的回测
以单股票为粒度，进行并行计算
对每支股票按分钟顺序播放行情，同时可以计算相关指标，同时记录交易
全部完成后，对交易记录进行分析，统计胜率、收益率、盈亏比等基本指标，方便对策略进行初步判断和快速调参
"""

import DataAPI.DataToolkit as Dtk
from pandas import DataFrame, Series
import multiprocessing


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
        self.__order_list = []
        self.__lag = 20
        self.__cost = 0.0012
        self.__prepare_daily_data()

    # 准备日度数据，进行日度因子的计算
    def __prepare_daily_data(self):
        start_date_minus_lag_2 = Dtk.get_n_days_off(self.__start_date, -(self.__lag + 2))[0]
        self.__adj_factor_df = Dtk.get_panel_daily_info(self.__total_stock_list, start_date_minus_lag_2,
                                                        self.__end_date, info_type="adjfactor")
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
        trade_list = []
        position = -1  # -1表示无仓位，0表示有仓位但当日不可卖，大于0的数表示持仓天数
        cut_loss_price = None
        cut_profit_price = None
        for date in self.__trading_days:
            # 每日盘前数据处理
            if code not in self.__daily_stock_pool[date] and position == -1:
                continue
            high_max = self.__stock_daily_high_max.at[date, code]
            low_min = self.__stock_daily_low_min.at[date, code]
            high_idxmax = self.__stock_daily_high_idxmax.at[date, code]
            low_idxmin = self.__stock_daily_low_idxmin.at[date, code]
            open_signal = low_idxmin - high_idxmax > 10 and low_idxmin > 15 and low_min / high_max - 1 < -0.2
            if not open_signal and position == -1:
                continue
            # 昨日仓位今日可卖
            if position >= 0:
                position += 1
            min_data_close = stock_min_data.loc[date, 'close']
            min_data_high = stock_min_data.loc[date, 'high']
            min_data_low = stock_min_data.loc[date, 'low']
            min_data_time = stock_min_data.loc[date].index.tolist()
            adj_factor = self.__adj_factor_df.at[date, code]
            hedge_index_close = self.__hedge_index_min_data.loc[date, 'close']
            inday_high = None
            inday_low = None
            # 播放日内分钟数据
            for i in range(min_data_time.__len__()):
                if inday_high is None or min_data_high.iloc[i] > inday_high:
                    inday_high = min_data_high.iloc[i]
                if inday_low is None or min_data_low.iloc[i] < inday_low:
                    inday_low = min_data_low.iloc[i]
                if 959 <= min_data_time[i] <= 1454:
                    if position >= 1:
                        if min_data_close.iloc[i] * adj_factor < cut_loss_price or \
                                (min_data_close.iloc[i] * adj_factor > cut_profit_price and
                                 min_data_close.iloc[i] < 2/3 * inday_high + 1/3 * inday_low):
                            close_order = [code, date, min_data_time[i], min_data_close.iloc[i],
                                           min_data_close.iloc[i] * adj_factor, hedge_index_close.loc[min_data_time[i]], 'S']
                            self.__order_list.append(close_order)
                            ret = close_order[4] / open_order[4] - 1 - self.__cost
                            ret_relative = close_order[4] / open_order[4] - close_order[5] / open_order[5] - self.__cost
                            trade_record = open_order[0:5] + close_order[1:5]
                            trade_record.append(position)
                            trade_record.append(ret)
                            trade_record.append(ret_relative)
                            trade_list.append(trade_record)
                            position = -1
                    if position == -1 and open_signal and min_data_close.iloc[i] > 1/3 * inday_high + 2/3 * inday_low:
                        position = 0
                        open_order = [code, date, min_data_time[i], min_data_close.iloc[i],
                                      min_data_close.iloc[i] * adj_factor, hedge_index_close.loc[min_data_time[i]], 'B']
                        self.__order_list.append(open_order)
                        cut_loss_price = min(low_min, inday_low * adj_factor) * 0.98
                        cut_profit_price = 1/3 * high_max + 2/3 * low_min
                        break
        return trade_list

    def get_total_stock_list(self):
        return self.__total_stock_list

    @staticmethod
    # 对交易记录进行数据统计
    def statistics(total_trade_list):
        analysis = {}
        analysis.update({'trade_num': [total_trade_list.__len__(), total_trade_list.__len__()]})
        analysis.update({'win_num': [sum(total_trade_list['ret'] > 0), sum(total_trade_list['ret_relative'] > 0)]})
        analysis.update({'lose_num': [sum(total_trade_list['ret'] < 0), sum(total_trade_list['ret_relative'] < 0)]})
        analysis.update({'win_rate': [analysis['win_num'][0] / analysis['trade_num'][0],
                                      analysis['win_num'][1] / analysis['trade_num'][1]]})
        analysis.update({'ave_ret': [total_trade_list['ret'].mean(), total_trade_list['ret_relative'].mean()]})
        analysis.update({'win_ret': [total_trade_list['ret'][total_trade_list['ret'] > 0].mean(),
                                     total_trade_list['ret_relative'][total_trade_list['ret_relative'] > 0].mean()]})
        analysis.update({'lose_ret': [total_trade_list['ret'][total_trade_list['ret'] < 0].mean(),
                                      total_trade_list['ret_relative'][total_trade_list['ret_relative'] < 0].mean()]})
        analysis.update({'win_lose_ratio': [analysis['win_ret'][0] / abs(analysis['lose_ret'][0]),
                                            analysis['win_ret'][1] / abs(analysis['lose_ret'][1])]})
        analysis.update({'ave_holding_days': [total_trade_list['holding_days'].mean(), total_trade_list['holding_days'].mean()]})
        trading_analysis = DataFrame.from_dict(analysis, orient='index').T
        trading_analysis.to_excel("d:\\006688\\Desktop\\Appollo\\Backtest\\log\\trading_analysis.xls")
        # trading_analysis.to_excel("/app/data/006688/Apollo/log/trading_analysis.xls")
        print(trading_analysis)


def main():
    start_date = 20150101
    end_date = 20150130
    complete_stock_list = Dtk.get_complete_stock_list()
    df1 = Dtk.get_panel_daily_info(complete_stock_list, start_date, end_date, 'index_500')
    daily_stock_pool = {}
    trading_days = Dtk.get_trading_day(start_date, end_date)
    for date in trading_days:
        df2 = df1.loc[date]
        daily_stock_pool.update({date: df2[df2 == 1].index.tolist()})
    strategy = Strategy(start_date, end_date, daily_stock_pool, '000905.SH')
    total_stock_list = strategy.get_total_stock_list()
    multi_trade_list = []
    total_trade_list = []
    pool = multiprocessing.Pool(processes=3)
    for i in range(total_stock_list.__len__()):
        trade_list = pool.apply_async(strategy.single_stock_trading, (total_stock_list[i],))
        multi_trade_list.append(trade_list)
    pool.close()
    pool.join()
    for trade_list in multi_trade_list:
        total_trade_list += trade_list.get()
    total_trade_list = DataFrame(total_trade_list, columns=['code', 'open_date', 'open_time', 'open_price',
                                                            'open_price_adj', 'close_date', 'close_time',
                                                            'close_price', 'close_price_adj', 'holding_days', 'ret',
                                                            'ret_relative'])
    total_trade_list.to_excel("d:\\006688\\Desktop\\Appollo\\Backtest\\log\\trade_list.xls")
    # total_trade_list.to_excel("/app/data/006688/Apollo/log/trade_list.xls")
    strategy.statistics(total_trade_list)


if __name__ == "__main__":
    main()
