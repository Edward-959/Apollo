"""
订单处理模块，主要功能为：
1、进行买卖订单的撮合返回成交结果，记录每笔委托，并输出到OrderRecord
2、匹配对应买卖订单，计算该笔交易的盈亏，并输出到TradeRecord
3、生成每日盈亏表DailyInfo
"""
from Backtest.PortfolioDivCache import PortfolioDivCache
import pandas as pd
import platform
from os import mkdir, path, environ


class Order:
    def __init__(self, code, date, time, price, volume, direction, deal=None):
        self.code = code
        self.date = date
        self.time = time
        self.price = price
        self.volume = volume
        self.direction = direction
        self.deal = deal


class Position:
    def __init__(self, code):
        self.code = code
        self.open_date = None
        self.open_amt = 0
        self.close_date = None
        self.close_amt = 0
        self.position = 0
        self.cash = 0
        self.holding_days = 0
        self.acc_pnl = 0
        self.avail_sell = 0


class OrderProcessor:
    def __init__(self):
        self.__max_ratio = 0.5
        self.__cost = 0.0012
        self.__portAdjCache = PortfolioDivCache()
        self.__minute_data = None
        self.__match_data = None
        self.__order_record = []
        self.__trade_record = []
        self.__daily_info = []
        self.__position = None
        self.__daily_close = None

    # 开始新一支股票回测前，初始化相关变量
    def initiate_data(self, code, stock_min_data, daily_close):
        self.__match_data = stock_min_data.shift(-1)
        self.__position = Position(code)
        self.__daily_close = daily_close
        self.__order_record = []
        self.__trade_record = []
        self.__daily_info = []

    # 对订单进行撮合，并将委托和成交结果存入OrderRecord
    def insert_order(self, order):
        volume_matching = 0
        if order.time == 1500:
            order.deal = 0
            self.__order_record.append(order)
            return
        else:
            k_line_matching = self.__match_data.loc[(order.date, order.time), :]
        if k_line_matching['volume'] == 0:
            volume_matching = 0
        else:
            if order.direction == 'buy':
                if order.price < k_line_matching['low']:
                    fill_ratio = 0
                elif order.price > k_line_matching['high']:
                    fill_ratio = self.__max_ratio
                else:
                    if k_line_matching['high'] - k_line_matching['low'] != 0:
                        fill_ratio = min((order.price - k_line_matching['low']) / (
                                k_line_matching['high'] - k_line_matching['low']), self.__max_ratio)
                    else:
                        fill_ratio = 0
                volume_matching = k_line_matching['volume'] * fill_ratio
            elif order.direction == 'sell':
                if order.price > k_line_matching['high']:
                    fill_ratio = 0
                elif order.price < k_line_matching['low']:
                    fill_ratio = self.__max_ratio
                else:
                    if k_line_matching['high'] - k_line_matching['low'] != 0:
                        fill_ratio = min((k_line_matching['high'] - order.price) / (
                                k_line_matching['high'] - k_line_matching['low']), self.__max_ratio)
                    else:
                        fill_ratio = 0
                volume_matching = k_line_matching['volume'] * fill_ratio
        order.deal = min(order.volume, round(volume_matching, -2))
        self.__order_record.append(order)
        # 根据成交结果，更新仓位信息或者记录交易信息和每日信息
        if order.direction == 'buy' and order.deal > 0:
            if self.__position.position == 0:
                self.__position.open_date = order.date
            self.__position.open_amt += order.deal * order.price
            self.__position.position += order.deal
        elif order.direction == 'sell' and order.deal > 0:
            self.__position.close_amt += order.deal * order.price
            self.__position.position -= order.deal
            self.__position.avail_sell -= order.deal
            if self.__position.position == 0:
                self.__position.close_date = order.date
                trade_pnl = self.__position.close_amt * (1-self.__cost) + self.__position.cash - self.__position.open_amt
                trade_ret = trade_pnl / self.__position.open_amt
                self.__trade_record.append([self.__position.code, self.__position.open_date, self.__position.close_date,
                                            self.__position.holding_days, self.__position.open_amt, trade_pnl, trade_ret])
                self.__daily_info.append([self.__position.code, order.date, trade_pnl-self.__position.acc_pnl, 0])
                self.__reset_position()

    # 用于一笔交易完成后，重置仓位信息
    def __reset_position(self):
        self.__position.open_date = None
        self.__position.open_amt = 0
        self.__position.close_date = None
        self.__position.close_amt = 0
        self.__position.position = 0
        self.__position.cash = 0
        self.__position.holding_days = 0
        self.__position.acc_pnl = 0
        self.__position.avail_sell = 0

    # 主要用于处理分红除权信息，在日期更新后即调用
    # 这里的处理方式与完整框架有所区别，在除权日交易开始前已经进行了除权
    def on_new_day(self, code, yesterday, today):
        if self.__position.position == 0:
            return
        self.__position.holding_days += 1
        holding_amt = self.__position.position * self.__daily_close.loc[yesterday]
        acc_pnl = self.__position.close_amt * (1-self.__cost) + holding_amt + self.__position.cash - self.__position.open_amt
        self.__daily_info.append([code, yesterday, acc_pnl - self.__position.acc_pnl, holding_amt])
        self.__position.acc_pnl = acc_pnl
        dividend_info = self.__portAdjCache.get_query_day_div_info(code, today)
        # odd_share是除以10后的零股
        position_div_by_10, odd_share = divmod(self.__position.position, 10)
        if dividend_info["per_cashpaidaftertax"] != 0:
            self.__position.cash += position_div_by_10 * dividend_info["per_cashpaidaftertax"] * 10
        if dividend_info["per_div_trans"] != 0:
            self.__position.position = int(position_div_by_10 * (1 + dividend_info["per_div_trans"]) * 10 + odd_share)
        self.__position.avail_sell = self.__position.position

    def avail_sell(self):
        return self.__position.avail_sell

    def position(self):
        return self.__position.position

    def get_single_result(self):
        order_record = []
        for order in self.__order_record:
            order_record.append([order.code, order.date, order.time, order.price, order.volume, order.direction, order.deal])
        trade_record = self.__trade_record
        daily_info = self.__daily_info
        return order_record, trade_record, daily_info


# 对输出结果进行分析以及输出
def analyze_and_export_result(order_list, trade_list, daily_info, trading_days, file_name):
    if platform.system() == "Windows":
        simulation_path = "log/"
        if not path.exists(simulation_path):
            mkdir(simulation_path)
    else:
        user_id = environ['USER_ID']
        simulation_path = "/app/data/" + user_id + "/StrategyResearch/"
        if not path.exists(simulation_path):
            mkdir(simulation_path)
    order_list_df = pd.DataFrame(order_list, columns=['code', 'date', 'time', 'price', 'volume', 'direction', 'deal'])
    trade_list_df = pd.DataFrame(trade_list, columns=['code', 'open_date', 'close_date', 'holding_days', 'open_amt', 'pnl', 'ret'])
    daily_info_df = pd.DataFrame(daily_info, columns=['code', 'date', 'pnl', 'holding_amt'])
    daily_pnl = daily_info_df.groupby(by='date')['holding_amt', 'pnl'].sum()
    daily_pnl = daily_pnl.reindex(trading_days)
    daily_pnl = daily_pnl.fillna(0)
    daily_pnl['acc_pnl'] = daily_pnl['pnl'].cumsum()
    trade_num = trade_list_df.__len__()
    win_num = trade_list_df[trade_list_df['ret'] > 0].__len__()
    lose_num = trade_num - win_num
    win_rate = win_num / trade_num
    ave_equal_ret = trade_list_df['ret'].mean()
    ave_weight_ret = trade_list_df['pnl'].sum() / trade_list_df['open_amt'].sum()
    win_ret = trade_list_df[trade_list_df['ret'] > 0]['ret'].mean()
    lose_ret = trade_list_df[trade_list_df['ret'] <= 0]['ret'].mean()
    win_lose_ratio = win_ret / abs(lose_ret)
    ave_holding_days = trade_list_df['holding_days'].mean()
    max_holding_amt = daily_pnl['holding_amt'].max()
    trade_stats_df = pd.DataFrame([trade_num, win_num, lose_num, win_rate, ave_equal_ret, ave_weight_ret, win_ret,
                                   lose_ret, win_lose_ratio, ave_holding_days, max_holding_amt],
                                  index=['trade_num', 'win_num', 'lose_num', 'win_rate', 'ave_equal_ret',
                                         'ave_weight_ret', 'win_ret', 'lose_ret', 'win_lose_ratio', 'ave_holding_days',
                                         'max_holding_amt'])
    out_file = pd.ExcelWriter(simulation_path + file_name + ".xlsx")
    order_list_df.to_excel(out_file, "order")
    trade_list_df.to_excel(out_file, "trade")
    daily_pnl.to_excel(out_file, "daily_pnl")
    trade_stats_df.to_excel(out_file, "trade_stats")
    out_file.save()
