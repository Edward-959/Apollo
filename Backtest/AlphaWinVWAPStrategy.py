from Backtest.StrategyBase import StrategyBase
from Backtest.PositionManager import PositionManager
from Backtest.common import Order, Quote, KBar, Transaction, Trade, Direction, OrdStatus, OrdType, SecurityType
from typing import Dict
from pandas import DataFrame, read_csv
from copy import copy
import logging
from Backtest.StockPosition import  StockPosition
from datetime import datetime
from DataAPI.FactorLoader import load_factor
from DataAPI.DataToolkit import get_panel_daily_info, get_complete_stock_list
from Backtest.utils.const_defines import *
from platform import system

STOP_LOSS = -0.15
PARAM1 = 0.01
PARAM2 = 0.01
PARAM3 = 2


class AlphaTest(StrategyBase):
    def __init__(self):
        super().__init__("AlphaTest")
        self.__init_cash = 300000000
        self.__postion_mngt = PositionManager(self.__init_cash)
        self.__signals: DataFrame = None
        self.__newest_signal_line = 0
        self.__order_list = {}
        self.__single_stock_max_weight = 0.01  # 若没在set_config中设置对冲标的，就会是它
        self.__hedge_index = "000300.SH"  # 若没在set_config中设置对冲标的，就会是它
        self.__all_stock_list = None
        self.__daily_stock_pool: Dict[str, list] = None
        ###########################################################################
        #  因子值，以DataFrame形式存储
        #  日期， 股票1， ... , 股票n
        #  self.__stock_column_dict 为因子中对应列的序列号，以便快速查询
        ##########################################################################
        self.__F_M_IntraOrderSignalVWAPBuy: DataFrame = None
        self.__F_M_IntraOrderSignalVWAPSell: DataFrame = None
        self.__stock_column_dict = {}
        ##########################################################################
        #  所有日期的权重因子以DataFrame放在内存中，以便快速查询
        #  self.__current_adj 存储了当日的权重因子， 改变量每日更新一次
        #  在on_new_day()中被更新
        ###################################################################
        self.__pre_close: Dict[float] = {}
        self.__adj_factor_df: DataFrame = None
        self.__current_adj: Dict[float] = {}
        #################################################################################
        # 临时变量
        #######################################################
        self.__time = 0
        self.__date = 0
        self.__pre_bar = {}
        self.__factor_row = 0
        self.__hedge_threshold = 1000000  # 暴露超过多少开始对冲
        self.__current_stock_pool = []

    def set_config(self, start_time: datetime= ..., hedge_index: str = ...,
                   end_time: datetime = ..., trade_stock_list: list = ..., daily_stock_pool=Dict[str, list],
                   single_stock_max_weight: float = ..., initial_position: str = None):
        self.__daily_stock_pool = daily_stock_pool
        self.__all_stock_list = trade_stock_list
        self.__hedge_index = hedge_index
        self.__single_stock_max_weight = single_stock_max_weight
        ################################################
        # 读取因子文件
        #################################################
        print('Loading Factor')
        self.__F_M_IntraOrderSignalVWAPBuy = load_factor("F_M_IntraOrderSignalVWAPBuy0.02to-0.04", self.__all_stock_list, start_time, end_time)
        self.__F_M_IntraOrderSignalVWAPSell = load_factor("F_M_IntraOrderSignalVWAPSell0.02to-0.04", self.__all_stock_list, start_time, end_time)
        print('finish loading Factor')

        self.__F_M_IntraOrderSignalVWAPBuy = self.__F_M_IntraOrderSignalVWAPBuy.reset_index().values
        self.__F_M_IntraOrderSignalVWAPSell = self.__F_M_IntraOrderSignalVWAPSell.reset_index().values

        for index, stock_code in enumerate(self.__all_stock_list):
            self.__stock_column_dict.update({stock_code: index})
        ################################################################
        # 初始化持仓
        #################################################################
        # if initial_position is not None:
        #     postion_df = read_csv(initial_position)
        #     for _, row in postion_df.iterrows():
        #         pos_new = StockPosition(row["symbol"])
        #         pos_new.sec_type = row["sec_type"]
        #         pos_new.long_position = row["long_position"]
        #         pos_new.short_position = row["short_position"]
        #         pos_new.position = pos_new.long_position - pos_new.short_position
        #         pos_new.cur_price = row["price"]

    def on_new_day(self, day: int=...):
        logging.info("{} -> {}".format(self.__date, day))
        self.__date = int(day)
        position_dict = self.__postion_mngt.get_position()
        for code in position_dict.keys():
            logging.info(position_dict.get(code))
        self.__postion_mngt.on_new_day(day)
        self.__order_list = {}
        ################################################################
        # 如果当日的股票池有变化，则更新，否则用上一个交易日的股票池
        if str(self.__date) in self.__daily_stock_pool.keys():
            self.__current_stock_pool = self.__daily_stock_pool.get(str(self.__date))
        else:
            self.__current_stock_pool = []
        #################################################################
        # for code in self.__all_stock_list:
        #     self.__current_adj.update({code: self.__adj_factor_df.at[day, code]})

    def on_order_updated(self, order: Order=...):
        #  logging.info(str(order))
        self.__postion_mngt.on_order_update(order=order)
        if order.status == OrdStatus.PARTIALLY_FILLED or order.status == OrdStatus.NEW:
            self.__order_list.update({order.order_id: copy(order)})
        else:
            if order.order_id in self.__order_list.keys():
                self.__order_list.pop(order.order_id)

    def get_online_order(self, symbol: str):
        """
        获取股票的在挂委托， 如果没有则返回None
        :param symbol:股票代码
        :return: Order
        """
        for order in self.__order_list.values():
            if order.symbol == symbol:
                return order
        return None

    def on_quote_updated(self, quote: Quote=...):
        pass

    def __process_risk_check(self, symbol):
        """
        在每个K线都检查股票是否到达了止损线，并执行止损操作
        :param symbol: 股票代码
        :return: None
        """
        pos: StockPosition = self.__postion_mngt.get_position().get(symbol)
        if pos is not None and pos.return_rate < STOP_LOSS and pos.sec_type != SecurityType.FUT:
            exist_order: Order = self.get_online_order(symbol)
            if exist_order is None:
                logging.info("止损: {}".format(symbol))
                self.insert_order(Order(symbol, 0, pos.available_sell, Direction.SELL, OrdType.TWAP))
            elif exist_order.direction == Direction.BUY:
                self.cancel_order(exist_order.order_id)

    def __process_signal(self, bar: KBar):
        """
        """
        if bar.time < 930 or bar.symbol == self.__hedge_index or bar.symbol not in self.__all_stock_list or self.__current_stock_pool == []:
            return

        ##################################################################
        timestamp = datetime.strptime("{}{}".format(bar.date, str(bar.time).zfill(4)), '%Y%m%d%H%M')
        # adj_factor = self.__current_adj.get(bar.symbol)
        timestamp1 = timestamp.timestamp()

        while timestamp1 > self.__F_M_IntraOrderSignalVWAPBuy[self.__factor_row, 0]:
            self.__factor_row += 1

        bar_col = self.__stock_column_dict[bar.symbol] + 1

        buy_signal = self.__F_M_IntraOrderSignalVWAPBuy[self.__factor_row, bar_col]
        sell_signal = self.__F_M_IntraOrderSignalVWAPSell[self.__factor_row, bar_col]
        # 平仓逻辑
        ################################################################
        not_in_current_stock_pool = bar.symbol not in self.__current_stock_pool
        # net_sell = self.__postion_mngt.get_stock_sell_amount() - self.__postion_mngt.get_stock_buy_amount()
        if sell_signal == 1 and not_in_current_stock_pool and self.__postion_mngt.get_stock_available_cash() <= self.__postion_mngt.get_market_value() * 0.05:
            # if bar.symbol == '002475.SZ':
            #     a = 1
            available_sell = self.__postion_mngt.get_available_sell(bar.symbol)
            # if available_sell > self.__postion_mngt.get_position().get(bar.symbol).position:
            #     a = 1
            if available_sell >= 100:
                order = Order(bar.symbol, bar.close, int(available_sell / 100) * 100, Direction.SELL, OrdType.FOC)
                self.insert_order(order=order)

        ##############################################################
        # 开仓逻辑
        ################################################################
        if buy_signal == 1 and not not_in_current_stock_pool:
            # if bar.symbol == '000725.SZ':
            #     a = 1
            ###################################################
            # 有开仓信号后计算该股票可以下的委托量
            #############################################
            stock_mv = self.__postion_mngt.get_market_value_by_symbol(bar.symbol)
            total_mv = self.__postion_mngt.get_market_value()
            allow_to_buy = total_mv * self.__single_stock_max_weight - stock_mv
            if self.__postion_mngt.available_cash >= allow_to_buy:
                allow_to_buy /= bar.close
                allow_to_buy = int(allow_to_buy / 100) * 100
            else:
                allow_to_buy = 0
            # if allow_to_buy == 100:
            #     a = 1
            if allow_to_buy > 0:
                order = Order(bar.symbol, bar.close, allow_to_buy, Direction.BUY, OrdType.FOC)
                self.insert_order(order=order)

    def on_bar_update(self, bar: KBar=...):
        self.__postion_mngt.on_bar(bar)
        # self.__process_risk_check(bar.symbol)
        self.__process_signal(bar)
        self.__pre_bar.update({bar.symbol: bar})
        self.__on_hedge(bar)
        self.__time = bar.time

    def on_finished(self):
        self.__postion_mngt.balance()
        logging.debug("Final Position:")
        for pos in self.__postion_mngt.get_position().values():
            logging.debug(str(pos))
        #  self.__show_pnl_line()

    def __show_pnl_line(self):
        import plotly
        import plotly.graph_objs as go
        pnl_data = self.__postion_mngt.get_pnl_data()
        x = ["D{}".format(_x[0]) for _x in pnl_data]
        y = [(100 * (_x[1] - self.__postion_mngt.total_cash) / self.__init_cash) for _x in pnl_data]
        plotly.offline.plot({
            "data": [go.Scatter(x=x, y=y, line=dict(color='rgb(205, 12, 24)', width=4))],
            "layout": go.Layout(title="收益率曲线(初始资金{})".format(self.__init_cash), xaxis=dict(title='日期'),
                                yaxis=dict(title='收益率%'))},
            filename="pnl_chart.html", auto_open=True)
        logging.info("Generated PNL Chart file: {}".format("pnl_chart.html"))

    def on_transaction_updated(self, trade: Transaction=...):
        pass

    def on_trade_updated(self, trade: Trade=...):
        self.__postion_mngt.on_trade(trade)
        #  logging.debug(str(trade))
        pass

    def suspend(self):
        pass

    def stop(self):
        pass

    def start(self):
        pass

    def __on_hedge(self, bar: KBar):
        if bar.symbol != self.__hedge_index:
            return
        half_hour = bar.time % 100
        if half_hour != 0 and half_hour != 30:
            return
        portfolio_value = self.__postion_mngt.get_exposure()
        if abs(portfolio_value) < self.__hedge_threshold:
            # 没有达到对冲敞口阈值， 无需对冲
            return
        one_index_value = bar.close * Const.CONTRACT_MULTIPLIER.get(bar.symbol)
        contracts = round(portfolio_value / one_index_value, 3)
        if contracts > 0:
            order = Order(self.__hedge_index, bar.high, contracts, Direction.OPEN_SHORT, OrdType.TWAP, SecurityType.FUT)
            self.insert_order(order)
            logging.info("hedge: sell IF {} contracts".format(contracts))
        elif contracts < 0:
            order = Order(self.__hedge_index, bar.high, -contracts, Direction.CLOSE_SHORT, OrdType.TWAP, SecurityType.FUT)
            self.insert_order(order)
            logging.info("hedge: buys IF {} contracts".format(-contracts))


def main():
    # 如设为logging.DEBUG，则会输出信号、成交和每日pnl等信息；如设为logging.INFO，则不打印信号和成交的信息
    app_start_time = datetime.now()
    logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.DEBUG)
    from Backtest.MarketReplayer import MDReplay
    from Backtest.TradeMaker import TradeMaker
    import json
    start = "20160101 09:30:00"
    end = "20171231 15:00:00"
    date_start = datetime.strptime(start, '%Y%m%d %H:%M:%S')
    date_end = datetime.strptime(end, '%Y%m%d %H:%M:%S')
    hedgeindex = "000905.SH"
    dealer = TradeMaker(int(start[0:8]), int(end[0:8]), hedgeindex)
    Test = AlphaTest()
    #stock_code_list = ["000858.SZ"]
    # stock_code_list = ["600000.SH", "601318.SH", "600030.SH", "601688.SH", "000001.SZ", "000002.SZ", "000858.SZ"]
    single_stock_max_weight = 0.0067
    # import csv
    # import pickle
    # file_name = 'D:\\Apollo\\Backtest\\alphapositon2016-2017.csv'
    # with open(file_name) as f:
    #     reader = csv.reader(f)
    #     daily_stock_pool = {}
    #     row1 = []
    #     import numpy as np
    #     for row in reader:
    #         if reader.line_num == 1:
    #             row1 = row
    #         else:
    #             temp_date = row[0]
    #             temp_date = datetime.strptime(temp_date, '%Y/%m/%d')
    #             temp_row = np.array(row)
    #             row_code = np.array(row1)[np.argwhere(temp_row != '').flatten()]
    #             daily_stock_pool.update({temp_date.strftime('%Y%m%d'): row_code[1:].tolist()})
    # with open('daily_stock_pool_jingong500.pickle', 'wb') as f:
    #     pickle.dump(daily_stock_pool, f)

    from xquant.pyfile import Pyfile
    import pickle
    py = Pyfile()
    with py.open('daily_stock_pool_jingong500' + '.pickle') as f:
        print('Use Xquant open')
        dailyStockPool = pickle.load(f)

    # dsp_fp = open("DailyStockPool.json")
    # daily_stock_pool = json.load(dsp_fp)
    # dsp_fp.close()
    # all_stock = set([])
    # for day_info in daily_stock_pool.values():
    #     all_stock = all_stock.union(set(day_info))
    # all_stock = list(all_stock)
    # complete_stock_list = get_complete_stock_list()
    # df3 = get_panel_daily_info(complete_stock_list, 20171229, 20180110, 'index_300')
    # df3_20180102 = df3.loc[20180102]
    # df3_20180102 = list(df3_20180102[df3_20180102 == 1].index)
    # all_stock = df3_20180102
    # daily_stock_pool = {"20170102": all_stock, "20170103": all_stock}
    # complete_stock_list = get_complete_stock_list()
    # stock_weight = get_panel_daily_info(complete_stock_list, int(start[0:8]), int(end[0:8]), 'index_weight_' + 'hs300')
    # dailyStockPool = {}
    # for i_date in stock_weight.index:
    #     dailyStockPool.update({str(i_date): stock_weight.columns[stock_weight.loc[i_date, :] > 0]})
    all_stock = set([])
    for day_info in dailyStockPool.values():
        all_stock = all_stock.union(set(day_info))
    all_stock = list(all_stock)
    Test.set_config(trade_stock_list=all_stock,
                    daily_stock_pool=dailyStockPool,
                    hedge_index=hedgeindex, start_time=date_start, end_time=date_end,
                    single_stock_max_weight=single_stock_max_weight)

    # replay_list = copy(all_stock)
    # replay_list.append(hedgeindex)
    replayer = MDReplay()
    replayer.set_config(stock_list="merged_market_data_all", start_time=date_start,
                        end_time=date_end, data_type=4)
    if system() == "Windows":
        replayer.data_root = "S:\Apollo\merged_MarketData"
    else:
        replayer.data_root = "/app/data/006566/log"

    ######################################################
    # hookup callback with order maker
    ########################################################
    dealer.sub_order_update(Test.on_order_updated)
    dealer.sub_trade_update(Test.on_trade_updated)

    #############################################
    #  hookup callbacks with market data replayer
    ##############################################
    replayer.subscribe_callback_bar(dealer.on_bar)
    replayer.subscribe_callback_bar(Test.on_bar_update)
    replayer.subscribe_callback_new_day(dealer.on_new_day)
    replayer.subscribe_callback_new_day(Test.on_new_day)
    replayer.subscribe_callback_finished(Test.on_finished)

    #############################################
    # set place order interfaces for strategy
    #############################################
    Test.set_interface(dealer.insert_order, dealer.cancel_order)
    replayer.async_run()
    logging.info("Test Spent time: {}".format(datetime.now() - app_start_time))


if __name__ == "__main__":
    main()
