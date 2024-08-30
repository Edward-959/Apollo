from .StrategyBase import StrategyBase
from .PositionManager import PositionManager
from .common import Order, Quote, KBar, Transaction, Trade, Direction, OrdStatus, OrdType, SecurityType
from typing import Dict
from pandas import DataFrame
from copy import copy
import logging
import pandas as pd
from .StockPosition import  StockPosition
from datetime import datetime


STOP_LOSS = -0.15


class Athena(StrategyBase):
    def __init__(self):
        super().__init__("Prometheus")
        self.__init_cash = 50000000
        self.__postion_mngt = PositionManager(self.__init_cash * 1.2)
        self.__signals: DataFrame = None
        self.__max_holding_period = 5
        self.__max_stock_num = 5
        self.__newest_signal_line = 0
        self.__order_list = {}
        self.__single_stock_weight = 1 / self.__max_stock_num
        self.__time = 0
        self.__date = 0
        self.__hedge_index = "000300.SH"
        self.__store: pd.HDFStore = None
        self.__pre_bar = {}

    def on_new_day(self, day: int=...):
        logging.info("{} -> {}".format(self.__date, day))
        self.__date = int(day)
        self.__signals = self.__store.select("/data/D{}S".format(self.__date))
        self.__signals = self.__signals.groupby(by="Timestamp")
        self.__postion_mngt.on_new_day(day)
        self.__order_list = {}
        ##########################
        # 检查是否有持仓时间过长的股票，如果有且今天没有发出买入信号，则卖出，
        # 若有信号则继续买入，持仓时间重置
        ###############################
        keys = self.__postion_mngt.get_position().keys()
        for key in keys:
            pos = self.__postion_mngt.get_position().get(key)
            if pos.sec_type != SecurityType.FUT and pos.holding_days > self.__max_holding_period:
                logging.info("持仓时间过长，平仓：{} {} {}".format(pos.symbol, pos.position, pos.pre_close))
                self.insert_order(Order(pos.symbol, pos.pre_close, pos.position, Direction.SELL, OrdType.TWAP))

    def on_order_updated(self, order: Order=...):
        logging.info(str(order))
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
        根据预测值中PredictHigh最大的3个进行尝试开仓动作，如果还有坑位或者该股票仓位未满则可以开仓
        如果持仓中的股票在PredictLow最小的5个中，平仓
        :param bar: 所有股票的某一个一分钟k线播放完后，处理信号
        :return:
        """
        if self.__time != int(bar.time) and bar.time >= 930:
            # new time slice
            time_str = "{} {}".format(int(self.__date), bar.time)
            group_key = datetime.strptime(time_str, '%Y%m%d %H%M').timestamp()
            cur_signl: pd.DataFrame = self.__signals.get_group(group_key)
            buy_signals = cur_signl.nlargest(20, "PredictHigh")
            sell_signals = cur_signl.nsmallest(50, "PredictLow")
            current_position = self.__postion_mngt.get_position()
            order_stock_list = [order.symbol for order in self.__order_list.values()]
            order_stock_list.extend(list(current_position.keys()))
            if len(order_stock_list) > 0:
                order_stock_list = set(order_stock_list)
            stocks_available_buy = self.__max_stock_num - order_stock_list.__len__()
            if stocks_available_buy <= 0:
                logging.debug("Reach Max Stock Number, Order Rejected")
                return
            for _, row in buy_signals.iterrows():
                if stocks_available_buy <= 0:
                    break
                pre_order = self.get_online_order(row["Code"])
                if pre_order is not None or self.__pre_bar.get(row["Code"]) is None:
                    continue
                volume = int(self.__init_cash * self.__single_stock_weight / self.__pre_bar.get(row["Code"]).close / 100)
                volume *= 100
                order = Order(row["Code"], 0, volume, Direction.BUY, OrdType.TWAP)
                if row["Code"] not in current_position.keys():
                    self.insert_order(order)
                    stocks_available_buy -= 1
                else:
                    order.order_qty = volume - current_position.get(row["Code"]).position
                    if order.order_qty > 0:
                        self.insert_order(order)

            for _, row in sell_signals.iterrows():
                code = row["Code"]
                pre_order = self.get_online_order(row["Code"])
                if pre_order is not None:
                    continue
                if code in current_position.keys():
                    pos: StockPosition = current_position.get(code)
                    if pos.available_sell > 0:
                        order = Order(code, 0, pos.available_sell, Direction.BUY, OrdType.TWAP)
                        self.insert_order(order)
            pass

        ###
        # get current signals which is stock list with prediction
        # 预先算好top10 和 平均值， 如果平均值以下并且有仓位的需要平仓
        pass

    def on_bar_update(self, bar: KBar=...):
        self.__postion_mngt.on_bar(bar)
        self.__process_risk_check(bar.symbol)
        self.__process_signal(bar)
        self.__pre_bar.update({bar.symbol: bar})
        self.__on_hedge(bar)
        self.__time = bar.time

    def on_finished(self):
        self.__postion_mngt.balance()
        logging.debug("Final Position:")
        for pos in self.__postion_mngt.get_position().values():
            logging.debug(str(pos))
        self.__show_pnl_line()
        self.__store.close()

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
        pass

    def suspend(self):
        pass

    def stop(self):
        pass

    def start(self):
        pass

    def set_config(self, para: Dict=...):
        if "signal" in para.keys():
            signal_file = para.get("signal")
            self.__store: pd.HDFStore = pd.HDFStore(signal_file)
        if "hedge_index" in para.keys():
            self.__hedge_index = para.get("hedge_index")

    def __on_hedge(self, bar: KBar):
        if bar.time != 1430 or bar.time == self.__time:
            return
        portfolio_value = 0
        position_dict = self.__postion_mngt.get_position()
        for pos in position_dict.values():
            if pos.sec_type == SecurityType.FUT:
                portfolio_value += pos.position * pos.cur_price * 300
            else:
                portfolio_value += pos.position * pos.cur_price
        index_bar = self.__pre_bar.get(self.__hedge_index)
        if index_bar is None:
            raise Exception("No {} Market Data".format(self.__hedge_index))
        one_index_value = index_bar.close * 300
        contracts = portfolio_value // one_index_value
        if contracts > 0:
            order = Order(self.__hedge_index, 0, contracts, Direction.SELL, OrdType.TWAP, SecurityType.FUT)
            self.insert_order(order)
            logging.info("hedge: sell IF {} contracts".format(contracts))
        elif contracts < 0:
            order = Order(self.__hedge_index, 0, -contracts, Direction.BUY, OrdType.TWAP, SecurityType.FUT)
            self.insert_order(order)
            logging.info("hedge: buys IF {} contracts".format(contracts))

