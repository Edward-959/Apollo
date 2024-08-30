from .StrategyBase import StrategyBase
from .PositionManager import PositionManager
from .common import Order, Quote, KBar, Transaction, Trade, Direction, OrdStatus, OrdType
from typing import Dict
from pandas import DataFrame
from copy import copy
import logging


class Prometheus(StrategyBase):
    def __init__(self):
        super().__init__("Prometheus")
        self.__init_cash = 50000000
        self.__postion_mngt = PositionManager(self.__init_cash * 1.2)
        self.__signals: DataFrame = None
        self.__max_holding_period = 300
        self.__max_stock_num = 50
        self.__newest_signal_line = 0
        self.__order_list = {}
        self.__single_stock_weight = 1 / self.__max_stock_num

    def on_new_day(self, day: int=...):
        self.__postion_mngt.on_new_day(day)
        self.__order_list = {}
        ##########################
        # 检查是否有持仓时间过长的股票，如果有且今天没有发出买入信号，则卖出，
        # 若有信号则继续买入，持仓时间重置
        ###############################
        keys = self.__postion_mngt.get_position().keys()
        for key in keys:
            pos = self.__postion_mngt.get_position().get(key)
            if pos.holding_days > self.__max_holding_period:
                logging.info("持仓时间过长，平仓：{} {} {}".format(pos.symbol, pos.position, pos.pre_close))
                self.insert_order(Order(pos.symbol, pos.pre_close, pos.position, Direction.SELL))

        logging.info(day)
        pass

    def on_order_updated(self, order: Order=...):
        logging.debug(str(order))
        if order.status == OrdStatus.PARTIALLY_FILLED or order.status == OrdStatus.NEW:
            self.__order_list.update({order.order_id: copy(order)})
        else:
            if order.order_id in self.__order_list.keys():
                self.__order_list.pop(order.order_id)

    def on_quote_updated(self, quote: Quote=...):
        pass

    def on_bar_update(self, bar: KBar=...):
        self.__postion_mngt.on_bar(bar)
        current_positoin = self.__postion_mngt.get_position()
        key_code_datetime = bar.symbol + str(int(bar.date)) + str(int(bar.time))
        # 如果在指定日期、指定时间、指定股票有signal, 那么可以下单了
        # 但和我们要求的还有一些差距
        # 初始化：初始资金、每只股票的权重（可换算得股票数量上限）、起始日期、终止日期、止盈阈值、止损阈值、自动平仓周期
        # 输入：信号，包括股票代码、委托方向、委托价格、委托时间；如一次信号中包含多只股票，信号需按强度排序
        # —— 根据输入的信号，结合已有持仓和自动平仓周期，进行输出
        # 输出：委托，包括股票代码、委托方向、委托时间、委托数量、委托方式（两种，限价或vwap）
        # 初版前以单利的形式进行投资，即每次委托的金额都一致
        if key_code_datetime in self.__signals.index:
            logging.debug("SIGNAL: {} {} {} {} {}".format(bar.time, bar.symbol,
                                                             self.__signals.loc[key_code_datetime]['price'],
                                                             self.__signals.loc[key_code_datetime]['volume'],
                                                             self.__signals.loc[key_code_datetime]['bs_flag']))
            if 'B' in self.__signals.loc[key_code_datetime]['bs_flag']:
                order_stock_list = [order.symbol for order in self.__order_list.values()]
                order_stock_list.extend(list(current_positoin.keys()))
                if len(order_stock_list) > 0:
                    order_stock_list = set(order_stock_list)
                if order_stock_list.__len__() == self.__max_stock_num:
                    logging.info("Reach Max Stock Number, Order Rejected")
                    return
                order = Order(bar.symbol, self.__signals.loc[key_code_datetime]['price'],
                              self.__signals.loc[key_code_datetime]['volume'], Direction.BUY)
                ###############################################
                # 获取个股最大持仓 和 已有持仓， 计算出可买数量
                ###############################################
                if 'T' in self.__signals.loc[key_code_datetime]['bs_flag']:
                    order.order_type = OrdType.TWAP
                    volume = self.__init_cash * self.__single_stock_weight / (bar.amount / bar.volume)
                elif 'C' in self.__signals.loc[key_code_datetime]['bs_flag']:
                    order.order_type = OrdType.FOC
                    volume = self.__init_cash * self.__single_stock_weight / order.price
                else:
                    volume = self.__init_cash * self.__single_stock_weight / order.price
                if order.symbol in current_positoin.keys():
                    volume -= current_positoin.get(order.symbol).position
                order_qty_div_by_100, _ = divmod(min(order.order_qty, volume), 100)
                order.order_qty = order_qty_div_by_100 * 100
                if order.order_qty > 0:
                    self.insert_order(order)
            else:
                available_sell = self.__postion_mngt.get_available_sell(bar.symbol)
                if available_sell > 0:
                    order = Order(bar.symbol, self.__signals.loc[key_code_datetime]['price'],
                                  min(available_sell, self.__signals.loc[key_code_datetime]['volume']),
                                  Direction.SELL)
                    if 'T' in self.__signals.loc[key_code_datetime]['bs_flag']:
                        order.order_type = OrdType.TWAP
                    elif 'C' in self.__signals.loc[key_code_datetime]['bs_flag']:
                        order.order_type = OrdType.FOC
                    self.insert_order(order)

    def on_finished(self):
        self.__postion_mngt.balance()
        logging.debug("Final Position:")
        for pos in self.__postion_mngt.get_position().values():
            logging.debug(str(pos))
        self.__show_pnl_line()

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
            self.__signals = para.get("signal")
            # 给signal设置一个index, index的形式是str(code)+str(date)+str(time)，以便后续播放行情时查找
            self.__signals['code_date_time'] = self.__signals['code'] + self.__signals['date'].apply(str) + \
                self.__signals['time'].apply(str)
            self.__signals = self.__signals.set_index('code_date_time')
