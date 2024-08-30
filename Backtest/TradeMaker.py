"""
this module is used for trade making
"""
from .common import Quote, Trade, Order, Transaction, KBar, Direction, OrdStatus, OrdType, SecurityType
from typing import Callable, Dict, List
import logging
import copy
from datetime import datetime
from .utils.const_defines import *
import DataAPI.DataToolkit as Dtk


class TradeMaker:
    __max_deal_ratio = 0.3
    __global_order_seq = 0
    __high_limit = 1 + 0.098
    __low_limit = 1 - 0.098

    def __init__(self, start_date, end_date, hedge_index):
        self.__callbacks_order_update: List[Callable[[Order], None]] = []
        self.__callbacks_trade: List = []
        self.__current_bar_dict: Dict[str, KBar] = {}
        self.__current_quote_dict: Dict[str, List[Quote]] = {}
        self.__current_trans_dict: Dict[str, List[Transaction]] = {}
        self.__order_list_dict: Dict[str, List[Order]] = {}
        self.__order_list: List[Order] = []
        self.__pre_make_trade_time = None
        __complete_stock_list = Dtk.get_complete_stock_list()
        __complete_stock_list.append(hedge_index)
        self.__start_date = start_date
        self.__end_date = end_date
        self.__pre_close_df = Dtk.get_panel_daily_pv_df(__complete_stock_list, start_date, end_date, 'pre_close')
        self.__current_day_pre_close_dict = {}

    def __update_order(self, order: Order = ...):
        for func in self.__callbacks_order_update:
            func(order)

    def __update_trade(self, trade: Trade = ...):
        for func in self.__callbacks_trade:
            func(trade)
        logging.debug(str(trade))

    def on_transaction(self, trans: Transaction)->None:
        pass

    def on_bar(self, bar: KBar) -> None:
        self.__current_bar_dict.update({bar.symbol: bar})
        if bar.time != self.__pre_make_trade_time:
            for it_symbol in self.__order_list_dict.keys():
                self.__make_trade_bar(it_symbol)
            self.__pre_make_trade_time = bar.time

    def on_quote(self, quote: Quote)->None:
        pass

    def cancel_order(self, order_id: int = ...):
        order = self.__order_list[order_id]
        if order.cum_qty != 0:
            order.status = OrdStatus.PARTIALLY_CANCELLED
        else:
            order.status = OrdStatus.CANCELLED
        ord_list: List[Order] = self.__order_list_dict.get(order.symbol)
        ord_list.remove(order)

    def insert_order(self, new_order: Order):
        if new_order.order_qty <= 0:
            raise Exception("order quantity is less than 0!!!")
        new_order.status = OrdStatus.NEW
        order = copy.copy(new_order)
        if order.symbol not in self.__order_list_dict.keys():
            self.__order_list_dict.update({order.symbol: []})
        order.order_id = self.__global_order_seq
        self.__global_order_seq += 1
        self.__order_list.append(order)
        self.__order_list_dict.get(order.symbol).append(order)
        self.__update_order(order)
        # self.__make_order_deal(order)
        # if order.status == OrdStatus.NEW or order.status == OrdStatus.PARTIALLY_FILLED:
        #     self.__order_list_dict.get(order.symbol).append(order)

    def sub_order_update(self, func):
        self.__callbacks_order_update.append(func)

    def sub_trade_update(self, func):
        self.__callbacks_trade.append(func)

    def __make_trade_bar(self, symbol: str = ...):
        """
        撮合所有未完成的委托
        :param symbol:
        :return:
        """
        # if symbol not in self.__order_list_dict.keys() or len(self.__order_list_dict.get(symbol)) == 0:
        #     return
        remains_order_list: List[Order] = []
        for order in self.__order_list_dict.get(symbol):
            self.__make_order_deal(order)
            if not order.is_finished():
                remains_order_list.append(order)
        self.__order_list_dict.update({symbol: remains_order_list})

    def __make_order_deal(self, order: Order):
        if order.direction == Direction.BUY or order.direction == Direction.OPEN_LONG \
                or order.direction == Direction.CLOSE_SHORT:
            self.__make_buy_order(order)
        else:
            self.__make_sell_order(order)

    def __make_buy_order(self, order: Order):
        pre_order_update_time = order.update_time
        new_trade = None
        flag = True
        while flag:
            flag = False
            volume_remain = order.order_qty - order.cum_qty
            if order.symbol not in self.__current_bar_dict.keys():
                order.status = OrdStatus.NEW
                break
            cur_bar: KBar = self.__current_bar_dict.get(order.symbol)
            order_price = self.__order_pre_process(order, cur_bar)
            pre_close = self.__current_day_pre_close_dict[cur_bar.symbol]
            ######################################
            # 不成交
            ###############################
            # 如是涨停、则无法买到
            if order.price >= cur_bar.high and order.price >= pre_close * self.__high_limit:
                order.status = OrdStatus.REJECTED
                order.update_time = cur_bar.time
                break
            if order_price < cur_bar.low or cur_bar.volume <= 0:
                if order.status == OrdStatus.PENDING_NEW:
                    order.status = OrdStatus.NEW
                    order.update_time = cur_bar.time
                break
            #################################
            # 根据价格成交
            ###############################
            new_trade = Trade(symbol=order.symbol, order_id=order.order_id, time=cur_bar.time,
                              direction=order.direction, sec_type=order.sec_type)
            new_trade.price = order_price
            if order.sec_type == SecurityType.FUT:
                new_trade.quantity = order.order_qty
                contract_multiplier = Const.CONTRACT_MULTIPLIER.get(order.symbol)
            else:
                contract_multiplier = 1
                temp_price = max(order_price, cur_bar.low)
                if temp_price >= pre_close * self.__low_limit >= cur_bar.high:  # 如是跌停、是可以买入的
                    new_trade.quantity = volume_remain
                elif cur_bar.high == cur_bar.low:
                    ratio = 0.0001
                    new_trade.quantity = min(int((ratio * cur_bar.volume) / 100) * 100, volume_remain)
                else:
                    ratio = (temp_price - cur_bar.low) / (cur_bar.high - cur_bar.low)
                    new_trade.quantity = min(int((ratio * cur_bar.volume) / 100) * 100, volume_remain)
                if new_trade.quantity == 0:
                    break
            order.cum_qty += new_trade.quantity
            order.trade_price = order_price
            new_trade.turnover = round(contract_multiplier * new_trade.price * new_trade.quantity, 2)
            order.update_time = datetime.strptime(str(cur_bar.date * 10000 + cur_bar.time), "%Y%m%d%H%M")
        self.__order_after_process(order, cur_bar)
        if pre_order_update_time != order.update_time:
            self.__update_order(order)
        if new_trade is not None and new_trade.quantity != 0:
            self.__update_trade(new_trade)

    def __make_sell_order(self, order: Order):
        pre_order_update_time = order.update_time
        new_trade = None
        flag = True
        while flag:
            flag = False
            volume_remain = order.order_qty - order.cum_qty
            if order.symbol not in self.__current_bar_dict.keys():
                order.status = OrdStatus.NEW
                break
            cur_bar: KBar = self.__current_bar_dict.get(order.symbol)

            order_price = self.__order_pre_process(order, cur_bar)
            pre_close = self.__current_day_pre_close_dict[cur_bar.symbol]
            ######################################
            #  不成交
            ###############################
            # 如是跌停、则无法卖出
            if order.price <= cur_bar.low and order.price <= pre_close * self.__low_limit:
                order.status = OrdStatus.REJECTED
                order.update_time = cur_bar.time
                break
            if order_price > cur_bar.high or cur_bar.volume <= 0:
                if order.status == OrdStatus.PENDING_NEW:
                    order.status = OrdStatus.NEW
                    order.update_time = cur_bar.time
                break
            #################################
            # 根据价格成交
            ###############################
            new_trade = Trade(symbol=order.symbol, order_id=order.order_id, time=cur_bar.time,
                              direction=order.direction, sec_type=order.sec_type)
            new_trade.price = order_price
            if order.sec_type == SecurityType.FUT:
                new_trade.quantity = order.order_qty
                contract_multiplier = Const.CONTRACT_MULTIPLIER.get(order.symbol)
            else:
                contract_multiplier = 1
                temp_price = min(order_price, cur_bar.high)
                if temp_price <= pre_close * self.__high_limit <= cur_bar.low:  # 如是涨停、是可以卖出的
                    new_trade.quantity = volume_remain
                elif cur_bar.high == cur_bar.low:
                    ratio = 0.0001
                    new_trade.quantity = min(int((ratio * cur_bar.volume) / 100) * 100, volume_remain)
                else:
                    ratio = (cur_bar.high - temp_price) / (cur_bar.high - cur_bar.low)
                    new_trade.quantity = min(int((ratio * cur_bar.volume) / 100) * 100, volume_remain)
                if new_trade.quantity == 0:
                    break
            order.cum_qty += new_trade.quantity
            order.trade_price = order_price
            new_trade.turnover = round(contract_multiplier * new_trade.price * new_trade.quantity, 2)
            order.update_time = datetime.strptime(str(cur_bar.date * 10000 + cur_bar.time), "%Y%m%d%H%M")
        self.__order_after_process(order, cur_bar)
        if pre_order_update_time != order.update_time:
            self.__update_order(order)
        if new_trade is not None and new_trade.quantity != 0:
            self.__update_trade(new_trade)

    @staticmethod
    def __order_pre_process(order: Order, cur_bar: KBar):
        order_price = order.price
        if order.order_type == OrdType.TWAP:
            if cur_bar.volume == 0:
                order_price = cur_bar.close
            else:
                if order.sec_type != SecurityType.FUT:
                    order_price = cur_bar.amount / cur_bar.volume
                else:
                    order_price = (cur_bar.high + cur_bar.low) / 2
        return order_price

    def __order_after_process(self, order: Order, cur_bar: KBar):
        if order.status == OrdStatus.REJECTED:
            return
        if order.order_type == OrdType.FOC:
            if order.cum_qty > 0:
                if order.cum_qty != order.order_qty:
                    order.status = OrdStatus.PARTIALLY_CANCELLED
                else:
                    order.status = OrdStatus.FILLED
            else:
                order.status = OrdStatus.CANCELLED
            order.update_time = datetime.strptime(str(cur_bar.date * 10000 + cur_bar.time), "%Y%m%d%H%M")
        else:
            if order.cum_qty > 0:
                if order.cum_qty != order.order_qty:
                    order.status = OrdStatus.PARTIALLY_FILLED
                else:
                    order.status = OrdStatus.FILLED
            else:
                order.status = OrdStatus.NEW

    def on_new_day(self, day: int = ...):
        for stock in self.__pre_close_df.columns:
            current_day_pre_close = self.__pre_close_df.loc[day]
            self.__current_day_pre_close_dict.update({stock: current_day_pre_close[stock]})
        for order_list in self.__order_list_dict.values():
            for order in order_list:
                if order.cum_qty > 0:
                    order.status = OrdStatus.PARTIALLY_CANCELLED
                else:
                    order.status = OrdStatus.CANCELLED
                self.__update_order(order)
        self.__order_list.clear()
        self.__order_list_dict.clear()
        self.__current_bar_dict.clear()
