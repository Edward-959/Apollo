from abc import abstractmethod
from .common import *
from typing import Dict, Callable, List
from datetime import datetime


class StrategyBase:
    """

    """
    __insert_order:  Callable[[Order], bool]
    __cancel_order:  Callable[[int], bool]
    __get_history_quotes: Callable[[str], List[Quote]]

    def __init__(self, name: str=...):
        self.__strategyName = name
        self.__paraFactor = []
        self.__insert_order = None
        self.__cancel_order = None
        self.__register_factor = None
        self.__get_history_quotes = None

    def set_interface(self, insert_order: Callable[[Order], bool],
                      cancel_order: Callable[[int], None]):
        self.__cancel_order = cancel_order
        self.__insert_order = insert_order

    @property
    def get_history_quotes(self):
        """
        get the handle of fetching history market data, but this always be in day quotes
        :return:
        """
        return self.__get_history_quotes

    @get_history_quotes.setter
    def get_history_quotes(self, value):
        if self.__get_history_quotes is not None:
            raise Exception("{}: get_history_quotes() is re-assigned".format(__file__))
        else:
            self.__get_history_quotes = value

    @property
    def strategy_name(self):
        return self.__strategyName

    @abstractmethod
    def set_config(self, para: Dict=...) ->None:
        pass

    @abstractmethod
    def on_bar_update(self, bar: KBar=...) ->None:
        pass

    @abstractmethod
    def on_quote_updated(self, quote: Quote=...) ->None:
        pass

    @abstractmethod
    def on_transaction_updated(self, trade: Transaction=...) ->None:
        pass

    @abstractmethod
    def on_order_updated(self, order: Order=...) ->None:
        pass

    @abstractmethod
    def on_trade_updated(self, trade: Trade=...) ->None:
        """
        callback function will be invoked when a new trade happened
        :param trade: commom.Trade object
        :return:
        """
        pass

    @abstractmethod
    def suspend(self) ->None:
        pass

    @abstractmethod
    def stop(self) ->None:
        print("{} strategy {} is stopped ".format(datetime.now().strftime("%Y-%m-%d %H:%M:%S"), self.strategy_name))
        pass

    @abstractmethod
    def start(self) ->None:
        print("{} strategy {} is started ".format(datetime.now().strftime("%Y-%m-%d %H:%M:%S"), self.strategy_name))
        pass

    def insert_order(self, order: Order):
        """
        insert a new order
        :param order: Order object, detail in common.MarketData package
        :return
        """
        #self.on_order_updated(order)
        return self.__insert_order(order)

    def cancel_order(self, order_id: int=...):
        """
        cancel order
        :param order_id: Order id
        :return:
        """
        return self.__cancel_order(order_id)

    def register_factor(self, param: Dict):
        """
        register a new factor into factor manager, the factor manager will handle the dependency between factors
        :param param: a dict object for describing factor, it look like:
            {
            "classname": xx,
            "name": yy,
            "save": true
            "param": {...}
            }
            if 'save' set to true, then 'name' must be unique
        :return: FactorBase object
        """
        return self.__register_factor(param)

    def on_new_day(self, day: int = ...):
        """
        this function will be called when finding that new market data is from different Date
        :return: None
        """
        pass

    # def dump_result(self, mode: str = 'pickle'):
    #     print("warning: not implement dump behavior in {}".format(self.__strategyName))
    #     pass
