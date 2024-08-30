from enum import IntEnum, unique, Enum
from .MarketData import Quote, Transaction, KBar

__all__ = ['Direction', 'Order', 'OrdStatus', 'OrdType', 'SecurityType', 'Trade',  'Quote', 'Transaction', 'KBar']


@unique
class Direction(Enum):
    BUY = 0
    SELL = 1
    OPEN_LONG = 2
    OPEN_SHORT = 3
    CLOSE_LONG = 4
    CLOSE_SHORT = 5


@unique
class OrdType(Enum):
    MARKET = 1
    LIMIT = 2
    FOC = 3
    FOK = 4
    TWAP = 5


@unique
class OrdStatus(Enum):
    PENDING_NEW = 0
    NEW = 1
    PARTIALLY_FILLED = 2
    FILLED = 3
    CANCELLED = 4
    PARTIALLY_CANCELLED = 5
    REJECTED = 6


@unique
class SecurityType(Enum):
    CS = 0
    FUT = 1
    OPT = 2
    COMMODITY = 3


class Trade:
    # __slots__ = ['symbol', 'price', 'quantity', 'direction', 'order_id', 'timestamp', 'turnover', 'id', 'sec_type']
    # 润泽：似乎是类变量sequence和slots之间有什么冲突，导致self.sequence只读，所以self.sequence += 1不成功
    sequence: int = 0

    def __init__(self, symbol, time, order_id,  price: float=0, qty: int=0,
                 direction: Direction = Direction.BUY, turnover: float = 0, sec_type=SecurityType.CS):
        """
        成交信息
        :param symbol: str, stock code
        :param time: trade occur time
        :param order_id: order id which this trade belongs to
        :param price: order price
        :param qty: order quatity
        :param direction: order direction, type Direction
        :param turnover: order amount
        :param sec_type: Security type
        """
        self.symbol = symbol
        self.price = price
        self.quantity = qty
        self.direction = direction
        self.id = self.sequence
        self.sequence += 1
        self.order_id = order_id
        self.sec_type = sec_type
        self.turnover = turnover
        self.timestamp = time

    def __str__(self):
        return "{} {} {} {} {} {}".format(self.timestamp, self.symbol, round(self.price, 2), self.quantity,
                                                 round(self.turnover, 2),
                                                 self.direction)


class Order:
    __slots__ = ['symbol', 'price', 'order_qty', 'cum_qty', 'direction', 'order_id', 'order_type', 'timestamp',
                 'sec_type', 'trade_price', 'status', 'cash_frozen', 'update_time']

    def __init__(self, symbol, price, quantity, direction, order_type=OrdType.LIMIT, security_type=SecurityType.CS):
        """

        :param symbol: stock or future or option id
        :param price:
        :param direction: Buy, Sell, open long, open short, close long, close short
        :param order_type: order type eg. limit which isn't used yet
        :param security_type: stock, future or option
        """
        self.symbol = symbol
        self.price = price
        self.order_qty = quantity
        self.direction = direction
        self.order_id = -1
        self.cum_qty = 0
        self.order_type = order_type
        self.sec_type = security_type
        self.trade_price = 0
        self.status = OrdStatus.PENDING_NEW
        self.timestamp = 0
        self.cash_frozen = 0
        self.update_time = 0

    def __str__(self):
        return "Order: {} {} {} {} {} {} {} {} {} {}".format(self.order_id, self.symbol, self.status, self.order_qty,
                                                             self.cum_qty, self.trade_price, self.order_type,
                                                             self.direction, self.sec_type, self.timestamp)

    def is_finished(self):
        if self.status in [OrdStatus.REJECTED, OrdStatus.CANCELLED, OrdStatus.FILLED, OrdStatus.PARTIALLY_CANCELLED]:
            return True
        else:
            return False
