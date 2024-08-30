from datetime import datetime

__all__ = ['Quote', 'OrderRecord', 'Transaction', 'KBar']


class Quote:
    """
    high: highest price in day
    low: lowest price in day
    """
    __slots__ = ['symbol',
                 'bids',
                 'asks',
                 'date',
                 'timestamp',
                 'time',
                 'preClose',
                 'openPrice',
                 'high',
                 'low',
                 'lastPrice',
                 'totalVolume',
                 'totalAmount',
                 'turnover',
                 'bidVol',
                 'askVol',
                 'limitHigh',
                 'limitLow',
                 'volume',
                 'amount'
                 ]

    def __init__(self, code=0, timestamp: int = ..., time: datetime = ..., bid_price: [] = ...,
                 ask_price: [] = ..., bid_volume: [] = ..., ask_volume: [] = ...,
                 last_price: float = ..., volume: int = ..., amount: float = ...,
                 total_volume: int = ..., total_amount: float = ..., previous_closing_price: float = ...):
        self.symbol = code
        self.time = time
        self.timestamp = timestamp
        self.bids = bid_price
        self.asks = ask_price
        self.bidVol = bid_volume
        self.askVol = ask_volume
        self.lastPrice = last_price
        self.volume = volume
        self.amount = amount
        self.totalVolume = total_volume
        self.totalAmount = total_amount
        self.preClose = previous_closing_price


class KBar:
    __slots__ = ['symbol', 'open', 'close', 'low', 'high', 'volume', 'amount', 'pre_close', 'time', 'date']

    def __init__(self, symbol: str = ..., open: float = ..., close : float = ..., low: float = ...,
                 high: float = ..., volume: int = ..., amount: float = ..., pre_close: float = ...,
                 time: int = ..., date: int = 0):
        self.symbol = symbol
        self.close = close
        self.open = open
        self.high = high
        self.volume = volume
        self.pre_close = pre_close
        self.time = time
        self.low = low
        self.amount = amount
        self.date = date

    def __str__(self):
        return "{} {} {} {} {} {} {} {} {}".format(self.symbol, self.date, self.time, self.open,
                                            self.close, self.high, self.low, self.volume, self.amount)


class OrderRecord:

    __slots__ = ['symbol', 'data', 'direction', 'orderType', 'orderPrice', 'orderQty', 'orderId']

    def __int__(self):
        pass


class Transaction:
    __slots__ = ['symbol', 'time', 'price', 'quantity', 'bs_flag', 'timestamp']

    def __init__(self, symbol, time, price, quantity, bs_flag, timestamp):
        self.symbol = symbol
        self.time = time
        self.price = price
        self.quantity = quantity
        self.bs_flag = bs_flag
        self.timestamp = timestamp
        pass
