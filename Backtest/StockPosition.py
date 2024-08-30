from .common import SecurityType


class StockPosition:
    __slots__ = ["__symbol",
                 'pre_position',
                 'position',
                 'available_sell',
                 'daily_buy',
                 'daily_sell',
                 'daily_pnl',
                 'pre_close',
                 'cur_price',
                 'daily_trade_cost',
                 'holding_days',
                 'return_rate',
                 'cost',
                 'sec_type',
                 'cash_frozen',
                 'long_position',
                 'short_position'
                 ]

    def __init__(self, symbol: str = ...):
        self.__symbol = symbol
        self.pre_position: float = 0  # 前一天收盘时的持仓数量（如昨有分红送转，已考虑进来）
        self.position: int = 0  # 当前时刻的持仓数量
        self.daily_buy: int = 0  # 今日买量
        self.daily_sell: int = 0  # 今日卖量
        self.daily_pnl: float = 0
        self.pre_close: float = 0
        self.daily_trade_cost: float = 0
        self.cur_price = 0
        self.available_sell = 0
        self.holding_days = 0
        self.return_rate = 0
        self.cost = 0
        self.sec_type = SecurityType.CS
        self.cash_frozen = 0
        self.long_position = 0
        self.short_position = 0

    def __str__(self):
        return "position: {} {} {} {} {} {} {} {}".\
            format(self.symbol, round(self.position, 3), self.long_position, self.short_position,
                   round(self.cur_price, 2), self.holding_days,
                   self.available_sell, self.cash_frozen)

    @property
    def symbol(self):
        return self.__symbol
