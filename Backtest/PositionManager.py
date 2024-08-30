from typing import Dict, List, Callable
from .common import Trade, Direction, SecurityType, KBar, Order, OrdStatus
from .StockPosition import StockPosition
from Backtest.PortfolioDivCache import PortfolioDivCache
from .utils.const_defines import *
import logging
from logging import handlers
from datetime import datetime
from copy import deepcopy
from os import mkdir, path, environ
from platform import system

__all__ = ['PositionManager']


class PositionManager:
    """
    账户净资产(持仓市值 + 虚拟现金)计算：持仓市值计算全部用证券价格*证券数量来算，期货或期权都考虑名义本金且有方向。
                                         初始虚拟现金=初始现金. 虚拟现金仅在成交时变动,股票交易和手续费对其直接全额影响,
                                         期货交易对虚拟现金的影响是, 虚拟现金变动=期货成交的名义本金*-1.
                                         例如,原有300万现金(初始虚拟现金300万). 买入100万股票, 开空期货名义本金100万,
                                         手续费2万, 虚拟现金 = 300 - 100(股票) - 2(手续费) + 100(期货名义本金*-1)= 298万.
                                         账户净资产 = 298(虚拟现金)+100(股票)-100(期货名义本金) = 298.
                                         若之后不再交易，则虚拟现金不变，股票市值上涨为102，期货名义本金下跌至-101，
                                         账户净资产=  298(虚拟现金)+102(股票)-101(期货名义本金) = 299.

    手续费：
        不管是期货还是股票， 交易费用都是在收到成交信息时处理， 期货是双边收手续费， 股票只在卖出时收
    资金冻结：
        股票， 在发出委托时, 冻结资金= 委托价* 委托量
               在委托终结时  冻结资金的释放， 释放量 = 委托价* 委托量 - 成交价 * 成交量
        期货， 开仓单
               在发出委托时, 冻结资金= 委托价* 委托量 * 保证金比例
               在委托终结时，释放冻结的资金，  释放量 = (委托价* 委托量 - 成交价 * 成交量) * 保证金比例
               （委托终结状态包括：撤单、部撤和已成）；应当注意：收盘时未成的委托应当撤单

               收到成交信息时：
               每个合约的总保证金＝在持仓表中累计填入冻结的资金.
               为了简化起见，保证金仅在交易(委托)时和收盘时调整，盘中持有期间不调整.

               平仓单只在成交时释放保证金

               以下没有特别说明　持仓量＝期货多头＋期货空头
               该笔交易的实际盈亏 = 1 / 保证金比例 * （开仓的保证金 - 平仓时刻计算的保证金）
               所以简化为平仓时将 保证金释放金额　＝（成交量/持仓量） * 持仓总保证金
               　　　　　　　　　　现金调整　＝　该笔交易的实际盈亏

               每日收盘时的保证金调整：
                调整金额　＝　持仓量 * 收盘价 * 保证金比例 -　持仓表中的期货的总保证金
    --------------------------------------------
    On_order_updated:
        股票或期货开仓委托：减少available_cash. 冻结金额 = 委托价 * 委托量 * 合约乘数（股票1） * 保证金比例（股票100%）
        股票或期货开仓委托结束：增加available_cash. 释放金额 = (委托价* 委托量 - 成交价 * 成交量) * 合约乘数 * 保证金比例
        （委托结束状态包括：撤单、部撤和已成）
        股票或期货平仓委托：pass
    On_trade_updated:
        股票开仓成交：减少available_cash，金额 = 手续费
        期货开仓成交：1) 计算合约保证金、累加到总保证金中
                      2) 并减少available_cash，金额 = 手续费
                      3) 调整virtual_cash，金额 = 名义本金 * (-1) - 手续费
        股票平仓成交：增加available_cash，解冻金额 = 委托价 * 委托量 - 手续费
        期货平仓成交：1) 冻结的保证金解冻（直接按前一日收盘时结算的保证金解冻即可）
                      2) 增加available_cash，金额 = 保证金 + 盈亏结转 - 手续费
                      [该笔交易的实际盈亏 = 1 / 保证金比例 * （开仓的保证金 - 平仓时刻计算的保证金）]
                      3) 调整virtual_cash，金额 = 名义本金 * (-1) - 手续费
    On_new_day:
        收盘时撤掉所有未成交的在挂委托（不在本模块实现，在策略里实现时可转入On_order_updated撤单）
        收盘时调整期货保证金
        收盘时计算每日盈亏，先计算当日净资产，再减去前一日净资产

    """
    def __init__(self, cash: float = ..., cost_ratio: float = 0.0012, fut_cost_ratio: float = 0.00026,
                 margin_ratio: float = 0.2):
        self.__insurance_rate = margin_ratio
        self.__total_cash: float = cash
        self.__virtual_cash: float = cash  # 用于计算盈亏的虚拟现金
        self.__stock_available_cash: float = cash - round(cash * self.__insurance_rate, 2)  # 股票账户可用现金
        self.__stock_frozen_cash: float = 0
        self.__fut_available_cash: float = round(cash * self.__insurance_rate, 2)  # 期货账户可用现金
        self.__fut_frozen_cash: float = 0
        self.__position_dict: Dict[str, StockPosition] = {}
        self.__trade_cost_ratio: float = cost_ratio
        self.__trade_cost_fut_ratio: float = fut_cost_ratio
        self.__trading_date: int = None
        self.__get_balance_data: Callable[[str, List], List] = None
        self.__net_asset_list: List = [[-1, cash, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, cash]]
        self.__max_cash_used: float = 0
        self.__portAdjCache = PortfolioDivCache()
        self.__acc_trade_fee = 0
        self.__daily_trade_fee = 0
        if system() == "Windows":
            log_path = "log"
        else:
            user_id = environ['USER_ID']
            log_path = "/app/data/" + user_id + "/Apollo/log/"
        if not path.exists(log_path):
            if system() == "Windows":
                mkdir(log_path)
            else:
                user_id = environ['USER_ID']
                log_path0 = "/app/data/" + user_id + "/Apollo/"
                if not path.exists(log_path0):
                    mkdir(log_path0)
                mkdir(log_path)
        filename = log_path + "/position_{}.csv".format(datetime.now().strftime("%Y%m%d_%H%M%S"))
        self.__position_logger = logging.getLogger(filename)
        th = handlers.RotatingFileHandler(filename=filename, encoding='utf-8')
        self.__position_logger.addHandler(th)
        self.__position_logger.info("date, code, long, short, net position, last price, security type")
        filename = log_path + "/PNL_{}.csv".format(datetime.now().strftime("%Y%m%d_%H%M%S"))
        self.__mv_logger = logging.getLogger(filename)
        th = handlers.RotatingFileHandler(filename=filename, encoding='utf-8')
        self.__mv_logger.addHandler(th)
        self.__mv_logger.info(
            "date, net asset, daily pnl, daily fee, total fee, stock mv, stock cash, future available cash, "
            "future frozen cash, stock fee, fut fee, stock buy amount, "
            "stock sell amount, fut buy amount, fut sell amount, dividends")
        filename = log_path + "/trades_{}.csv".format(datetime.now().strftime("%Y%m%d_%H%M%S"))
        self.__trade_logger = logging.getLogger(filename)
        th = handlers.RotatingFileHandler(filename=filename, encoding='utf-8')
        self.__trade_logger.addHandler(th)
        self.__trade_logger.info("date, time,  code, price, quantity, turnover, direction")
        #############################################
        # 每日统计数据:
        # 股票买入金额, 股票卖出金额,
        # 期货买入金额, 期货卖出金额,
        # 股票交易手续费, 期货交易手续费
        #######################################
        self.__stock_buy_amount = 0
        self.__stock_sell_amount = 0
        self.__fut_buy_amount = 0
        self.__fut_sell_amount = 0
        self.__fut_fee = 0
        self.__stock_fee = 0
        self.__stock_pos_number = 0
        self.__dividends = 0

        pass

    @property
    def total_cash(self):
        return self.__total_cash

    @property
    def available_cash(self):
        return self.__stock_available_cash

    @property
    def frozen_cash(self):
        return self.__stock_frozen_cash

    @property
    def max_cash_used(self):
        return self.__max_cash_used

    def load_position(self, file_name: str = ...):
        """
        读取保存的持仓
        :param file_name:
        :return:
        """
        pass

    def on_trade(self, trade: Trade = ...):
        self.__trade_logger.info("{}, {}".format(self.__trading_date, str(trade)))
        if trade.symbol not in self.__position_dict.keys():
            self.__position_dict.update({trade.symbol: StockPosition(trade.symbol)})
        if trade.sec_type == SecurityType.CS:  # 股票
            self.__on_trade_stock(trade)
        elif trade.sec_type == SecurityType.FUT:  # 期货
            self.__on_trade_future(trade)
        elif trade.sec_type == SecurityType.OPT:  # 期权
            self.__on_trade_option(trade)
        else:
            print("{} trade isn't supported yet".format(trade.sec_type))
        self.__max_cash_used = max(self.__max_cash_used, self.total_cash - self.available_cash)

    def __on_trade_stock(self, trade: Trade =...):
        position = self.__position_dict.get(trade.symbol)
        if trade.direction == Direction.BUY:
            position.position += trade.quantity
            position.long_position += trade.quantity
            position.daily_buy += trade.quantity
            position.cost += trade.turnover
            self.__stock_buy_amount += trade.turnover
            self.__virtual_cash -= trade.turnover
            ############################################
            # 买入股票的时候在收到委托信息是资金已经冻结
            # 此处不再做处理
            ############################################
        else:
            if position.position - trade.quantity < 0:
                Exception("股票持仓不能为负的")
            trade_cost = trade.turnover * self.__trade_cost_ratio
            position.position -= trade.quantity
            position.long_position -= trade.quantity
            position.daily_sell += trade.quantity
            self.__virtual_cash += (trade.turnover - trade_cost)
            self.__stock_available_cash += trade.turnover - trade_cost
            position.cost -= trade.turnover - trade_cost
            if position.position == 0 and position.daily_buy == 0 and position.daily_sell == 0:
                self.__position_dict.pop(position.symbol)
            position.daily_trade_cost += trade_cost
            self.__acc_trade_fee += trade_cost
            self.__daily_trade_fee += trade_cost
            self.__stock_fee += trade_cost
            self.__stock_sell_amount += trade.turnover

    def __on_trade_future(self, trade: Trade =...):
        position = self.__position_dict.get(trade.symbol)
        position.sec_type = SecurityType.FUT
        trade_cost = trade.turnover * self.__trade_cost_fut_ratio
        self.__daily_trade_fee += trade_cost
        self.__acc_trade_fee += trade_cost
        self.__virtual_cash -= trade_cost
        self.__fut_available_cash -= trade_cost
        self.__fut_fee += trade_cost
        if trade.direction == Direction.OPEN_LONG:
            position.position += trade.quantity
            position.daily_buy += trade.quantity
            position.cost += trade.turnover
            position.long_position += trade.quantity
            position.cash_frozen += trade.quantity * trade.price * Const.CONTRACT_MULTIPLIER.get(trade.symbol)
            self.__virtual_cash -= trade.turnover
            self.__fut_buy_amount += trade.turnover
        elif trade.direction == Direction.CLOSE_SHORT:
            #########################################################
            # 释放保证金， 计算盈亏， 调整可用资金
            ###########################################
            weight = Const.CONTRACT_MULTIPLIER.get(trade.symbol)
            frozen_cash_to_free = position.cash_frozen * trade.quantity
            frozen_cash_to_free /= (position.long_position + position.short_position)
            frozen_cash_to_free = frozen_cash_to_free
            profit = (weight * trade.price * trade.quantity - frozen_cash_to_free / self.__insurance_rate)
            position.cash_frozen -= frozen_cash_to_free
            self.__fut_available_cash += frozen_cash_to_free - profit
            self.__fut_frozen_cash -= frozen_cash_to_free
            #######################################################
            position.position += trade.quantity
            position.daily_buy += trade.quantity
            position.cost += trade.turnover
            position.short_position -= trade.quantity
            self.__virtual_cash -= trade.turnover
            self.__fut_buy_amount += trade.turnover

        elif trade.direction == Direction.OPEN_SHORT:
            position.position -= trade.quantity
            position.daily_sell += trade.quantity
            position.short_position += trade.quantity
            cash = trade.quantity * trade.price * Const.MARGIN_RATE * Const.CONTRACT_MULTIPLIER.get(trade.symbol)
            position.cash_frozen += cash
            self.__virtual_cash += trade.turnover
            position.cost -= trade.turnover - trade_cost
            self.__fut_sell_amount += trade.turnover
        else:
            #########################################################
            # 释放保证金， 计算盈亏， 调整可用资金
            ###########################################
            weight = Const.CONTRACT_MULTIPLIER.get(trade.symbol)
            frozen_cash_to_free = position.cash_frozen * trade.quantity
            frozen_cash_to_free /= (position.long_position + position.short_position)
            frozen_cash_to_free = frozen_cash_to_free
            profit = (weight * trade.price * trade.quantity - frozen_cash_to_free / self.__insurance_rate)
            position.cash_frozen -= frozen_cash_to_free
            self.__fut_available_cash += frozen_cash_to_free + profit
            self.__fut_frozen_cash -= frozen_cash_to_free
            ####################################################
            position.position -= trade.quantity
            position.daily_sell += trade.quantity
            position.long_position -= trade.quantity
            self.__virtual_cash += trade.turnover
            position.cost -= trade.turnover - trade_cost
            self.__fut_sell_amount += trade.turnover

        if position.long_position + position.short_position < 0.0001:
            self.__position_dict.pop(position.symbol)
        # position.cur_price = trade.price
        position.daily_trade_cost += trade_cost

    def __on_trade_option(self, trade: Trade =...):
        position = self.__position_dict.get(trade.symbol)
        trade_cost = trade.turnover * self.__trade_cost_ratio
        if trade.direction in [Direction.OPEN_LONG, Direction.CLOSE_SHORT]:
            position.position += trade.quantity
            position.daily_buy += trade.quantity
            position.cost += trade.turnover
            self.__virtual_cash -= trade.turnover
        else:
            position.position -= trade.quantity
            position.daily_sell += trade.quantity
            self.__virtual_cash += (trade.turnover - trade_cost)
            position.cost -= trade.turnover - trade_cost
            if position.position == 0:
                self.__position_dict.pop(position.symbol)
        # position.return_rate = ((position.cur_price * position.position) - position.cost)
        position.daily_trade_cost += trade_cost

    def on_new_day(self, date: int = ...):
        self.balance()
        self.__trading_date = date

    def balance(self):
        """
        处理结算事件
        1. 读取结算文件
        2. 处理除权除息后的股票仓位变化
        3. 计算日内PNL
        :return:
        """
        today_total_mv = 0
        stock_mv = 0
        stock_num = 0
        temp_position_keys = deepcopy(list(self.__position_dict.keys()))
        for symbol in temp_position_keys:
            pos = self.__position_dict[symbol]
            if pos.position > 0:
                stock_num += 1
            if pos.position == 0 and pos.daily_buy == 0 and pos.daily_sell == 0:
                self.__position_dict.pop(pos.symbol)
                continue
        for pos in self.__position_dict.values():
            temp_mv = self.__take_balance_action(pos)
            today_total_mv += temp_mv
            if pos.sec_type == SecurityType.CS:
                stock_mv += temp_mv
            pos.holding_days += 1
            ####################################################
            #   date, code, long, short, net,
            pos_record = "{}, {}, {}, {}, {}, {} ,{}".format(self.__trading_date, pos.symbol, pos.long_position,
                                                             pos.short_position, pos.position, pos.cur_price,
                                                             pos.sec_type)
            self.__position_logger.info(pos_record)
        today_total_mv += self.__virtual_cash
        if stock_mv == 0:
            actual_cash_used = self.__stock_available_cash + self.__fut_available_cash
        else:
            actual_cash_used = stock_mv + self.__fut_frozen_cash
        pre_day_actual_cash_used = self.__net_asset_list[-1][-1]
        daily_pnl = today_total_mv - self.__net_asset_list[-1][1]
        net_value = self.__net_asset_list[-1][-2] * (1 + daily_pnl/pre_day_actual_cash_used)
        if self.__net_asset_list[-1][7] != 0:
            turnover = (self.__stock_buy_amount + self.__stock_sell_amount) / 2 / self.__net_asset_list[-1][7]
        else:
            turnover = 0
        self.__net_asset_list.append([self.__trading_date,
                                      today_total_mv,
                                      round(daily_pnl/pre_day_actual_cash_used*100, 5),
                                      self.__stock_available_cash,
                                      daily_pnl,
                                      round(self.__daily_trade_fee, 2),
                                      round(self.__acc_trade_fee, 2),
                                      round(stock_mv, 2),
                                      round(self.__fut_available_cash, 2),
                                      round(self.__fut_frozen_cash, 2),
                                      round(self.__stock_fee, 2),
                                      round(self.__fut_fee, 2),
                                      round(self.__stock_buy_amount, 2),
                                      round(self.__stock_sell_amount, 2),
                                      round(self.__fut_buy_amount, 2),
                                      round(self.__fut_sell_amount, 2),
                                      round(self.__dividends, 2),
                                      stock_num,
                                      turnover,
                                      net_value,
                                      actual_cash_used,
                                      ])

        self.__mv_logger.info("{}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}"
                              .format(self.__trading_date, net_value, actual_cash_used, round(today_total_mv), round(daily_pnl, 2),
                                      round(self.__daily_trade_fee, 2),
                                      round(self.__acc_trade_fee, 2),
                                      round(stock_mv, 2),
                                      round(self.__stock_available_cash, 2), round(self.__fut_available_cash, 2),
                                      round(self.__fut_frozen_cash, 2),
                                      round(self.__stock_fee, 2),
                                      round(self.__fut_fee, 2),
                                      round(self.__stock_buy_amount, 2),
                                      round(self.__stock_sell_amount, 2),
                                      round(self.__fut_buy_amount, 2),
                                      round(self.__fut_sell_amount, 2),
                                      round(self.__dividends, 2)
                                      ))
        self.__daily_trade_fee = 0
        self.__fut_fee = 0
        self.__stock_fee = 0
        self.__fut_buy_amount = 0
        self.__stock_buy_amount = 0
        self.__fut_sell_amount = 0
        self.__stock_sell_amount = 0
        self.__dividends = 0

    def get_market_value(self):
        """
        获取持仓市值，包括现金
        :return:
        """
        mv = 0
        for pos in self.__position_dict.values():
            if pos.sec_type == SecurityType.FUT:
                continue
            mv += pos.cur_price * pos.position
        mv += self.__stock_available_cash
        return mv

    def get_market_value_by_symbol(self, symbol: str = ...):
        """
        获取单个股票的持仓市值
        :param symbol: 股票代码
        :return: double 金额
        """
        mv = 0
        if symbol in self.__position_dict.keys():
            pos = self.__position_dict.get(symbol)
            mv = pos.position * pos.cur_price
            if pos.sec_type == SecurityType.FUT:
                mv = mv * Const.CONTRACT_MULTIPLIER.get(symbol)
        return mv

    def get_pnl_data(self):
        """
        获取持仓和资金数据时间序列， 日期序列有一个为INITDATE为初始日期，非数字，表示期初
        :return: 返回 [[日期序列]，
                        [每日总市值(stock+cash)序列],
                        [每日相对前一日的总市值收益率序列]，
                        [每日现金序列],
                        [每日收益金额序列]
                        ]
        """
        return self.__net_asset_list

    def save_pnl_chart(self, file):
        if self.max_cash_used == 0:
            logging.info("no trade happened")
            return
        import plotly
        import plotly.graph_objs as go
        x = ["D{}".format(_x[0]) for _x in self.__net_asset_list]
        y = [((_x[1] - self.total_cash) / self.max_cash_used) for _x in self.__net_asset_list]
        plotly.offline.plot({
            "data": [go.Scatter(x=x, y=y, line=dict(color='rgb(205, 12, 24)', width=4))],
            "layout": go.Layout(title="PNL", xaxis=dict(title='日期'))},
            filename=file, auto_open=True)
        logging.info("Generated PNL Chart file: {}".format(file))

    def set_trade_cost(self, ref: float=...) -> None:
        self.__trade_cost_ratio = ref

    def get_position(self) -> dict:
        """
        返回头寸，Dict[str, StockPosition]
        :return:
        """
        return self.__position_dict

    def get_hold_position(self, code):
        if code in self.__position_dict.keys():
            return self.__position_dict.get(code).position
        return 0

    def on_bar(self, bar: KBar = ...):
        if bar.symbol in self.__position_dict.keys():
            position: StockPosition = self.__position_dict.get(bar.symbol)
            position.cur_price = bar.close
            position.return_rate = ((position.cur_price * position.position) - position.cost) / position.cost

    def __take_balance_action(self, position: StockPosition = ...):
        """
        处理分红送股，分红的现金会放入资金池中
        获取的分红送转信息，是对应10股为单位的，所以股数要先除以10，换算了分红送转之后再乘以10
        :param position:
        :return: 返回股票市值
        """
        if position.sec_type == SecurityType.FUT:
            weight = Const.CONTRACT_MULTIPLIER.get(position.symbol)
            position.daily_trade_cost = 0
            ##############################################
            # 结算时调整冻结资金
            ################################################
            total_pos = (position.long_position + position.short_position)
            profit = (position.cur_price * weight - position.cash_frozen/total_pos/Const.MARGIN_RATE)
            profit *= position.position
            adj_frozen_cash = total_pos * position.cur_price
            adj_frozen_cash *= Const.MARGIN_RATE * weight
            adj_frozen_cash -= position.cash_frozen
            adj_frozen_cash = adj_frozen_cash
            position.cash_frozen += adj_frozen_cash
            self.__fut_available_cash -= adj_frozen_cash
            self.__fut_available_cash += profit
            self.__fut_frozen_cash += adj_frozen_cash
            return position.position * position.cur_price * weight

        ####################################################################
        # 计算分红时：除权除息日（分红送转当日）买入的持仓不参与分红送转，昨日的持仓（即使今天卖出）也可以拿到分红
        # position.position是t日末交易结束后的头寸
        # 先通过position - daily_buy + daily_sell还原出t-1日收盘时的头寸（参与分红送转的有效头寸）
        # 随后计算当天的分红送转，因为分红送转一般是【10派X、10送X】的形式，所以要先除以10、算完后再乘以10
        # 最后 + daily_buy - daily_sell 计算出t日收盘时应有的头寸
        ######################################################################
        dividend_info = self.__portAdjCache.get_query_day_div_info(position.symbol, self.__trading_date)
        # odd_share是除以10后的零股
        position_div_by_10, odd_share = divmod(position.position - position.daily_buy + position.daily_sell, 10)
        if dividend_info["per_cashpaidaftertax"] != 0:
            dividend_cash = position_div_by_10 * dividend_info["per_cashpaidaftertax"] * 10
            self.__virtual_cash += dividend_cash
            self.__stock_available_cash += dividend_cash
            self.__dividends += dividend_cash
            print(position.symbol, "分红:", str(round(dividend_cash, 2)), position.position, position.daily_buy,
                  position.daily_sell)
        if dividend_info["per_div_trans"] != 0:
            position.position = int(position_div_by_10 * (1 + dividend_info["per_div_trans"]) * 10 + odd_share)
            position.position += position.daily_buy - position.daily_sell
            position.long_position = position.position
        position.pre_close = position.cur_price
        position.pre_position = position.position
        position.available_sell = position.position
        position.daily_sell = 0
        position.daily_buy = 0
        position.daily_trade_cost = 0
        return position.position * position.cur_price

    def get_available_sell(self, symbol):
        if symbol not in self.__position_dict:
            return 0
        return self.__position_dict.get(symbol).available_sell

    def on_order_update(self, order: Order):
        #################################################
        #  卖股票的资金收入在收到成交信息时加入到资金池了
        #  由于可能发生一次委托多次成交，在这里计算不更方便
        ###################################################
        if order.sec_type == SecurityType.CS:
            if order.direction == Direction.BUY:

                if order.status == OrdStatus.NEW:
                    #########################
                    # 冻结资金
                    #########################
                    self.__stock_available_cash -= order.price * order.order_qty
                elif order.is_finished():
                    #########################
                    # 成交时更具成交价和成两调整冻结资金
                    #########################
                    cash = order.price * order.order_qty - order.trade_price * order.cum_qty
                    cash = cash
                    self.__stock_available_cash += cash
            else:
                if order.status == OrdStatus.NEW:
                    #################################
                    # 冻结可卖余额
                    ##################################
                    position = self.__position_dict.get(order.symbol)
                    position.available_sell -= order.order_qty
                elif order.is_finished():
                    ############################
                    # 如果未全部成交则释放未成交的部分可卖量
                    #########################
                    position = self.__position_dict.get(order.symbol)
                    position.available_sell += order.order_qty - order.cum_qty

        elif order.sec_type == SecurityType.FUT and order.direction in [Direction.OPEN_LONG, Direction.OPEN_SHORT]:
            weight = Const.CONTRACT_MULTIPLIER.get(order.symbol)
            if order.status == OrdStatus.NEW:
                #########################
                # 冻结资金
                #########################
                adj_cash = weight * order.price * order.order_qty * Const.MARGIN_RATE
                self.__fut_available_cash -= adj_cash
                self.__fut_frozen_cash += adj_cash
            elif order.is_finished():
                #########################
                # 成交时更具成交价和成两调整冻结资金
                #########################
                adj_cash = (order.price * order.order_qty - order.trade_price * order.cum_qty)
                adj_cash = Const.MARGIN_RATE * weight * adj_cash
                self.__fut_available_cash += adj_cash
                self.__fut_frozen_cash -= adj_cash

    def get_exposure(self):
        portfolio_value = 0
        for pos in self.__position_dict.values():
            if pos.sec_type == SecurityType.FUT:
                weight = Const.CONTRACT_MULTIPLIER.get(pos.symbol)
                portfolio_value += weight * pos.position * pos.cur_price
            else:
                portfolio_value += pos.position * pos.cur_price
        return portfolio_value

    def get_stock_buy_amount(self):
        return self.__stock_buy_amount

    def get_stock_sell_amount(self):
        return self.__stock_sell_amount

    def get_stock_available_cash(self):
        return self.__stock_available_cash

    def stock_clearance(self, stock_code):  # 将一只股票清仓
        if stock_code in self.__position_dict:
            pos = self.__position_dict[stock_code]
            self.__virtual_cash += pos.position * pos.cur_price
            self.__stock_available_cash += pos.position * pos.cur_price
        self.__position_dict.pop(stock_code)
