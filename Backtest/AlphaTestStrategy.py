from Backtest.StrategyBase import StrategyBase
from Backtest.PositionManager import PositionManager
from Backtest.common import Order, Quote, KBar, Transaction, Trade, Direction, OrdStatus, OrdType, SecurityType
from typing import Dict
from pandas import DataFrame
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
        self.__F_M_MA130: DataFrame = None
        self.__F_M_Std130: DataFrame = None
        self.__F_M_MinKSwing20: DataFrame = None
        self.__F_M_Slope20: DataFrame = None
        self.__pre_F_MA130: Dict[float] = {}
        self.__pre_F_Std130: Dict[float] = {}
        self.__pre_F_Slop20: Dict[float] = {}
        self.__pre_F_MinKSwing20: Dict[float] = {}
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
        self.__F_M_MinKSwing20 = load_factor("F_M_MinKSwing20", self.__all_stock_list, start_time, end_time)
        self.__F_M_Slope20 = load_factor("F_M_Slope20", self.__all_stock_list, start_time, end_time)
        self.__F_M_Std130 = load_factor("F_M_Std130", self.__all_stock_list, start_time, end_time)
        self.__F_M_MA130 = load_factor("F_M_MA130", self.__all_stock_list, start_time, end_time)

        self.__F_M_MinKSwing20 = self.__F_M_MinKSwing20.reset_index().values
        self.__F_M_Slope20 = self.__F_M_Slope20.reset_index().values
        self.__F_M_Std130 = self.__F_M_Std130.reset_index().values
        self.__F_M_MA130 = self.__F_M_MA130.reset_index().values

        self.__adj_factor_df = get_panel_daily_info(trade_stock_list, int(start_time.strftime("%Y%m%d")),
                                                    int(end_time.strftime("%Y%m%d")), info_type="adjfactor")

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
        #################################################################
        for code in self.__all_stock_list:
            self.__current_adj.update({code: self.__adj_factor_df.at[day, code]})

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
        F_M_MinKSwing20 < 0.01
        F_M_Slope20 < 0.01
        Close_t-1 < F_M_MA130_t-1 + 2*F_M_Std130_t-1
        Close_t > F_M_MA130_t + 2*F_M_Std_t

        先以这4个条件开仓:
        F_M_MinKSwing20 < 0.01
        F_M_Slope20 < 0.01
        Close_t-1 < F_M_MA130_t-1 + 2*F_M_Std130_t-1
        Close_t > F_M_MA130_t + 2*F_M_Std_t

        平仓条件:
        Close_t < F_M_MA130_t-1
        止损

        :param bar: 所有股票的某一个一分钟k线播放完后，处理信号
        :return:
        """
        if bar.time < 930 or bar.symbol == self.__hedge_index:
            return

        ##################################################################
        timestamp = datetime.strptime("{}{}".format(bar.date, str(bar.time).zfill(4)), '%Y%m%d%H%M')
        adj_factor = self.__current_adj.get(bar.symbol)
        timestamp1 = timestamp.timestamp()

        while timestamp1 < self.__F_M_MA130[self.__factor_row, 0]:
            self.__factor_row += 1

        bar_col = self.__stock_column_dict[bar.symbol] + 1
        ma130_value = self.__F_M_MA130[self.__factor_row, bar_col]
        std130_value = self.__F_M_Std130[self.__factor_row, bar_col]
        slop20_value = self.__F_M_Slope20[self.__factor_row, bar_col]
        mks20_value = self.__F_M_MinKSwing20[self.__factor_row, bar_col]

        close = adj_factor * bar.close
        #############################################
        if bar.symbol not in self.__pre_close.keys():
            self.__pre_F_MA130.update({bar.symbol: ma130_value})
            self.__pre_F_MinKSwing20.update({bar.symbol: mks20_value})
            self.__pre_F_Std130.update({bar.symbol: std130_value})
            self.__pre_F_Slop20.update({bar.symbol: slop20_value})
            self.__pre_close.update({bar.symbol: close})
            return

        ###################################################################
        # 平仓逻辑
        ##############################################################
        available_sell = self.__postion_mngt.get_available_sell(bar.symbol)
        if available_sell >= 100:
            if close <= ma130_value:
                order = Order(bar.symbol, bar.high, int(available_sell/100)*100, Direction.SELL, OrdType.TWAP)
                self.insert_order(order=order)

        ###################################################################
        # 如果不在当日的股票池中则不做开仓交易
        ##############################################################
        if bar.symbol not in self.__current_stock_pool:
            return
        ##############################################################
        # 开仓逻辑
        ################################################################
        if mks20_value < PARAM1 and slop20_value < PARAM2 and self.__pre_close.get(
                    bar.symbol) < self.__pre_F_MA130.get(bar.symbol) + PARAM3 * self.__pre_F_Std130.get(
                bar.symbol) and close > ma130_value + PARAM3 * std130_value:
            ###################################################
            # 有开仓信号后计算该股票可以下的委托量
            #############################################
            stock_mv = self.__postion_mngt.get_market_value_by_symbol(bar.symbol)
            total_mv = self.__postion_mngt.get_market_value()
            allow_to_buy = min(total_mv * self.__single_stock_max_weight - stock_mv, 1000000)
            allow_to_buy = min(allow_to_buy, self.__postion_mngt.available_cash)
            allow_to_buy /= bar.close
            allow_to_buy = int(allow_to_buy / 100) * 100
            if allow_to_buy > 0:
                order = Order(bar.symbol, bar.close, allow_to_buy, Direction.BUY, OrdType.FOC)
                self.insert_order(order=order)

        ##################################
        # store value for next bar
        #################################
        self.__pre_F_MA130.update({bar.symbol: ma130_value})
        self.__pre_F_MinKSwing20.update({bar.symbol: mks20_value})
        self.__pre_F_Std130.update({bar.symbol: std130_value})
        self.__pre_F_Slop20.update({bar.symbol: slop20_value})
        self.__pre_close.update({bar.symbol: close})

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
    start = "20150101 09:30:00"
    end = "20171231 15:00:00"
    date_start = datetime.strptime(start, '%Y%m%d %H:%M:%S')
    date_end = datetime.strptime(end, '%Y%m%d %H:%M:%S')
    hedgeindex = "000300.SH"
    dealer = TradeMaker(int(start[0:8]), int(end[0:8]), hedgeindex)
    Test = AlphaTest()
    #stock_code_list = ["000858.SZ"]
    # stock_code_list = ["600000.SH", "601318.SH", "600030.SH", "601688.SH", "000001.SZ", "000002.SZ", "000858.SZ"]
    single_stock_max_weight = 0.01
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
    complete_stock_list = get_complete_stock_list()
    stock_weight = get_panel_daily_info(complete_stock_list, int(start[0:8]), int(end[0:8]), 'index_weight_' + 'hs300')
    dailyStockPool = {}
    for i_date in stock_weight.index:
        dailyStockPool.update({str(i_date): stock_weight.columns[stock_weight.loc[i_date, :] > 0]})
    all_stock = set([])
    for day_info in dailyStockPool.values():
        all_stock = all_stock.union(set(day_info))
    all_stock = list(all_stock)
    Test.set_config(trade_stock_list=all_stock,
                    daily_stock_pool=dailyStockPool,
                    hedge_index=hedgeindex, start_time=date_start, end_time=date_end,
                    single_stock_max_weight=single_stock_max_weight)

    replay_list = copy(all_stock)
    replay_list.append(hedgeindex)
    replayer = MDReplay()
    replayer.set_config(stock_list=replay_list, start_time=date_start,
                        end_time=date_end, data_type=3)   # 4是merged的, 3是逐个读取
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