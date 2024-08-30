from Backtest.PositionManager import PositionManager
from Backtest.common import Trade, Direction, SecurityType, Order, OrdStatus
from DataAPI.DataToolkit import get_panel_daily_pv_df, get_complete_stock_list, get_trading_day, get_stock_latest_info
import pandas as pd
import math
import logging
import os
from Backtest.utils.const_defines import *
import numpy as np


class PortfolioBackTester:
    index_code_dict = {'index_500': '000905.SH', 'index_300': '000300.SH', 'index_50': '000016.SH'}

    def __init__(self, start_date, end_date, signal: dict = ..., group_test_need_hedge: bool = False, group_test_hedge_index="index_500",
                 cost_ratio: float = 0.004, fut_cost_ratio: float = 0.00026, margin_ratio: float = 0.2,
                 deal_price_type='twap'):
        """

        :param signal: list [date_list, signal_list] each signal in signal_list is DataFrame
        :param group_test_need_hedge:
        :param group_test_hedge_index:
        """
        self.__postion_mnger: PositionManager = PositionManager(300000000, cost_ratio, fut_cost_ratio, margin_ratio)
        self.__init_fund = 240000000
        self.__hedge_index_code = self.index_code_dict.get(group_test_hedge_index)
        self.__is_hedge = group_test_need_hedge
        # self.__pnl_chart_name = pnl_chart_name
        self.__signal = signal
        self.__start_date = start_date
        self.__end_date = end_date
        self.__multiplier = Const.CONTRACT_MULTIPLIER.get(self.__hedge_index_code)
        self.__deal_price_type = deal_price_type


    # def postion_mnger(self):
    #     return self.__postion_mnger

    def run_test_old(self):

        all_stock_list = get_complete_stock_list()
        delist_dates = get_stock_latest_info(all_stock_list, 'Delisting_date')
        all_stock_list.append(self.__hedge_index_code)
        start_date = int(min(self.__signal.keys()))
        end_date = int(max(self.__signal.keys()))
        trading_date_list = get_trading_day(start_date, end_date)
        pv_type_dict = {"coda": "twp_coda", "vwap": "vwap", "twap": "twap", "close": "close"}
        df_deal_price: pd.DataFrame = get_panel_daily_pv_df(all_stock_list, start_date, end_date,
                                                            pv_type=pv_type_dict[self.__deal_price_type])
        # df_cut: pd.DataFrame = get_panel_daily_pv_df(all_stock_list, start_date, end_date, pv_type="close_cut")
        df_close: pd.DataFrame = get_panel_daily_pv_df(all_stock_list, start_date, end_date, pv_type="close")
        # df_open: pd.DataFrame = get_panel_daily_pv_df(all_stock_list, start_date, end_date, pv_type="open")
        df_pre_close: pd.DataFrame = get_panel_daily_pv_df(all_stock_list, start_date, end_date, pv_type="pre_close")
        adjust_position_date_list = self.__signal.keys()
        for i_date in trading_date_list:
            self.__postion_mnger.on_new_day(i_date)
            if i_date not in adjust_position_date_list:
                position_stock_list = self.__postion_mnger.get_position().keys()
                close_price_list = df_close.loc[i_date, position_stock_list]
                for code, price in zip(position_stock_list, close_price_list):
                    if math.isnan(price):
                        print(code, price)
                    else:
                        self.__postion_mnger.get_position().get(code).cur_price = round(price, 2)
                continue
            signal = self.__signal.get(i_date)
            code_list = signal["Code"].tolist()
            weight_list = signal["Weight"].tolist()
            # cut_list = df_cut.loc[i_date, code_list].tolist()
            # open_price_list = df_open.loc[i_date, code_list].tolist()
            pre_close_price_list = df_pre_close.loc[i_date, code_list].tolist()
            ###############################################################
            # code_price_pair_list = [[code1, price1] ...[codeN, priceN]]
            # 计算新的股票列表中交易的量
            ##########################################################
            code_price_pair_list = list(filter(lambda _x: not math.isnan(_x[1]), zip(code_list, pre_close_price_list)))
            weight_price_pair_list = list(filter(lambda _x: not math.isnan(_x[1]), zip(weight_list, pre_close_price_list)))
            code_list = list(map(lambda _x: _x[0], code_price_pair_list))

            #################################################################################
            # 用最新的pre_close计算当天应交易的股票数量（前一天的take_balance已完成，故不会影响前一天收盘市值）
            position_stock_list = self.__postion_mnger.get_position().keys()
            preclose_price_list = df_pre_close.loc[i_date, position_stock_list].tolist()
            for code, price in zip(position_stock_list, preclose_price_list):
                if not math.isnan(price):
                    self.__postion_mnger.get_position().get(code).cur_price = round(price, 2)
            stock_mv = self.__postion_mnger.get_market_value()
            vol_list = list(map(lambda _x: 100*(int((stock_mv*_x[0])/_x[1]/100)), weight_price_pair_list))
            ####################################################################################

            target_close_price_array = np.array(df_close.loc[i_date, code_list])
            self.__init_fund = sum(np.array(vol_list) * target_close_price_array)
            # pre_init_fund = self.__init_fund

            # cash = self.__postion_mnger.get_stock_available_cash()
            # print(cash)
            # if cash > 200000000.0 or self.__init_fund < 200000000:
            #     a=1
            #     pass

            cur_pos_list = list(map(self.__postion_mnger.get_hold_position, code_list))
            trade_vol_list = list(map(lambda x: x[0]-x[1], zip(vol_list, cur_pos_list)))
            ################################################################
            # 调仓: 当日新的股票清单出来后, 减去现有持仓,
            # 如果它们之间的差值为正则买入, 为负则卖出,为零则不作任何操作
            # 成交价格为deal_price
            ##################################################################
            deal_price_list = df_deal_price.loc[i_date, code_list].tolist()
            for code, vol, price in zip(code_list, trade_vol_list, deal_price_list):
                if vol > 0:
                    direction = Direction.BUY
                elif vol < 0:
                    direction = Direction.SELL
                else:
                    continue
                if math.isnan(price):
                    #  如果没有行情表示停牌或者无法交易
                    continue
                # 这里trade的1450并非真实交易时间，只是要初始化Trade这个类不得不给个时间
                # 实际上我们没有用到撮合模块，而是在前面已经决定了成交的价格
                trade = Trade(code, 1450, -1, round(price, 2), abs(vol), direction)
                order = Order(code, round(price, 2), abs(vol), direction)
                order.status = OrdStatus.NEW
                self.__postion_mnger.on_order_update(order)
                trade.turnover = trade.quantity * trade.price
                self.__postion_mnger.on_trade(trade)
            ####################################################################
            # 对于昨日持仓，当日没有信号的股票则全部平仓
            # 成交价格为deal_price
            ###################################################################
            code_list = signal["Code"].tolist()
            close_position_list = set(self.__postion_mnger.get_position().keys()).difference(set(code_list))
            # cur_pos_list2 = list(map(self.__postion_mnger.get_hold_position, close_position_list))
            if self.__hedge_index_code in close_position_list:
                close_position_list.remove(self.__hedge_index_code)
            close_price_list: list = df_deal_price.loc[i_date, close_position_list].tolist()
            for code, price in zip(close_position_list, close_price_list):
                if math.isnan(price):
                    continue
                trade = Trade(code, 1450, -1, round(price, 2), self.__postion_mnger.get_available_sell(code),
                              Direction.SELL)
                order = Order(code, round(price, 2), self.__postion_mnger.get_available_sell(code), Direction.SELL)
                order.status = OrdStatus.NEW
                self.__postion_mnger.on_order_update(order)
                trade.turnover = trade.price * trade.quantity
                self.__postion_mnger.on_trade(trade)
            ##################################################################
            # 如果暴露超过了一张期货合约的价值则进行对冲
            #####################################################################
            position_stock_list = self.__postion_mnger.get_position().keys()
            close_price_list = df_close.loc[i_date, position_stock_list]
            for code, price in zip(position_stock_list, close_price_list):
                if math.isnan(price):
                    # 如果股票退市则清理掉该股票的持仓, 以退市时的价格卖出
                    if code in delist_dates.keys() and delist_dates.get(code) <= i_date:
                        pos = self.__postion_mnger.get_position().get(code)
                        if pos.position > 0:
                            last_price = pos.cur_price
                            clear_vol = pos.position
                            trade = Trade(code, 1450, -1, round(last_price, 2), clear_vol, Direction.SELL)
                            order = Order(code, round(last_price, 2), clear_vol, Direction.SELL)
                            order.status = OrdStatus.NEW
                            self.__postion_mnger.on_order_update(order)
                            trade.turnover = trade.price * trade.quantity
                            self.__postion_mnger.on_trade(trade)
                else:
                    self.__postion_mnger.get_position().get(code).cur_price = round(price, 2)

            if self.__is_hedge:
                index_price = df_deal_price.loc[i_date, self.__hedge_index_code]
                if not math.isnan(index_price):
                    self.__on_hedge(index_price)
                    self.__postion_mnger.get_position().get(self.__hedge_index_code).cur_price = df_close.loc[
                        i_date, self.__hedge_index_code]

            #####################################################################
            # 以收盘价进行清算
            ######################################################################
        self.__postion_mnger.balance()
        # self.__show_pnl_line()

    # revised on 2019/02/21 在原来的基础上考虑了当日停牌股票的影响
    # revised on 2019/02/26 增加对一字涨跌停的判断，限制相关交易
    def run_test_old1(self):
        all_stock_list = get_complete_stock_list()
        delist_dates = get_stock_latest_info(all_stock_list, 'Delisting_date')
        all_stock_list.append(self.__hedge_index_code)
        start_date = int(min(self.__signal.keys()))
        end_date = int(max(self.__signal.keys()))
        trading_date_list = get_trading_day(start_date, end_date)
        pv_type_dict = {"coda": "twp_coda", "vwap": "vwap", "twap": "twap", "close": "close"}
        df_deal_price: pd.DataFrame = get_panel_daily_pv_df(all_stock_list, start_date, end_date,
                                                            pv_type=pv_type_dict[self.__deal_price_type])
        # df_cut: pd.DataFrame = get_panel_daily_pv_df(all_stock_list, start_date, end_date, pv_type="close_cut")
        df_close: pd.DataFrame = get_panel_daily_pv_df(all_stock_list, start_date, end_date, pv_type="close")
        # df_open: pd.DataFrame = get_panel_daily_pv_df(all_stock_list, start_date, end_date, pv_type="open")
        df_pre_close: pd.DataFrame = get_panel_daily_pv_df(all_stock_list, start_date, end_date, pv_type="pre_close")
        df_high: pd.DataFrame = get_panel_daily_pv_df(all_stock_list, start_date, end_date, pv_type="high")
        df_low: pd.DataFrame = get_panel_daily_pv_df(all_stock_list, start_date, end_date, pv_type="low")
        adjust_position_date_list = self.__signal.keys()
        for i_date in trading_date_list:
            self.__postion_mnger.on_new_day(i_date)
            if i_date not in adjust_position_date_list:
                position_stock_list = self.__postion_mnger.get_position().keys()
                close_price_list = df_close.loc[i_date, position_stock_list]
                for code, price in zip(position_stock_list, close_price_list):
                    if math.isnan(price):
                        print(code, price)
                    else:
                        self.__postion_mnger.get_position().get(code).cur_price = round(price, 2)
                continue
            signal = self.__signal.get(i_date)
            code_list = signal["Code"].tolist()
            weight_list = signal["Weight"].tolist()
            # cut_list = df_cut.loc[i_date, code_list].tolist()
            # open_price_list = df_open.loc[i_date, code_list].tolist()
            pre_close_price_list = df_pre_close.loc[i_date, code_list].tolist()
            deal_price_list = df_deal_price.loc[i_date, code_list].tolist()
            ###############################################################
            # code_price_pair_list = [[code1, price1] ...[codeN, priceN]]
            # 计算新的股票列表中交易的量
            ##########################################################
            # code_price_pair_list = list(filter(lambda _x: not math.isnan(_x[1]), zip(code_list, pre_close_price_list)))
            # weight_price_pair_list = list(filter(lambda _x: not math.isnan(_x[1]), zip(weight_list, pre_close_price_list)))
            # code_list = list(map(lambda _x: _x[0], code_price_pair_list))
            # 当日可交易的股票及权重
            pair_list = list(filter(lambda _x: not math.isnan(_x[3]), zip(code_list, weight_list, pre_close_price_list, deal_price_list)))
            weight_price_pair_list = list(zip(np.array(pair_list)[:, 1].astype(float), list(np.array(pair_list)[:, 2].astype(float))))
            cut_weight_list = list(np.array(pair_list)[:, 1].astype(float) / np.sum(np.array(pair_list)[:, 1].astype(float)))
            weight_price_pair_list_cut = list(zip(cut_weight_list, list(np.array(pair_list)[:, 2].astype(float))))
            code_list = list(map(lambda _x: _x[0], pair_list))
            #################################################################################
            # 用最新的pre_close计算当天应交易的股票数量（前一天的take_balance已完成，故不会影响前一天收盘市值）
            position_stock_list = self.__postion_mnger.get_position().keys()
            preclose_price_list = df_pre_close.loc[i_date, position_stock_list].tolist()
            position_deal_price = df_deal_price.loc[i_date, position_stock_list].tolist()
            frozen_stock_mv = 0
            for code, price, dealprice in zip(position_stock_list, preclose_price_list, position_deal_price):
                if not math.isnan(price):
                    self.__postion_mnger.get_position().get(code).cur_price = round(price, 2)
                    if math.isnan(dealprice):
                        frozen_stock_mv += self.__postion_mnger.get_position().get(code).position * round(price, 2)
            # 正常调仓时的数量
            stock_mv = self.__postion_mnger.get_market_value()
            vol_list = list(map(lambda _x: 100 * (int((stock_mv * _x[0]) / _x[1] / 100)), weight_price_pair_list))
            # 考虑停牌股票后重新分配权重的数量
            stock_mv_cut = self.__postion_mnger.get_market_value() - frozen_stock_mv
            vol_list_cut = list(map(lambda _x: 100 * (int((stock_mv_cut * _x[0]) / _x[1] / 100)), weight_price_pair_list_cut))
            vol_list = list(map(lambda _x: min(_x[0], _x[1]), zip(vol_list, vol_list_cut)))
            ####################################################################################

            target_close_price_array = np.array(df_close.loc[i_date, code_list])
            self.__init_fund = sum(np.array(vol_list) * target_close_price_array)
            # pre_init_fund = self.__init_fund

            cur_pos_list = list(map(self.__postion_mnger.get_hold_position, code_list))
            trade_vol_list = list(map(lambda x: x[0] - x[1], zip(vol_list, cur_pos_list)))
            ################################################################
            # 调仓: 当日新的股票清单出来后, 减去现有持仓,
            # 如果它们之间的差值为正则买入, 为负则卖出,为零则不作任何操作
            # 成交价格为deal_price
            ##################################################################
            deal_price_list = df_deal_price.loc[i_date, code_list].tolist()
            # 一字板判断
            price_gap = df_high.loc[i_date, code_list] - df_low.loc[i_date, code_list]
            limit_up = ((price_gap == 0) & (df_high.loc[i_date, code_list] > df_pre_close.loc[i_date, code_list])).tolist()
            limit_down = ((price_gap == 0) & (df_low.loc[i_date, code_list] < df_pre_close.loc[i_date, code_list])).tolist()
            for code, vol, price, up, down in zip(code_list, trade_vol_list, deal_price_list, limit_up, limit_down):
                if vol > 0:
                    direction = Direction.BUY
                elif vol < 0:
                    direction = Direction.SELL
                else:
                    continue
                if math.isnan(price):
                    #  如果没有行情表示停牌或者无法交易
                    continue
                if direction == Direction.BUY and up:
                    # 一字涨停无法买入
                    continue
                if direction == Direction.SELL and down:
                    # 一字跌停无法卖出
                    continue
                # 这里trade的1450并非真实交易时间，只是要初始化Trade这个类不得不给个时间
                # 实际上我们没有用到撮合模块，而是在前面已经决定了成交的价格
                trade = Trade(code, 1450, -1, round(price, 2), abs(vol), direction)
                order = Order(code, round(price, 2), abs(vol), direction)
                order.status = OrdStatus.NEW
                self.__postion_mnger.on_order_update(order)
                trade.turnover = trade.quantity * trade.price
                self.__postion_mnger.on_trade(trade)
            ####################################################################
            # 对于昨日持仓，当日没有信号的股票则全部平仓
            # 成交价格为deal_price
            ###################################################################
            code_list = signal["Code"].tolist()
            close_position_list = set(self.__postion_mnger.get_position().keys()).difference(set(code_list))
            # cur_pos_list2 = list(map(self.__postion_mnger.get_hold_position, close_position_list))
            if self.__hedge_index_code in close_position_list:
                close_position_list.remove(self.__hedge_index_code)
            close_price_list: list = df_deal_price.loc[i_date, close_position_list].tolist()
            # 一字跌停判断
            price_gap = df_high.loc[i_date, close_position_list] - df_low.loc[i_date, close_position_list]
            limit_down = ((price_gap == 0) & (df_low.loc[i_date, close_position_list] < df_pre_close.loc[i_date, close_position_list])).tolist()
            for code, price, down in zip(close_position_list, close_price_list, limit_down):
                if math.isnan(price) or down:
                    # 停牌或一字跌停无法卖出
                    continue
                trade = Trade(code, 1450, -1, round(price, 2), self.__postion_mnger.get_available_sell(code),
                              Direction.SELL)
                order = Order(code, round(price, 2), self.__postion_mnger.get_available_sell(code), Direction.SELL)
                order.status = OrdStatus.NEW
                self.__postion_mnger.on_order_update(order)
                trade.turnover = trade.price * trade.quantity
                self.__postion_mnger.on_trade(trade)
            ##################################################################
            # 如果暴露超过了一张期货合约的价值则进行对冲
            #####################################################################
            position_stock_list = self.__postion_mnger.get_position().keys()
            close_price_list = df_close.loc[i_date, position_stock_list]
            for code, price in zip(position_stock_list, close_price_list):
                if math.isnan(price):
                    # 如果股票退市则清理掉该股票的持仓, 以退市时的价格卖出
                    if code in delist_dates.keys() and delist_dates.get(code) <= i_date:
                        pos = self.__postion_mnger.get_position().get(code)
                        if pos.position > 0:
                            last_price = pos.cur_price
                            clear_vol = pos.position
                            trade = Trade(code, 1450, -1, round(last_price, 2), clear_vol, Direction.SELL)
                            order = Order(code, round(last_price, 2), clear_vol, Direction.SELL)
                            order.status = OrdStatus.NEW
                            self.__postion_mnger.on_order_update(order)
                            trade.turnover = trade.price * trade.quantity
                            self.__postion_mnger.on_trade(trade)
                else:
                    self.__postion_mnger.get_position().get(code).cur_price = round(price, 2)

            if self.__is_hedge:
                index_price = df_deal_price.loc[i_date, self.__hedge_index_code]
                if not math.isnan(index_price):
                    self.__on_hedge(index_price)
                    self.__postion_mnger.get_position().get(self.__hedge_index_code).cur_price = df_close.loc[
                        i_date, self.__hedge_index_code]

            #####################################################################
            # 以收盘价进行清算
            ######################################################################
        self.__postion_mnger.balance()
        # self.__show_pnl_line()

    # revised on 2019/03/13 twap价格中考虑了盘中触及涨跌停的情况，相应对成交价格和比例进行了调整
    # 以twap价格下单不再只取小数点后两位
    def run_test(self):
        all_stock_list = get_complete_stock_list()
        delist_dates = get_stock_latest_info(all_stock_list, 'Delisting_date')
        all_stock_list.append(self.__hedge_index_code)
        start_date = int(min(self.__signal.keys()))
        end_date = int(max(self.__signal.keys()))
        if self.__start_date < start_date or self.__end_date > end_date:
            print("daily_stock_pool does not cover dates")
            exit()
        trading_date_list = get_trading_day(self.__start_date, self.__end_date)
        start_date = trading_date_list[0]
        end_date = trading_date_list[-1]
        pv_type_dict = {"coda": "twp_coda", "vwap": "vwap", "twap": "twap", "close": "close"}
        df_deal_price = get_panel_daily_pv_df(all_stock_list, start_date, end_date,
                                              pv_type=pv_type_dict[self.__deal_price_type])
        df_close = get_panel_daily_pv_df(all_stock_list, start_date, end_date, pv_type="close")
        df_pre_close = get_panel_daily_pv_df(all_stock_list, start_date, end_date, pv_type="pre_close")
        df_buy_twap = get_panel_daily_pv_df(all_stock_list, start_date, end_date, 'buy_' + self.__deal_price_type)
        df_buy_twap_fill_rate = get_panel_daily_pv_df(all_stock_list, start_date, end_date, 'buy_' +
                                                      self.__deal_price_type + '_fill_rate')
        df_sell_twap = get_panel_daily_pv_df(all_stock_list, start_date, end_date, 'sell_' + self.__deal_price_type)
        df_sell_twap_fill_rate = get_panel_daily_pv_df(all_stock_list, start_date, end_date, 'sell_' +
                                                       self.__deal_price_type + '_fill_rate')
        adjust_position_date_list = self.__signal.keys()
        for i_date in trading_date_list:
            self.__postion_mnger.on_new_day(i_date)
            if i_date not in adjust_position_date_list:
                position_stock_list = self.__postion_mnger.get_position().keys()
                close_price_list = df_close.loc[i_date, position_stock_list]
                for code, price in zip(position_stock_list, close_price_list):
                    if math.isnan(price):
                        print(code, price)
                    else:
                        self.__postion_mnger.get_position().get(code).cur_price = round(price, 2)
                continue
            signal = self.__signal.get(i_date)
            pair_df = signal.set_index('Code')
            pair_df['pre_close'] = df_pre_close.loc[i_date, pair_df.index]
            pair_df['deal_price'] = df_deal_price.loc[i_date, pair_df.index]
            pair_df['buy_twap'] = df_buy_twap.loc[i_date, pair_df.index]
            pair_df['buy_fill_rate'] = df_buy_twap_fill_rate.loc[i_date, pair_df.index]
            pair_df['sell_twap'] = df_sell_twap.loc[i_date, pair_df.index]
            pair_df['sell_fill_rate'] = df_sell_twap_fill_rate.loc[i_date, pair_df.index]
            ###############################################################
            # code_price_pair_list = [[code1, price1] ...[codeN, priceN]]
            # 计算新的股票列表中交易的量
            ##########################################################
            # 当日可交易的股票及权重
            pair_df.dropna(subset=['deal_price'], inplace=True)
            pair_df['cut_weight'] = pair_df['Weight'] / pair_df['Weight'].sum()
            code_list = pair_df.index.tolist()
            #################################################################################
            # 用最新的pre_close计算当天应交易的股票数量（前一天的take_balance已完成，故不会影响前一天收盘市值）
            position_stock_list = self.__postion_mnger.get_position().keys()
            preclose_price_list = df_pre_close.loc[i_date, position_stock_list].tolist()
            position_deal_price = df_deal_price.loc[i_date, position_stock_list].tolist()
            frozen_stock_mv = 0
            for code, price, dealprice in zip(position_stock_list, preclose_price_list, position_deal_price):
                if not math.isnan(price):
                    self.__postion_mnger.get_position().get(code).cur_price = round(price, 2)
                    if math.isnan(dealprice):
                        frozen_stock_mv += self.__postion_mnger.get_position().get(code).position * round(price, 2)
            # 正常调仓时的数量
            stock_mv = self.__postion_mnger.get_market_value()
            vol_raw = (stock_mv * pair_df['Weight'] / pair_df['pre_close'] / 100).astype(int) * 100
            # 考虑停牌股票后重新分配权重的数量
            stock_mv_cut = self.__postion_mnger.get_market_value() - frozen_stock_mv
            vol_cut = (stock_mv_cut * pair_df['cut_weight'] / pair_df['pre_close'] / 100).astype(int) * 100
            vol_target = np.minimum(vol_raw, vol_cut)
            ####################################################################################

            target_close_price_array = np.array(df_close.loc[i_date, code_list])
            self.__init_fund = sum(np.array(vol_target) * target_close_price_array)
            # pre_init_fund = self.__init_fund

            cur_pos = pd.Series(map(self.__postion_mnger.get_hold_position, code_list), index=code_list)
            pair_df['trade_vol'] = vol_target - cur_pos
            ################################################################
            # 调仓: 当日新的股票清单出来后, 减去现有持仓,
            # 如果它们之间的差值为正则买入, 为负则卖出,为零则不作任何操作
            # 成交价格为deal_price
            ##################################################################
            for code in pair_df.index:
                vol = pair_df.loc[code, 'trade_vol']
                buy_fill_rate = pair_df.loc[code, 'buy_fill_rate']
                sell_fill_rate = pair_df.loc[code, 'sell_fill_rate']
                # 买入时，buy_fill_rate>0排除了nan(停牌)和buy_fill_rate=0(一字涨停)的情况
                if vol > 0 and buy_fill_rate > 0:
                    direction = Direction.BUY
                    price = pair_df.loc[code, 'buy_twap']
                    fill_rate = buy_fill_rate
                elif vol < 0 and sell_fill_rate > 0:
                    direction = Direction.SELL
                    price = pair_df.loc[code, 'sell_twap']
                    fill_rate = sell_fill_rate
                else:
                    continue
                # 这里trade的1450并非真实交易时间，只是要初始化Trade这个类不得不给个时间
                # 实际上我们没有用到撮合模块，而是在前面已经决定了成交的价格
                vol = int(vol * fill_rate / 100) * 100
                trade = Trade(code, 1450, -1, price, abs(vol), direction)
                order = Order(code, price, abs(vol), direction)
                order.status = OrdStatus.NEW
                self.__postion_mnger.on_order_update(order)
                trade.turnover = trade.quantity * trade.price
                self.__postion_mnger.on_trade(trade)
            ####################################################################
            # 对于昨日持仓，当日没有信号的股票则全部平仓
            # 成交价格为deal_price
            ###################################################################
            code_list = signal["Code"].tolist()
            close_position_list = set(self.__postion_mnger.get_position().keys()).difference(set(code_list))
            if self.__hedge_index_code in close_position_list:
                close_position_list.remove(self.__hedge_index_code)
            close_pair_df = pd.DataFrame(index=close_position_list)
            close_pair_df['sell_twap'] = df_sell_twap.loc[i_date, close_position_list]
            close_pair_df['sell_fill_rate'] = df_sell_twap_fill_rate.loc[i_date, close_position_list]
            for code in close_pair_df.index:
                sell_fill_rate = close_pair_df.loc[code, 'sell_fill_rate']
                if sell_fill_rate > 0:
                    price = close_pair_df.loc[code, 'sell_twap']
                else:
                    continue
                sell_vol = int(self.__postion_mnger.get_available_sell(code) * sell_fill_rate / 100) * 100
                if self.__postion_mnger.get_available_sell(code) - sell_vol <= 100:
                    sell_vol = self.__postion_mnger.get_available_sell(code)
                trade = Trade(code, 1450, -1, price, sell_vol, Direction.SELL)
                order = Order(code, price, sell_vol, Direction.SELL)
                order.status = OrdStatus.NEW
                self.__postion_mnger.on_order_update(order)
                trade.turnover = trade.price * trade.quantity
                self.__postion_mnger.on_trade(trade)
            ##################################################################
            # 如果暴露超过了一张期货合约的价值则进行对冲
            #####################################################################
            position_stock_list = self.__postion_mnger.get_position().keys()
            close_price_list = df_close.loc[i_date, position_stock_list]
            for code, price in zip(position_stock_list, close_price_list):
                if math.isnan(price):
                    # 如果股票退市则清理掉该股票的持仓, 以退市时的价格卖出
                    if code in delist_dates.keys() and delist_dates.get(code) <= i_date:
                        pos = self.__postion_mnger.get_position().get(code)
                        if pos.position > 0:
                            last_price = pos.cur_price
                            clear_vol = pos.position
                            trade = Trade(code, 1450, -1, round(last_price, 2), clear_vol, Direction.SELL)
                            order = Order(code, round(last_price, 2), clear_vol, Direction.SELL)
                            order.status = OrdStatus.NEW
                            self.__postion_mnger.on_order_update(order)
                            trade.turnover = trade.price * trade.quantity
                            self.__postion_mnger.on_trade(trade)
                else:
                    self.__postion_mnger.get_position().get(code).cur_price = round(price, 2)

            if self.__is_hedge:
                index_price = df_deal_price.loc[i_date, self.__hedge_index_code]
                if not math.isnan(index_price):
                    self.__on_hedge(index_price)
                    self.__postion_mnger.get_position().get(self.__hedge_index_code).cur_price = df_close.loc[
                        i_date, self.__hedge_index_code]

            #####################################################################
            # 以收盘价进行清算
            ######################################################################
        self.__postion_mnger.balance()
        # self.__show_pnl_line()

    def __on_hedge(self,  index_price):
        portfolio_value = 0
        position_dict = self.__postion_mnger.get_position()
        fut_value = 0
        stock_value = 0

        for pos in position_dict.values():
            if pos.sec_type == SecurityType.FUT:
                portfolio_value += pos.position * pos.cur_price * self.__multiplier
                fut_value += pos.position * pos.cur_price * self.__multiplier
            else:
                portfolio_value += pos.position * pos.cur_price
                stock_value += pos.position * pos.cur_price
        one_index_value = index_price * self.__multiplier
        contracts = portfolio_value // one_index_value
        if contracts > 0:
            trade = Trade(self.__hedge_index_code, 1450, -1, index_price, contracts,
                          Direction.OPEN_SHORT, sec_type=SecurityType.FUT)
            trade.turnover = trade.price * trade.quantity * self.__multiplier
            order = Order(self.__hedge_index_code, index_price, contracts, Direction.OPEN_SHORT, security_type=SecurityType.FUT)
            order.status = OrdStatus.NEW
            self.__postion_mnger.on_order_update(order)
            self.__postion_mnger.on_trade(trade)
            logging.info("hedge: sell IF {} contracts".format(contracts))
        elif contracts < 0:
            trade = Trade(self.__hedge_index_code, 1450, -1, index_price, -contracts,
                          Direction.CLOSE_SHORT, sec_type=SecurityType.FUT)
            trade.turnover = trade.price * trade.quantity * self.__multiplier
            order = Order(self.__hedge_index_code, index_price, -contracts, Direction.CLOSE_SHORT, security_type=SecurityType.FUT)
            order.status = OrdStatus.NEW
            self.__postion_mnger.on_order_update(order)
            self.__postion_mnger.on_trade(trade)
            logging.info("hedge: buys IF {} contracts".format(contracts))

    @staticmethod
    def mkdir(i_path):
        folders = []
        while not os.path.isdir(i_path):
            i_path, suffix = os.path.split(i_path)
            folders.append(suffix)
        for folder in folders[::-1]:
            i_path = os.path.join(i_path, folder)
            os.mkdir(i_path)

    # def __show_pnl_line(self, auto_open: bool = False):
    #     import plotly
    #     import plotly.graph_objs as go
    #     pnl_data = self.__postion_mnger.get_pnl_data()
    #     x = ["D{}".format(_x[0]) for _x in pnl_data]
    #     y = [_x[1]/self.__init_fund for _x in pnl_data]
    #     import os.path
    #     parent_path = os.path.dirname(os.path.abspath(self.__pnl_chart_name))
    #     if not os.path.exists(parent_path):
    #         self.mkdir(parent_path)
    #     plotly.offline.plot({
    #         "data": [go.Scatter(x=x, y=y, line=dict(color='rgb(205, 12, 24)', width=4))],
    #         "layout": go.Layout(title="收益率曲线(初始资金{})".format(self.__postion_mnger.total_cash), xaxis=dict(title='日期'),
    #                             yaxis=dict(title='收益率%'))},
    #         filename=self.__pnl_chart_name, auto_open=auto_open)
    #     logging.info("Generated PNL Chart file: {}".format(self.__pnl_chart_name))

    def get_result(self):
        """

        :return: {"date": 日期序列
                "total_mv": 每日总市值(stock+cash)序列 ,
                "pnl_rate": 每日相对前一日的总市值收益率序列,
                "cash": 每日现金序列,
                "pnl": 每日收益金额序列
                }
        """
        pnl_data = self.__postion_mnger.get_pnl_data()
        return {"date": [_x[0] for _x in pnl_data],
                "net_value": [_x[19] for _x in pnl_data],
                "actual_cash_used": [_x[20] for _x in pnl_data],
                "total_mv": [_x[1] for _x in pnl_data],
                "pnl_rate": [_x[2] for _x in pnl_data],
                "stock_num": [_x[17] for _x in pnl_data],
                "turnover": [_x[18] for _x in pnl_data],
                "cash": [_x[3] for _x in pnl_data],
                "pnl": [_x[4] for _x in pnl_data],
                "daily_trade_fee": [_x[5] for _x in pnl_data],
                "acc_trade_fee": [_x[6] for _x in pnl_data],
                "stock_mv": [_x[7] for _x in pnl_data],
                "fut_available_cash": [_x[8] for _x in pnl_data],
                "fut_frozen_cash": [_x[9] for _x in pnl_data],
                "stock_fee": [_x[10] for _x in pnl_data],
                "fut_fee": [_x[11] for _x in pnl_data],
                "stock_buy_amount": [_x[12] for _x in pnl_data],
                "stock_sell_amount": [_x[13] for _x in pnl_data],
                "fut_buy_amount": [_x[14] for _x in pnl_data],
                "fut_sell_amount": [_x[15] for _x in pnl_data],
                "dividends": [_x[16] for _x in pnl_data],
                }

    @staticmethod
    def cal_max_dd(x: list):
        max_unit_value = x[0]
        max_dd = 0
        for i in range(1, len(x)):
            max_unit_value = max(x[i], max_unit_value)
            dd = x[i] / max_unit_value - 1
            max_dd = min(dd, max_dd)
        return max_dd


# if __name__ == "__main__":
#     import json
#     logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.INFO)
#     group_num = 10
#     result = {}
#     for i_group in range(10, group_num + 1):
#         logging.info("testing group {}".format(i_group))
#         store = pd.HDFStore("./signal/model1/group{}_signal.h5".format(i_group))
#         date_list = store.select("date")["date"].tolist()
#         sinal_list = []
#         for it_date in date_list:
#             sinal_list.append(store.select("data/D{}S".format(it_date)))
#         store.close()
#         portfolio_back_tester = PortfolioBackTester([date_list, sinal_list], False)
#         portfolio_back_tester.run_test()
#         result.update({"group{}".format(i_group): portfolio_back_tester.get_result()})
#         print(result.get('pnl'))
#     fp = open("result.json", 'w')
#     json.dump(result, fp)
