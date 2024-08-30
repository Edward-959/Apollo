import DataAPI.DataToolkit as Dtk
import DataAPI.FactorTestloader
import pandas as pd
import datetime as dt
import numpy as np
import os

class TopGroupNav:
    def __init__(self, start_date, end_date, save_path, is_day_factor=True, holding_period=3, group_num=20,
                 label_type='twap', universe='alpha_universe', neutral_factor_set={'size', 'industry3'},
                 outlier_filtering_method="MAD", stock_cost_rate=0,
                 factor_path='D:\\Apollo\\NeedUpdateFactors\\'):
        # 初始设置
        query_trade_date_list = Dtk.get_trading_day(start_date, end_date)
        self.is_day_factor = is_day_factor
        self.start_date_orignal = query_trade_date_list[0]
        if self.is_day_factor:
            self.start_date = Dtk.get_n_days_off(query_trade_date_list[0], -2)[0]
        else:
            self.start_date = query_trade_date_list[0]
        self.end_date = query_trade_date_list[-1]
        self.holding_period = holding_period
        self.label_type = label_type
        self.universe = universe
        self.group_num = group_num
        self.neutral_factor_set = neutral_factor_set
        self.stock_cost_rate = stock_cost_rate
        self.outlier_filter_method = outlier_filtering_method
        self.factor_path = factor_path  # 原始因子的路径
        self.save_path = save_path  # 保存top组收益的地方
        self.benchmark_list = ["000300.SH", "000905.SH", "MktMedian", "MktMean"]

        # 中间变量和最终结果
        self.complete_stock_list = Dtk.get_complete_stock_list()
        self.original_factor_data_df = None  # original factor dataframe -- won't modified it
        self.stock_universe_df = None
        self.label_data = None
        self.factor_test_report = {}
        self.factor_neutralized_test_report = {}
        self.group_nav_list = []  # 快速分组测试（不对冲）每组的回测净值
        self.group_total_annualized_return_dict = {}  # 快速分组测试（不对冲）每组的年化收益
        self.daily_return_median = pd.DataFrame()
        self.daily_return_mean = pd.DataFrame()
        # Top组相对基准的超额收益净值(nav)序列，有4个key: "000300.SH" , "000905.SH", "MktMean"和"MktMedian"
        self.top_group_hedge_nav_dict = {}
        self.top_group_excess_return_each_year = {}  # Top组相对基准每年的超额收益
        self.top_group_excess_return_each_month = {}  # Top组相对基准每月的超额收益
        self.top_group_monthly_winning_pct_stat = {}  # Top组相对基准超额收益月胜率统计，4个key
        self.top_group_daily_winning_pct_stat = {}  # Top组相对基准超额收益日胜率统计，4个key
        self.long_short_nav = None  # Long-short净值
        self.long_short_return_each_year = {}  # 每年Long-short的收益率
        self.report_timestamp = dt.datetime.now()
        self.factor_avg_turnover_rate = None  # 平均换手率
        self.factor_stat_output = {}  # 最终结果放在里面
        self.each_year_group_return_dict = {}  # 分组测试每年的收益率
        self.group_return_rank_coef = {}  # 分组测试收益率与组号的相关性

        self.old_factors_list = []  # 取所有原本的因子值矩阵
        self.old_factors_name = []
        self.clear_old_factors_list = []  # 去极值、标准化、中性化后的因子矩阵

    def load_new_factors(self):
        # 把需要更新的因子load进来
        name_list = os.listdir(self.factor_path)
        start_date_datetime = Dtk.convert_date_or_time_int_to_datetime(self.start_date)
        end_date_datetime = Dtk.convert_date_or_time_int_to_datetime(self.end_date)
        for ifactor_name in name_list:
            print('start loading' + ifactor_name)
            ifactor_name = ifactor_name.split('.h5')[0]
            self.old_factors_name.append(ifactor_name)
            self.old_factors_list.append(DataAPI.FactorTestloader.load_factor(ifactor_name, self.complete_stock_list,
                                                                              start_date_datetime, end_date_datetime,
                                                                              self.factor_path))

    def clear_old_factors(self):
        # 所有因子去除极值、标准化、中性化
        volume_df = Dtk.get_panel_daily_pv_df(self.complete_stock_list, self.start_date, self.end_date, "volume")
        volume_df = Dtk.convert_df_index_type(volume_df, 'date_int', 'timestamp')
        k = 0
        for origin_factor_data in self.old_factors_list:
            print('start clear' + str(k))
            k += 1
            factor_data = origin_factor_data * self.stock_universe_df / self.stock_universe_df
            factor_data = factor_data * volume_df / volume_df  # 将停牌股票的因子值置为nan
            factor_data = self.outlier_filter(factor_data, self.outlier_filter_method)  # 因子去除极值
            factor_data = self.z_score_standardizer(factor_data)  # 因子标准化
            factor_data_neutralized = self.factor_neutralizer(factor_data, self.start_date, self.end_date,
                                                              self.neutral_factor_set)  # 因子中性化
            self.clear_old_factors_list.append(factor_data_neutralized)

    def to_fators_nav_csv(self):
        # 生成hedged_nav的矩阵
        hedged_nav_df = pd.DataFrame()
        for i in range(self.clear_old_factors_list.__len__()):
            group_test_factor_data = self.clear_old_factors_list[i].reindex(self.old_factors_list[i].index)
            group_test_factor_data = group_test_factor_data.shift(1).iloc[1:]
            temp_series = self.get_topgroup_hedged_nav(group_num=self.group_num, test_factor=group_test_factor_data,
                                                       position_window=self.holding_period,
                                                       stock_cost_ratio=self.stock_cost_rate)
            hedged_nav_df[self.old_factors_name[i]] = temp_series
            print('start caculate' + str(i))
        hedged_nav_day_ret = hedged_nav_df - hedged_nav_df.shift(1)
        hedged_nav_df.to_csv(self.save_path + 'TopGroupNav' + '_' + str(self.universe) + '.csv')
        hedged_nav_day_ret.to_csv(self.save_path + 'TopGroupDayRet' + '_' + str(self.universe) + '.csv')

    def get_topgroup_hedged_nav(self, group_num=10, test_factor=..., position_window=1, stock_cost_ratio=0):
        test_factor2 = Dtk.convert_df_index_type(test_factor, 'timestamp', 'date_int')
        group_set = []
        for j, i_date in enumerate(list(test_factor2.index)):
            if j % position_window == 0:
                factor0 = test_factor2.loc[i_date]  # 在i_date这个交易日所有股票的因子值
                factor0 = factor0.sort_values()  # 从小到大排序
                factor0 = factor0.dropna()  # 去掉nan值
                num_stock = factor0.shape[0]  # factor0的长度
                stock_num_each_group = np.floor(num_stock / group_num)
                for i_group in range(group_num):
                    code_selected = list(factor0.index[int(stock_num_each_group * i_group):int(
                        stock_num_each_group * i_group + stock_num_each_group)])
                    if code_selected.__len__() > 0:
                        if group_set.__len__() < group_num:
                            group_set.append({i_date: code_selected})
                        else:
                            group_set[i_group].update({i_date: code_selected})
        # group_set是一个list, 长度等于group_num，其每个元素是一组的分组信息
        # group_set中的值（单组分组信息）是字典，字典的key是换仓日（例如20150105），value是对应日的股票列表
        deal_price_dict = {"coda": "twp_coda", "vwap": "vwap", "twap": "twap"}
        deal_price_type = deal_price_dict[self.label_type]
        deal_price_df = Dtk.get_panel_daily_pv_df(self.complete_stock_list, list(test_factor2.index)[0],
                                                  list(test_factor2.index)[-1],
                                                  deal_price_type, "FORWARD")  # 用这种价格撮合成交
        close_price_df = Dtk.get_panel_daily_pv_df(self.complete_stock_list, list(test_factor2.index)[0],
                                                   list(test_factor2.index)[-1], "close", "FORWARD")
        trading_day_list = list(test_factor2.index)

        group_nav_list = []
        for j, group in enumerate(group_set):
            if j == 0 or j == (group_set.__len__() - 1):
                temp_ans_list = self.equally_wt_fast_nav(group, trading_day_list, deal_price_df, close_price_df,
                                                         stock_cost_ratio)
                i_group_nav, i_group_annualized_return, factor_turnover_rate = temp_ans_list
                group_nav_list.append(i_group_nav)
            else:
                continue
        if group_nav_list[0].values[-1] > group_nav_list[-1].values[-1]:
            top_group_nav = group_nav_list[0]
            bottom_group_nav = group_nav_list[-1]
        else:
            top_group_nav = group_nav_list[-1]
            bottom_group_nav = group_nav_list[0]

        if self.universe == 'alpha_universe':
            hedge_index = '000905.SH'
        elif self.universe == 'index_800':
            hedge_index = '000300.SH'
        else:
            hedge_index = '000905.SH'
        hedged_nav_series, hedge_index_annualized_return, daily_excess_winning_pct = self.__fast_group_hedge_nav(
            top_group_nav, hedge_index)
        return hedged_nav_series

    ##############################################################################################################################################

    @staticmethod
    def outlier_filter(value_df, method="MAD"):
        if method == "3Std":
            factor_mean = value_df.mean(axis=1)
            factor_std = value_df.std(axis=1)
            upper_limit = factor_mean + 3 * factor_std
            lower_limit = factor_mean - 3 * factor_std
            upper_limit = upper_limit.fillna(method='ffill')
            lower_limit = lower_limit.fillna(method='ffill')
            value_df = value_df.clip_lower(lower_limit, axis='index')
            value_df = value_df.clip_upper(upper_limit, axis='index')
            return value_df
        else:
            # 在20160104和20160107这两天，全市场熔断，那么先删除全市场都是nan的数据
            factor_max = value_df.max(axis=1)
            factor_max = factor_max.dropna()
            value_df = value_df.reindex(factor_max.index)
            factor_median = value_df.median(axis=1)
            factor_deviation_from_median = value_df.sub(factor_median, axis=0)
            factor_mad = factor_deviation_from_median.abs().median(axis=1)
            lower_limit = factor_median - 3.14826 * factor_mad
            upper_limit = factor_median + 3.14826 * factor_mad
            lower_limit = lower_limit.fillna(method='ffill')
            upper_limit = upper_limit.fillna(method='ffill')
            value_df = value_df.clip_lower(lower_limit, axis='index')
            value_df = value_df.clip_upper(upper_limit, axis='index')
            return value_df

    @staticmethod
    def z_score_standardizer(value_df):
        factor_mean = value_df.mean(axis=1)
        factor_std = value_df.std(axis=1)
        value_df = value_df.sub(factor_mean, axis=0)
        value_df = value_df.div(factor_std, axis=0)
        return value_df

    @staticmethod
    def factor_neutralizer(factor_df, start_date, end_date, neutral_factor_set):
        def make_one_hot(input_data):
            max_value = np.max(input_data) + 1
            result = (np.arange(max_value) == input_data[:, None]).astype(np.int)
            return result

        if neutral_factor_set == {'size', 'industry3'} or neutral_factor_set == {'size', 'industry3', 'return20'}:
            pass
        else:
            neutralized_factor_df = factor_df
            return neutralized_factor_df
        valid_start_date = Dtk.get_n_days_off(start_date, -22)[0]
        stock_list = list(factor_df.columns)
        factor_date_list = list(factor_df.index)
        industry_df = Dtk.get_panel_daily_info(stock_list, valid_start_date, end_date, 'industry3', 'timestamp')
        industry_df = industry_df.shift(1)  # 日级别信息要取前一天的
        mkt_cap_ard_df = Dtk.get_panel_daily_info(stock_list, valid_start_date, end_date, 'mkt_cap_ard', 'timestamp')
        mkt_cap_ard_df = np.log(mkt_cap_ard_df)  # 对市值取对数
        mkt_cap_ard_df = mkt_cap_ard_df.shift(1)  # 日级别信息要取前一天的

        if neutral_factor_set == {'size', 'industry3'}:
            t1 = dt.datetime.now()
            factor_start_line = list(mkt_cap_ard_df.index).index(factor_df.index[0])
            end_line = mkt_cap_ard_df.__len__()
            repeat_lines_list = list(range(end_line))[factor_start_line: end_line]
            factor_array = factor_df.values
            industry_df2 = industry_df.fillna(0)  # 将行业的缺失值替换为0，以方便后续用np创造one_hot矩阵
            industry_array = industry_df2.values
            mkt_cap_ard_array = mkt_cap_ard_df.values
            stock_col_num = stock_list.__len__()
            residual_list = []
            residual_date_list = []
            for j, i_line in enumerate(repeat_lines_list):
                y0 = factor_array[j]
                x1_0 = industry_array[i_line].astype(np.int)
                x1_0 = make_one_hot(x1_0)  # 这个one_hot函数
                x1_0 = x1_0[:, 1:]  # 去掉第0列，也就是去掉原来无行业的值
                x2_0 = mkt_cap_ard_array[i_line]
                x2_0 = x2_0.reshape([stock_col_num, 1])
                x0 = np.hstack([x1_0, x2_0])
                y0_isnan = np.isnan(y0)
                x0_isnan = np.isnan(np.max(x0, axis=1))
                y0_isvalid = 1 - y0_isnan
                x0_isvalid = 1 - x0_isnan
                valid_rows = x0_isvalid * y0_isvalid
                valid_stock_list = [stock_list[i] for i in range(stock_list.__len__()) if valid_rows[i] == 1]
                x0_valid = x0[valid_rows == 1]
                y0_valid = y0[valid_rows == 1]
                ind_check = np.sum(x0_valid, axis=0)
                empty_industry = []
                for i_col in range(x1_0.shape[1]):
                    if ind_check[i_col] < 1:
                        empty_industry.append(i_col)
                if empty_industry.__len__() > 0:
                    x0_valid = np.delete(x0_valid, empty_industry, 1)
                if x0_valid.__len__() > 0:
                    b = np.linalg.inv(x0_valid.T.dot(x0_valid)).dot(x0_valid.T).dot(y0_valid)
                    residual = y0_valid - x0_valid.dot(b)  # 求残差
                    residual_dict = dict(zip(valid_stock_list, residual))  # 将残差与股票代码关联起来
                    residual_list.append(residual_dict)
                    residual_date_list.append(factor_date_list[j])
            # 将残差list一次性转变为DataFrame
            neutralized_factor_df = pd.DataFrame(residual_list, index=residual_date_list)
            t2 = dt.datetime.now()
            print("neutralizing costs", t2 - t1)
            return neutralized_factor_df
        elif neutral_factor_set == {'size', 'industry3', 'return20'}:
            close20_df = Dtk.get_panel_daily_pv_df(stock_list, valid_start_date, end_date, pv_type='close',
                                                   adj_type='FORWARD')
            ret20_df = close20_df.div(close20_df.shift(20)) - 1
            ret20_df = Dtk.convert_df_index_type(ret20_df, 'date_int', 'timestamp')
            t1 = dt.datetime.now()
            factor_start_line = list(mkt_cap_ard_df.index).index(factor_df.index[0])
            end_line = mkt_cap_ard_df.__len__()
            repeat_lines_list = list(range(end_line))[factor_start_line: end_line]
            factor_array = factor_df.values
            industry_df2 = industry_df.fillna(0)  # 将行业的缺失值替换为0，以方便后续用np创造one_hot矩阵
            industry_array = industry_df2.values
            mkt_cap_ard_array = mkt_cap_ard_df.values
            ret20_array = ret20_df.values
            stock_col_num = stock_list.__len__()
            residual_list = []
            residual_date_list = []

            for j, i_line in enumerate(repeat_lines_list):
                if j % 100 == 0:
                    print("factor neutralizing, {} / {} days".format(j, list(factor_df.index).__len__()))
                y0 = factor_array[j]
                x1_0 = industry_array[i_line].astype(np.int)
                x1_0 = make_one_hot(x1_0)  # 这个one_hot函数
                x1_0 = x1_0[:, 1:]  # 去掉第0列，也就是去掉原来无行业的值
                x2_0 = mkt_cap_ard_array[i_line]
                x2_0 = x2_0.reshape([stock_col_num, 1])
                x3_0 = ret20_array[i_line]
                x3_0 = x3_0.reshape([stock_col_num, 1])
                x0 = np.hstack([x1_0, x2_0, x3_0])
                y0_isnan = np.isnan(y0)
                x0_isnan = np.isnan(np.max(x0, axis=1))
                y0_isvalid = 1 - y0_isnan
                x0_isvalid = 1 - x0_isnan
                valid_rows = x0_isvalid * y0_isvalid
                valid_stock_list = [stock_list[i] for i in range(stock_list.__len__()) if valid_rows[i] == 1]
                x0_valid = x0[valid_rows == 1]
                y0_valid = y0[valid_rows == 1]
                ind_check = np.sum(x0_valid, axis=0)
                empty_industry = []
                for i_col in range(x1_0.shape[1]):
                    if ind_check[i_col] < 1:
                        empty_industry.append(i_col)
                if empty_industry.__len__() > 0:
                    x0_valid = np.delete(x0_valid, empty_industry, 1)
                if x0_valid.__len__() > 0:
                    b = np.linalg.inv(x0_valid.T.dot(x0_valid)).dot(x0_valid.T).dot(y0_valid)
                    residual = y0_valid - x0_valid.dot(b)  # 求残差
                    residual_dict = dict(zip(valid_stock_list, residual))  # 将残差与股票代码关联起来
                    residual_list.append(residual_dict)
                    residual_date_list.append(factor_date_list[j])
            # 将残差list一次性转变为DataFrame
            neutralized_factor_df = pd.DataFrame(residual_list, index=residual_date_list)
            t2 = dt.datetime.now()
            return neutralized_factor_df

    @staticmethod
    def equally_wt_fast_nav(stock_dict_input, trading_date_list, deal_price_df, close_price_df, stock_cost_rate):
        """等权重组合；不考虑对冲；在调仓日，新股票全部买入、旧股票全部卖出
        stock_dict_input的key是调仓日，value是股票代码的list
        """
        deal_price_df.fillna(method='ffill')  # 前值填充nan
        close_price_df.fillna(method='ffill')

        stock_column_dict = {}
        for index, stock_code in enumerate(deal_price_df.columns):
            stock_column_dict.update({stock_code: index})

        buy_array = np.zeros((deal_price_df.shape[0], deal_price_df.shape[1]))  # 买入股票数量的array
        sell_array = np.zeros((deal_price_df.shape[0], deal_price_df.shape[1]))  # 卖出股票数量的array
        hold_array = np.zeros((deal_price_df.shape[0], deal_price_df.shape[1]))  # 不调仓股票数量的array
        diff_array = np.zeros((deal_price_df.shape[0], deal_price_df.shape[1]))  # 换仓股票数量的array
        net_sell_array = np.zeros((deal_price_df.shape[0], deal_price_df.shape[1]))  # 净卖股票数量的array
        eod_holding_array = np.zeros((deal_price_df.shape[0], deal_price_df.shape[1]))  # 日末持仓股票数量的array

        last_buy_day_row = None
        for i_row, i_date in enumerate(trading_date_list):
            if i_date in stock_dict_input.keys():
                if last_buy_day_row is not None:
                    sell_array[i_row] = buy_array[last_buy_day_row]
                stock_list_to_buy_temp = stock_dict_input[i_date]
                weight_scalar = 1 / stock_list_to_buy_temp.__len__()
                for stock_code in stock_list_to_buy_temp:
                    # 这里假设不知道要买多少股票，而是按 【1亿元 * weight / twap价格】算出全天的可成交量
                    buy_array[i_row, stock_column_dict[stock_code]] = weight_scalar * 100000000 / deal_price_df.iat[
                        i_row, stock_column_dict[stock_code]]
                last_buy_day_row = i_row
                temp_diff_array = sell_array[i_row] - buy_array[i_row]  # 实际换仓的股票数量 （负为买入，正为卖出）
                temp_diff_array[np.isnan(temp_diff_array)] = 0
                diff_array[i_row] = temp_diff_array
                net_sell_array[i_row] = diff_array[i_row] * (diff_array[i_row] > 0)  # 净卖出的股票数量
                eod_holding_array[i_row] = buy_array[i_row]  # 日末持仓的股票数量
            else:
                if last_buy_day_row is not None:
                    hold_array[i_row] = buy_array[last_buy_day_row]  # 不调仓的股票数量
                    eod_holding_array[i_row] = hold_array[i_row]  # 日末持仓的股票数量

        buy_vol_df = pd.DataFrame(buy_array, deal_price_df.index, deal_price_df.columns)  # 买入股票数量的df
        hold_vol_df = pd.DataFrame(hold_array, deal_price_df.index, deal_price_df.columns)  # 不调仓股票数量的df
        sell_vol_df = pd.DataFrame(sell_array, deal_price_df.index, deal_price_df.columns)  # 卖出股票数量的df
        eod_holding_vol_df = pd.DataFrame(eod_holding_array, deal_price_df.index, deal_price_df.columns)
        net_sell_vol_df = pd.DataFrame(net_sell_array, deal_price_df.index, deal_price_df.columns)  # 净卖出股数

        hold_income_df = close_price_df - close_price_df.shift(1)
        hold_income_df = hold_income_df.fillna(0)  # 持仓股票的收益金额
        buy_income_df = close_price_df - deal_price_df  # 当天买入的股票的收益金额
        sell_income_df = deal_price_df - close_price_df.shift(1)
        sell_income_df = sell_income_df.fillna(0)  # 当天卖出股票的收益金额
        net_sell_amt_df = net_sell_vol_df * deal_price_df  # 当天净卖出的股票的交易额
        cost_df = net_sell_amt_df.mul(stock_cost_rate)  # 交易成本的金额 = 净卖出金额 * 交易成本费率
        eod_holding_amt_df = eod_holding_vol_df * close_price_df  # 日末持仓市值

        turnover_series = net_sell_amt_df.sum(axis=1) / eod_holding_amt_df.sum(axis=1).shift(1)
        avg_turnover_rate = np.nanmean(turnover_series)
        total_income_df = buy_vol_df * buy_income_df + hold_vol_df * hold_income_df + sell_vol_df * sell_income_df
        daily_income = total_income_df.sum(axis=1) - cost_df.sum(axis=1)
        cumsum_income = daily_income.cumsum() + 100000000
        nav = cumsum_income / 100000000
        annulized_return_rate = ((nav.values[-1]) - 1) / (nav.values.__len__() / 244)
        return nav, annulized_return_rate, avg_turnover_rate

    def __fast_group_hedge_nav(self, nav_series, hedge_index):
        pct_chg_list = nav_series.diff()
        pct_chg_list = pct_chg_list.replace({np.nan: 0})
        if hedge_index == "MktMedian":
            self.daily_return_median = self.daily_return_median.reindex(nav_series.index)
            hedge_index_pct_chg = self.daily_return_median
        elif hedge_index == "MktMean":
            self.daily_return_mean = self.daily_return_mean.reindex(nav_series.index)
            hedge_index_pct_chg = self.daily_return_mean
        else:
            hedge_index_close = Dtk.get_panel_daily_pv_df([hedge_index], self.start_date, self.end_date, 'close')
            hedge_index_pct_chg = hedge_index_close / hedge_index_close.shift(1) - 1
            hedge_index_pct_chg = hedge_index_pct_chg[hedge_index]
        hedge_index_pct_chg.iloc[0] = 0.0
        daily_alpha = pct_chg_list - hedge_index_pct_chg
        daily_alpha.iloc[0] = 0.0
        # 将每日超额收益降序排列，一般列首>0、列尾<=0，逐个循环，可计算月度超额收益>0的月数
        daily_alpha_descending_list = list(daily_alpha)
        daily_alpha_descending_list.sort(reverse=True)
        if daily_alpha_descending_list[0] > 0 >= daily_alpha_descending_list[-1]:
            for i in range(daily_alpha_descending_list.__len__()):
                if daily_alpha_descending_list[i] * daily_alpha_descending_list[i + 1] == 0:
                    break
            daily_alpha_winning_pct = (i + 1) / daily_alpha_descending_list.__len__()
        else:
            daily_alpha_winning_pct = 0
        hedged_nav = np.cumsum(daily_alpha) + 1
        hedged_nav = pd.Series(hedged_nav, index=nav_series.index)
        hedge_index_annualized_return = np.cumsum(hedge_index_pct_chg.values)[-1] / (
                hedge_index_pct_chg.__len__() / 244)
        return hedged_nav, hedge_index_annualized_return, daily_alpha_winning_pct

    def launch_test(self):
        self.load_new_factors()
        self.stock_universe_df = Dtk.get_panel_daily_info(self.complete_stock_list, self.start_date, self.end_date,
                                                          info_type=self.universe, output_index_type='timestamp')
        self.clear_old_factors()
        self.to_fators_nav_csv()



