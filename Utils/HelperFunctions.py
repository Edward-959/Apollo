"""
常用函数
"""
import numpy as np
import pandas as pd
import scipy.stats as sps
import datetime as dt
import DataAPI.DataToolkit as Dtk


def factor_distribution_calc(factor_value_df):
    factor_np = factor_value_df.values.flatten()
    factor_val = factor_np[np.isfinite(factor_np)]
    factor_min = np.nanmin(factor_val)
    factor_max = np.nanmax(factor_val)
    factor_mean = np.nanmean(factor_val)
    factor_median = np.nanmedian(factor_val)
    factor_std = np.nanstd(factor_val)
    factor_skewness = sps.skew(factor_val)
    factor_kurtosis = sps.kurtosis(factor_val)
    stat_item = ['Skewness', 'Kurtosis', 'Median', 'Mean', 'Max', 'Min', 'Std']
    factor_distribution = pd.DataFrame(
        [factor_skewness, factor_kurtosis, factor_median, factor_mean, factor_max, factor_min, factor_std],
        index=stat_item, columns=['factor_distribution'])
    return factor_distribution


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


def z_score_standardizer(value_df):
    factor_mean = value_df.mean(axis=1)
    factor_std = value_df.std(axis=1)
    value_df = value_df.sub(factor_mean, axis=0)
    value_df = value_df.div(factor_std, axis=0)
    return value_df


def make_one_hot(input_data):
    # 将输入的向量改造成one-hot矩阵
    # 例如：输入x=np.array([1, 2, 4])，shape是(3,)；输出一个(3,5)的矩阵，其中3和3对应，5对应的是max(x)+1
    # array([[0, 1, 0, 0, 0],
    #        [0, 0, 1, 0, 0],
    #        [0, 0, 0, 0, 1]])
    max_value = np.max(input_data) + 1
    result = (np.arange(max_value) == input_data[:, None]).astype(np.int)
    return result


def factor_neutralizer(factor_df, start_date, end_date, neutral_factor_set={'size', 'industry3'}):
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
            if j % 100 == 0:
                print("factor neutralizing, {} / {} days".format(j, list(factor_df.index).__len__()))
            y0 = factor_array[j]
            x1_0 = industry_array[i_line].astype(np.int)
            x1_0 = make_one_hot(x1_0)  # 构造one_hot矩阵
            x1_0 = x1_0[:, 1:]   # 去掉第0列，也就是去掉原来无行业的值
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
            x1_0 = x1_0[:, 1:]   # 去掉第0列，也就是去掉原来无行业的值
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
        print("neutralizing costs", t2 - t1)
        return neutralized_factor_df


def equally_wt_fast_nav(stock_dict_input, trading_date_list, deal_price_df, close_price_df, stock_cost_rate):
    """等权重组合；不考虑对冲；在调仓日，新股票全部买入、旧股票全部卖出
    stock_dict_input的key是调仓日，value是股票代码的list
    """
    deal_price_df.fillna(method='ffill')
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


def fast_long_short_nav(long_nav_series, short_nav_series):
    long_pct_chg_list = long_nav_series.diff()
    short_pct_chg_list = short_nav_series.diff()
    long_short_pct_chg = long_pct_chg_list - short_pct_chg_list
    long_short_pct_chg.iloc[0] = 0.0
    long_short_nav = np.cumsum(long_short_pct_chg) + 1
    long_short_nav = pd.Series(long_short_nav, index=long_short_nav.index)
    long_short_annualized_return = np.cumsum(long_short_nav.values)[-1] / (
                                   long_short_nav.__len__() / 244)
    return long_short_nav, long_short_annualized_return


def nav_series_annually_stat(nav_series, benchmark):
    """输入净值Series, index是日期（int），按年输出收益率"""
    index_date = list(nav_series.index)
    date_year_list = [i // 10000 for i in index_date]
    year_list = list(set(date_year_list))
    year_list.sort()
    year_begin_idx = {}  # 记录每年首日在index_date中的位置索引
    year_idx = 0
    year_begin_idx.update({year_list[year_idx]: 0})
    year_end_idx = {}  # 记录每年末日在index_date中的位置索引
    for j, i_date in enumerate(date_year_list):
        if date_year_list[j] > year_list[year_idx]:
            year_end_idx.update({year_list[year_idx]: j - 1})
            year_idx += 1
            year_begin_idx.update({year_list[year_idx]: j})
        if j == date_year_list.__len__() - 1:
            year_end_idx.update({year_list[year_idx]: j})
    year_dates_count = {}  # 记录回测期间每年的交易日天数
    for i_year in year_begin_idx.keys():
        year_dates_count.update({i_year: year_end_idx[i_year] - year_begin_idx[i_year] + 1})
    return_each_year = {}
    if year_dates_count[year_list[0]] < 30:  # 如果第1年的交易日小于30天，那么第1年的年化收益就没有计算的必要
        for j, i_year in enumerate(year_begin_idx.keys()):
            if j > 0:
                return_each_year.update(
                    {str(i_year) + "-" + benchmark: nav_series.iloc[year_end_idx[i_year]] -
                     nav_series.iloc[year_end_idx[i_year - 1]]})
    else:
        for j, i_year in enumerate(year_begin_idx.keys()):
            if j == 0:
                return_each_year.update(
                    {str(i_year) + "-" + benchmark: nav_series.iloc[year_end_idx[i_year]] -
                     nav_series.iloc[year_begin_idx[i_year]]})
            else:
                return_each_year.update(
                    {str(i_year) + "-" + benchmark: nav_series.iloc[year_end_idx[i_year]] -
                     nav_series.iloc[year_end_idx[i_year - 1]]})
    return return_each_year


def load_label(stock_list, start_date, end_date, label_type, holding_period, output_index_type='date_int'):
    valid_end_date = Dtk.get_n_days_off(end_date, holding_period + 2)[-1]
    if label_type == 'coda':
        price_df = Dtk.get_panel_daily_pv_df(stock_list, start_date, valid_end_date,
                                             pv_type='twp_coda', adj_type='FORWARD')
        ans_df = price_df.shift(-holding_period) / price_df - 1  # 计算收益率
    elif label_type == 'vwap':
        data_df_amt = Dtk.get_panel_daily_pv_df(stock_list, start_date, valid_end_date, pv_type='amt',
                                                adj_type='NONE')
        data_df_volume = Dtk.get_panel_daily_pv_df(stock_list, start_date, valid_end_date,
                                                   pv_type='volume', adj_type='NONE')
        data_vwap = data_df_amt / data_df_volume  # 计算vwap
        adj_df = Dtk.get_panel_daily_info(stock_list, start_date, end_date, 'adjfactor')
        data_vwap = data_vwap * adj_df  # 计算后复权的vwap
        ans_df = data_vwap.shift(-holding_period) / data_vwap - 1  # 计算收益率
    elif label_type == 'twap':
        price_df = Dtk.get_panel_daily_pv_df(stock_list, start_date, valid_end_date, pv_type='twap',
                                             adj_type='FORWARD')
        ans_df = price_df.shift(-holding_period) / price_df - 1  # 计算收益率
    elif label_type in ['twap_excess_300', 'twap_excess_500']:
        if label_type == 'twap_excess_300':
            benchmark = "000300.SH"
        else:
            benchmark = "000905.SH"
        price_df = Dtk.get_panel_daily_pv_df(stock_list, start_date, valid_end_date, pv_type='twap',
                                             adj_type='FORWARD')
        benchmark_price_df = Dtk.get_panel_daily_pv_df([benchmark], start_date, valid_end_date, pv_type='twap')
        return_rate_df = price_df.shift(-holding_period) / price_df - 1
        return_rate_benchmark_df = benchmark_price_df.shift(-holding_period) / benchmark_price_df - 1
        ans_df = return_rate_df.sub(return_rate_benchmark_df[benchmark], axis=0)
    elif label_type in ['twap_ir_300', 'twap_ir_500']:
        # information ratio; 因需要计算标准差, 故不建议holding_period太短
        if holding_period <= 2:
            raise Exception('A holding_period of {} is not enough for calculating std'.format(holding_period))
        if label_type == 'twap_ir_300':
            benchmark = "000300.SH"
        else:
            benchmark = "000905.SH"
        price_df = Dtk.get_panel_daily_pv_df(stock_list, start_date, valid_end_date, pv_type='twap',
                                             adj_type='FORWARD')
        benchmark_price_df = Dtk.get_panel_daily_pv_df([benchmark], start_date, valid_end_date, pv_type='twap')
        # 计算holding_period的超额收益率
        return_rate_hp_df = price_df.shift(-holding_period) / price_df - 1
        return_rate_benchmark_hp_df = benchmark_price_df.shift(-holding_period) / benchmark_price_df - 1
        excess_return_hp_df = return_rate_hp_df.sub(return_rate_benchmark_hp_df[benchmark], axis=0)
        # 计算1天的超额收益率，用于std计算需要
        return_rate_1d_df = price_df.shift(-1) / price_df - 1
        return_rate_benchmark_1d_df = benchmark_price_df.shift(-1) / benchmark_price_df - 1
        excess_return_1d_df = return_rate_1d_df.sub(return_rate_benchmark_1d_df[benchmark], axis=0)
        excess_return_1d_std = excess_return_1d_df.rolling(min_periods=holding_period, window=holding_period).std()
        # 计算information ratio
        ans_df = excess_return_hp_df / excess_return_1d_std.shift(-holding_period)
    else:
        raise TypeError
    if output_index_type == 'timestamp':
        # factor的index是timestamp, 而非8位数的日期，这里做转化，以便后续可比
        ans_df = Dtk.convert_df_index_type(ans_df, 'date_int', 'timestamp')
    return ans_df
