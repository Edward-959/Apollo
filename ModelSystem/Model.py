# -*- coding: utf-8 -*-
"""
Created on 2018/8/9 16:00
@author: 006547
revised by 006566 on 2019/3/4
"""
import DataAPI.DataToolkit as Dtk
from DataAPI.FactorTestloader import *
import platform


class Model:
    def __init__(self, para_model, model_name, model_management):
        self.complete_stock_list = Dtk.get_complete_stock_list()
        self.para_model = para_model
        self.model_name = model_name
        self.model_management = model_management
        self.trading_day = model_management.trading_day

    def train(self, date):
        pass

    def infer(self, date):
        pass

    def load_label(self, start_date, end_date, label_type='twap', holding_period=1):
        if label_type == 'coda':
            valid_end_date = Dtk.get_n_days_off(end_date, holding_period + 2)[-1]
            data_df_coda = Dtk.get_panel_daily_pv_df(self.complete_stock_list, start_date, valid_end_date,
                                                     pv_type='twp_coda', adj_type='FORWARD')
            return_rate_df = data_df_coda.shift(-holding_period) / data_df_coda - 1  # 计算收益率
            # factor的index是timestamp, 而非8位数的日期，这里做转化，以便后续可比
            return_rate_df = Dtk.convert_df_index_type(return_rate_df, 'date_int', 'timestamp')
            return return_rate_df
        elif label_type == 'vwap':
            valid_end_date = Dtk.get_n_days_off(end_date, holding_period + 2)[-1]
            data_df_amt = Dtk.get_panel_daily_pv_df(self.complete_stock_list, start_date, valid_end_date,
                                                    pv_type='amt', adj_type='NONE')
            data_df_volume = Dtk.get_panel_daily_pv_df(self.complete_stock_list, start_date, valid_end_date,
                                                       pv_type='volume', adj_type='NONE')
            data_vwap = data_df_amt / data_df_volume  # 计算vwap
            adj_df = Dtk.get_panel_daily_info(self.complete_stock_list, start_date, end_date, 'adjfactor')
            data_vwap = data_vwap * adj_df  # 计算后复权的vwap
            return_rate_df = data_vwap.shift(-holding_period) / data_vwap - 1  # 计算收益率
            # factor的index是timestamp, 而非8位数的日期，这里做转化，以便后续可比
            return_rate_df = Dtk.convert_df_index_type(return_rate_df, 'date_int', 'timestamp')
            return return_rate_df
        elif label_type == 'twap':
            valid_end_date = Dtk.get_n_days_off(end_date, holding_period + 2)[-1]
            data_df_coda = Dtk.get_panel_daily_pv_df(self.complete_stock_list, start_date, valid_end_date,
                                                     pv_type='twap', adj_type='FORWARD')
            return_rate_df = data_df_coda.shift(-holding_period) / data_df_coda - 1  # 计算收益率
            # factor的index是timestamp, 而非8位数的日期，这里做转化，以便后续可比
            return_rate_df = Dtk.convert_df_index_type(return_rate_df, 'date_int', 'timestamp')
            return return_rate_df
        elif label_type == 'twap_excess_300':
            benchmark = "000300.SH"

            valid_end_date = Dtk.get_n_days_off(end_date, holding_period + 2)[-1]
            complete_stock_list = Dtk.get_complete_stock_list()
            price_df = Dtk.get_panel_daily_pv_df(complete_stock_list, start_date, valid_end_date,
                                                 pv_type='twap', adj_type='FORWARD')
            benchmark_price_df = Dtk.get_panel_daily_pv_df([benchmark], start_date, valid_end_date,
                                                           pv_type='twap')
            return_rate_df = price_df.shift(-holding_period) / price_df - 1
            return_rate_benchmark_df = benchmark_price_df.shift(-holding_period) / benchmark_price_df - 1
            return_rate_benchmark_df = pd.DataFrame(np.tile(return_rate_benchmark_df.values, [1, return_rate_df.shape[1]]), index=return_rate_df.index, columns=return_rate_df.columns)
            excess_return_df = return_rate_df - return_rate_benchmark_df
            excess_return_df = Dtk.convert_df_index_type(excess_return_df, 'date_int', 'timestamp')
            return excess_return_df

        elif label_type == 'classify_excess_300':
            benchmark = "000300.SH"

            valid_end_date = Dtk.get_n_days_off(end_date, holding_period + 2)[-1]
            complete_stock_list = Dtk.get_complete_stock_list()
            price_df = Dtk.get_panel_daily_pv_df(complete_stock_list, start_date, valid_end_date,
                                                 pv_type='twap', adj_type='FORWARD')
            benchmark_price_df = Dtk.get_panel_daily_pv_df([benchmark], start_date, valid_end_date,
                                                           pv_type='twap')
            return_rate_df = price_df.shift(-holding_period) / price_df - 1
            return_rate_benchmark_df = benchmark_price_df.shift(-holding_period) / benchmark_price_df - 1
            return_rate_benchmark_df = pd.DataFrame(np.tile(return_rate_benchmark_df.values, [1, return_rate_df.shape[1]]), index=return_rate_df.index, columns=return_rate_df.columns)
            excess_return_df = return_rate_df - return_rate_benchmark_df
            excess_return_df = Dtk.convert_df_index_type(excess_return_df, 'date_int', 'timestamp')
            excess_return_df2 = excess_return_df.copy()
            excess_return_df2[:] = np.nan
            excess_return_df2[excess_return_df > 0.005] = 1
            excess_return_df2[excess_return_df < -0.005] = 0
            return excess_return_df2

        elif label_type == 'twap_excess_500':
            benchmark = "000905.SH"

            valid_end_date = Dtk.get_n_days_off(end_date, holding_period + 2)[-1]
            complete_stock_list = Dtk.get_complete_stock_list()
            price_df = Dtk.get_panel_daily_pv_df(complete_stock_list, start_date, valid_end_date,
                                                 pv_type='twap', adj_type='FORWARD')
            benchmark_price_df = Dtk.get_panel_daily_pv_df([benchmark], start_date, valid_end_date,
                                                           pv_type='twap')
            return_rate_df = price_df.shift(-holding_period) / price_df - 1
            return_rate_benchmark_df = benchmark_price_df.shift(-holding_period) / benchmark_price_df - 1
            return_rate_benchmark_df = pd.DataFrame(np.tile(return_rate_benchmark_df.values, [1, return_rate_df.shape[1]]), index=return_rate_df.index, columns=return_rate_df.columns)
            excess_return_df = return_rate_df - return_rate_benchmark_df
            excess_return_df = Dtk.convert_df_index_type(excess_return_df, 'date_int', 'timestamp')
            return excess_return_df

        elif label_type == 'classify_excess_500':
            benchmark = "000905.SH"

            valid_end_date = Dtk.get_n_days_off(end_date, holding_period + 2)[-1]
            complete_stock_list = Dtk.get_complete_stock_list()
            price_df = Dtk.get_panel_daily_pv_df(complete_stock_list, start_date, valid_end_date,
                                                 pv_type='twap', adj_type='FORWARD')
            benchmark_price_df = Dtk.get_panel_daily_pv_df([benchmark], start_date, valid_end_date,
                                                           pv_type='twap')
            return_rate_df = price_df.shift(-holding_period) / price_df - 1
            return_rate_benchmark_df = benchmark_price_df.shift(-holding_period) / benchmark_price_df - 1
            return_rate_benchmark_df = pd.DataFrame(np.tile(return_rate_benchmark_df.values, [1, return_rate_df.shape[1]]), index=return_rate_df.index, columns=return_rate_df.columns)
            excess_return_df = return_rate_df - return_rate_benchmark_df
            excess_return_df = Dtk.convert_df_index_type(excess_return_df, 'date_int', 'timestamp')
            excess_return_df2 = excess_return_df.copy()
            excess_return_df2[:] = np.nan
            excess_return_df2[excess_return_df > 0.005] = 1
            excess_return_df2[excess_return_df < -0.005] = 0
            return excess_return_df2

        elif label_type == 'rank_twap':
            valid_end_date = Dtk.get_n_days_off(end_date, holding_period + 2)[-1]
            data_df_coda = Dtk.get_panel_daily_pv_df(self.complete_stock_list, start_date, valid_end_date,
                                                     pv_type='twap', adj_type='FORWARD')
            return_rate_df = data_df_coda.shift(-holding_period) / data_df_coda - 1  # 计算收益率
            return_rate_df = return_rate_df.rank(axis=1, pct=True)
            # factor的index是timestamp, 而非8位数的日期，这里做转化，以便后续可比
            return_rate_df = Dtk.convert_df_index_type(return_rate_df, 'date_int', 'timestamp')
            return return_rate_df

    def load_day_factor(self, start_date, end_date, outlier_filter=False, z_score_standardizer=False, neutralize=False, fill=False):
        if platform.system() == "Windows":  # 云桌面环境运行是Windows
            path = "S:\\Apollo\\AlphaFactors"
        elif os.system("nvidia-smi") == 0:
            path = "/vipzrz/Apollo/AlphaFactors/AlphaFactors/"
        else:
            path = "/app/data/666889/Apollo/AlphaFactors/AlphaFactors/"
        # alpha_universe_timestamp = Dtk.get_panel_daily_info(self.complete_stock_list, start_date,
        #                                                     end_date, self.para_model["universe"],
        #                                                     output_index_type='timestamp')
        original_day_factor_data_df = {}
        start_date_datetime = Dtk.convert_date_or_time_int_to_datetime(start_date)
        end_date_datetime = Dtk.convert_date_or_time_int_to_datetime(end_date)

        industry_df = Dtk.get_panel_daily_info(self.complete_stock_list, start_date, end_date, 'industry3')
        print('loading factor')
        for day_factor_name in self.para_model['test_day_factor']:
            print('loading ' + day_factor_name)
            temp_factor = load_factor(day_factor_name, self.complete_stock_list, start_date_datetime, end_date_datetime, path)
            # temp_factor = temp_factor * alpha_universe_timestamp / alpha_universe_timestamp
            volume_df = Dtk.get_panel_daily_pv_df(self.complete_stock_list, start_date, end_date, "volume")
            volume_df = Dtk.convert_df_index_type(volume_df, 'date_int', 'timestamp')
            temp_factor = temp_factor * volume_df / volume_df  # 将停牌股票的因子值置为nan

            if outlier_filter:
                temp_factor = self.outlier_filter(temp_factor)  # 因子去除极值
            if z_score_standardizer:
                temp_factor = self.z_score_standardizer(temp_factor)  # 因子标准化
            if neutralize:
                temp_factor = self.factor_neutralizer(temp_factor, start_date, end_date)
            if fill:
                temp_factor = self.fillna_with_industry_median(temp_factor, industry_df, start_date, end_date)
            original_day_factor_data_df.update({day_factor_name: temp_factor})
        return original_day_factor_data_df

    @staticmethod
    def outlier_filter(value_df):
        """
        本函数以MAD的方式去除极端值
        """
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
    def factor_neutralizer(factor_df, start_date, end_date):
        # 初版为了方便，只写了size和industry中性；后续应该开发一个能支持更多因子正交化的函数

        valid_start_date = Dtk.get_n_days_off(start_date, -2)[0]
        stock_list = list(factor_df.columns)
        industry_df = Dtk.get_panel_daily_info(stock_list, valid_start_date, end_date, 'industry3', 'timestamp')
        industry_df = industry_df.shift(1)  # 日级别信息要取前一天的
        mkt_cap_ard_df = Dtk.get_panel_daily_info(stock_list, valid_start_date, end_date, 'mkt_cap_ard', 'timestamp')
        mkt_cap_ard_df = np.log(mkt_cap_ard_df)  # 对市值取对数
        mkt_cap_ard_df = mkt_cap_ard_df.shift(1)  # 日级别信息要取前一天的
        residual_list = []
        residual_date_list = []
        for j, i_date in enumerate(list(factor_df.index)):
            if j % 50 == 0:
                print("factor neutralizing, {} / {} days".format(j, list(factor_df.index).__len__()))
            y = factor_df.loc[i_date]
            x = pd.get_dummies(industry_df.loc[i_date])  # 将1*N的行业信息变为N*31的虚拟变量矩阵（dataframe）
            x = x.join(mkt_cap_ard_df.loc[i_date], on=None, how='left')  # 加入市值信息
            y = y.dropna()  # 去na、使矩阵non-singular，后续才能做回归
            x = x.dropna()
            index_intersect = list(set(y.index).intersection(x.index))  # 要取x和y都有值的
            y = y.reindex(index_intersect)
            x = x.reindex(index_intersect)
            ind_checker = x.sum()  # 再次检查是否有行业缺失，如有的话删除这个dummy variable，以保证矩阵non-singular
            for i in range(ind_checker.__len__()):
                if ind_checker.iloc[i] == 0:
                    x = x.drop(ind_checker.index[i], axis=1)
            if index_intersect.__len__() > 0:
                b = np.linalg.inv(x.T.dot(x)).dot(x.T).dot(y)  # OLS 求回归系数，这里要用到求逆，有点慢
                residual = y - x.dot(b)  # 求残差
                residual_list.append(residual)  # 将残差保存下来
                residual_date_list.append(i_date)
        # 将残差list一次性转变为DataFrame
        neutralized_factor_df = pd.DataFrame(residual_list, index=residual_date_list)

        return neutralized_factor_df

    def fillna_with_industry_median(self, raw_factor_df, ind_df, start_date, end_date):
        # 用当天的行业中位数填充因子中遇到的nan值
        # 算法原理是：
        #     计算31个行业的因子值的中位数，构造31个矩阵，每个矩阵都先用行业中位数填充所有的nan值，再将非本行业的股票的值
        #     设为0；再将上述31个矩阵直接叠加；最后再 * ind_df / ind_df，使得没有行业的股票的值为nan.
        # univ_df = Dtk.convert_df_index_type(univ_df, 'date_int', 'timestamp')
        ind_df = Dtk.convert_df_index_type(ind_df, 'date_int', 'timestamp')
        factor_df_na_filled = raw_factor_df.copy()
        factor_df_na_filled[:] = 0
        for i in range(1, 32):
            i_industry_df = ind_df == i  # a matrix full of True/False
            # 如是本行业的、则值为因子值，如不是本行业的、值为NaN
            factor_ind_i = raw_factor_df * i_industry_df / i_industry_df
            # 计算本行业的因子值的中位数
            factor_ind_i_median = factor_ind_i.median(axis=1)
            # 将形状为(n, )的series的形状转为(n, 1)
            factor_ind_i_median_reshaped = np.reshape(factor_ind_i_median.values, (factor_ind_i_median.__len__(), 1))
            # 把本行业的中位数的series拓展为矩阵
            factor_ind_i_median_matrix = pd.DataFrame(
                np.tile(factor_ind_i_median_reshaped, [1, raw_factor_df.shape[1]]),
                index=raw_factor_df.index, columns=raw_factor_df.columns)
            # 用行业中位数填充所有的NaN
            factor_ind_i[np.isnan] = factor_ind_i_median_matrix
            # 如是本行业的、则值为因子值，如不是本行业的、值为NaN
            factor_ind_i = factor_ind_i * i_industry_df / i_industry_df
            # 如是本行业的、则值为因子值，如不是本行业的、值为0
            factor_ind_i = factor_ind_i.fillna(0)
            factor_df_na_filled = factor_df_na_filled + factor_ind_i
        # 再以industry_df过滤，若没有行业的股票值为nan
        factor_df_na_filled = factor_df_na_filled * ind_df / ind_df
        # 再以universe_df过滤，若universe不为1的股票值为nan
        # factor_df_na_filled = factor_df_na_filled * univ_df / univ_df
        volume_df = Dtk.get_panel_daily_pv_df(self.complete_stock_list, start_date, end_date, "volume")
        volume_df = Dtk.convert_df_index_type(volume_df, 'date_int', 'timestamp')
        factor_df_na_filled = factor_df_na_filled * volume_df / volume_df  # 将停牌股票的因子值置为nan
        return factor_df_na_filled
