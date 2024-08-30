import DataAPI.DataToolkit as Dtk
import pandas as pd
import numpy as np
import datetime as dt
import statsmodels.api as sm
import cvxpy as cp
import pickle
import platform
from os import mkdir, path, environ
import os


if platform.system() == "Windows":  # 云桌面环境运行是Windows
    save_path = ""
elif os.system("nvidia-smi") == 0:
    save_path = "/app/data/" + "user" + "/AppolloResearch/"
    if not path.exists(save_path):
        mkdir(save_path)
else:
    user_id = environ['USER_ID']
    save_path = "/app/data/" + user_id + "/AppolloResearch/"
    if not path.exists(save_path):
        mkdir(save_path)


class Optimizer:
    __backward_days = 252  # 回测开始日期向前回滚天数，在估计中会用到
    __simu_times = 10000  # 因子协方差Eigenfactor风险调整中的monte-carlo模拟次数
    __simu_length = 1000  # 因子协方差Eigenfactor风险调整中的每次模拟生成样本数
    __para_eigen_adjust = 1.2  # 因子协方差Eigenfactor风险调整参数
    __structural_model_para = 1.05  # 特异风险矩阵估计结构化模型调整参数
    __bayesian_group_num = 10  # 特异风险矩阵估计中贝叶斯收缩调整分组数
    __shrinkage_para = 0.05  # 特异风险矩阵估计中贝叶斯收缩参数

    # risk_estimated表示风险矩阵是否已经计算，需要重新估算风险时，risk_estimated为False，且传入开始和结束日期参数
    # 建议将估算风险和组合优化分开进行，因为协方差估计过程中monte_carlo模拟耗时较长
    def __init__(self, start_date=None, end_date=None, risk_estimated=True):
        self.__risk_estimated = risk_estimated
        self.__start_date = start_date
        self.__end_date = end_date
        self.__stock_list = Dtk.get_complete_stock_list()
        self.__factor_return = None
        self.__stock_specific_return = None
        self.__factor_covariance = None
        self.__stock_specific_risk = None
        if not risk_estimated:
            self.__valid_start_date = Dtk.get_n_days_off(self.__start_date, -self.__backward_days - 2)[0]
            stock_close = Dtk.get_panel_daily_pv_df(self.__stock_list, self.__valid_start_date, self.__end_date,
                                                    pv_type='close', adj_type='FORWARD')
            stock_volume = Dtk.get_panel_daily_pv_df(self.__stock_list, self.__valid_start_date, self.__end_date,
                                                     pv_type='volume')
            self.__stock_return = (stock_close / stock_close.shift(1) - 1).mul(stock_volume).div(stock_volume).mul(100)
            self.__mkt_cap = Dtk.get_panel_daily_info(self.__stock_list, self.__valid_start_date, self.__end_date,
                                                      info_type='mkt_cap_ard')
            self.__barra_factors = {}
            industry = Dtk.get_panel_daily_info(self.__stock_list, self.__valid_start_date, self.__end_date,
                                                info_type='industry3')
            # self.__industry_cap = pd.DataFrame([])
            for i in range(1, 32):
                temp_industry = industry.clip_upper(0).copy()
                temp_industry[industry == i] = 1
                self.__barra_factors['industry'+str(i)] = temp_industry
                # self.__industry_cap['industry'+str(i)] = self.__mkt_cap[temp_industry == 1].sum(axis=1)
            for factor_name in ['Size', 'Beta', 'Momentum', 'ResidualVolatility', 'NonLinearSize', 'Value', 'Liquidity',
                                'EarningsYield', 'Growth', 'Leverage']:
                print(factor_name)
                temp_factor = Dtk.get_panel_daily_info(self.__stock_list, self.__valid_start_date, self.__end_date,
                                                       info_type=factor_name)
                temp_factor = self.outlier_filter(temp_factor)
                self.__barra_factors[factor_name] = self.standardize(temp_factor, self.__mkt_cap)

    # 对因子回归显著性进行检验，以及因子稳定性（自相关性）、VIF（多重共线性）
    def factor_significance_test(self):
        pass

    # 对每一期的股票收益，关于之前最近的因子暴露进行回归，得到每一期对应因子收益
    # 基于risk_universe进行样本筛选，并剔除停牌股票
    # 对于缺失值的处理：剔除未上市和停牌股票，其余缺失值赋0，这种处理比较粗糙
    # 这里行业市值是在进行股票剔除前就算好了，是否要放到剔除后，待商榷
    # 输出是一个DataFrame，index为int型的日期，columns为对应的因子名称
    def compute_barra_factor_return(self, country=True):
        estimation_universe = Dtk.get_panel_daily_info(self.__stock_list, self.__valid_start_date, self.__end_date,
                                                       info_type='risk_universe')
        stock_pct_chg = self.__stock_return.shift(-1)  # 为了因子日期和收益率日期对齐，方便取数据
        self.__stock_specific_return = self.__stock_return.copy()
        self.__stock_specific_return[:] = np.nan
        trading_days = Dtk.get_trading_day(self.__valid_start_date, self.__end_date)
        factor_return = pd.DataFrame([])
        print("start computing Barra factor return")
        t1 = dt.datetime.now()
        for date in trading_days:
            if date == trading_days[-1]:
                factor_return[date] = np.nan
                break
            factor_exposure = pd.DataFrame([])
            for factor in self.__barra_factors.keys():
                factor_exposure[factor] = self.__barra_factors[factor].loc[date, :]
            universe_daily = estimation_universe.loc[date]
            stock_return_series = stock_pct_chg.loc[date, universe_daily == 1].dropna()
            factor_exposure = factor_exposure.reindex(index=stock_return_series.index)
            daily_cap = self.__mkt_cap.loc[date, :].reindex(index=stock_return_series.index)
            factor_exposure.fillna(0, inplace=True)
            regression_weight = np.diag(np.sqrt(daily_cap))
            stock_return = np.array(stock_return_series)
            if country:
                factor_exposure['country'] = 1
                exposure_mat = np.array(factor_exposure)
                constrain_mat = np.eye(self.__barra_factors.__len__())
                industry_constrain = np.zeros(self.__barra_factors.__len__())
                for i in range(1, 32):
                    industry_cap = daily_cap[factor_exposure['industry'+str(i)] == 1].sum()
                    if i < 31:
                        industry_constrain[i-1] = industry_cap
                    else:
                        industry_constrain = industry_constrain / (-industry_cap)
                constrain_mat = np.insert(constrain_mat, 30, industry_constrain, axis=0)
                pure_factor_portfolio = constrain_mat.dot(np.linalg.inv(constrain_mat.T.dot(exposure_mat.T).dot(
                    regression_weight).dot(exposure_mat).dot(constrain_mat))).dot(constrain_mat.T).dot(
                    exposure_mat.T).dot(regression_weight)
            else:
                exposure_mat = np.array(factor_exposure)
                pure_factor_portfolio = np.linalg.inv(exposure_mat.T.dot(regression_weight).dot(exposure_mat)).dot(
                    exposure_mat.T).dot(regression_weight)
            factor_return[date] = pure_factor_portfolio.dot(stock_return)
            self.__stock_specific_return.loc[date, :] = stock_return_series - pd.Series(exposure_mat.dot(
                factor_return[date]), index=stock_return_series.index)
        factor_return.index = factor_exposure.columns
        factor_return = factor_return.T
        factor_return = factor_return.shift(1)  # 当期收益率对应的是前一期的因子
        self.__factor_return = factor_return
        self.__stock_specific_return = self.__stock_specific_return.shift(1)
        # factor_return.to_excel(save_path + "factor_return.xlsx")
        t2 = dt.datetime.now()
        print("Computing factor return costs", t2 - t1)
        return factor_return

    # 估计barra因子间协方差矩阵
    # 月度调整组合的情况中，Newey-West自相关调整这一步会乘上每月大致的交易天数21/22，日度情况下是否要处理？
    # Newey-West自相关调整中，相关研报是直接求协方差矩阵，barra原始模型里是分别求相关系数矩阵以及各个因子的标准差
    def estimate_factor_covariance(self, volatility_halflife, correlation_halflife, correlation_lag, vra_halflife):
        factor_return = self.__factor_return.copy()
        start_date_minus_1 = Dtk.get_n_days_off(self.__start_date, -2)[0]
        trading_days = Dtk.get_trading_day(start_date_minus_1, self.__end_date)
        # Newey-West自相关调整
        factor_corr = factor_return.ewm(halflife=correlation_halflife).corr()
        factor_volatility = factor_return.ewm(halflife=volatility_halflife).std()
        print("start running Newey-West correlation adjustment")
        t1 = dt.datetime.now()
        for delta in range(1, correlation_lag + 1):
            factor_return_delta = factor_return.shift(delta)
            corr_plus = pd.DataFrame([])
            corr_minus = pd.DataFrame([])
            for factor in factor_return.columns.tolist():
                corr_plus[factor] = factor_return_delta.ewm(halflife=correlation_halflife).corr(
                    factor_return[factor]).stack()
                corr_minus[factor] = factor_return.ewm(halflife=correlation_halflife).corr(
                    factor_return_delta[factor]).stack()
            factor_corr = factor_corr + (1 - delta / (correlation_lag + 1)) * (corr_plus + corr_minus)
        factor_covariance_nw = pd.DataFrame([])
        for date in trading_days:
            corr = factor_corr.loc[date]
            corr = corr.reindex(index=corr.columns)
            std = factor_volatility.loc[date]
            cov = np.dot(np.dot(np.diag(std), corr), np.diag(std))
            if factor_covariance_nw.empty:
                factor_covariance_nw = pd.DataFrame(cov, index=[[date]*corr.index.size, corr.index],
                                                    columns=corr.columns)
            else:
                factor_covariance_nw = factor_covariance_nw.append(
                    pd.DataFrame(cov, index=[[date]*corr.index.size, corr.index], columns=corr.columns))
        t2 = dt.datetime.now()
        print("Newey-West adjustment costs", t2 - t1)

        # 进行Eigenfactor风险调整
        factor_covariance_eigen = pd.DataFrame([])
        factor_volatility_eigen = pd.DataFrame([])
        print("start running Eigenfactor risk adjustment")
        t1 = dt.datetime.now()
        for date in trading_days:
            covariance_nw = factor_covariance_nw.loc[date]
            vals, U_0 = np.linalg.eig(covariance_nw)
            D_0 = np.diag(vals)
            # np.random.seed()
            bias = 0
            for i in range(self.__simu_times):
                b_m = np.dot(np.sqrt(D_0), np.random.randn(vals.size, self.__simu_length))
                r_m = np.dot(U_0, b_m)
                F_m = np.cov(r_m)
                vals_m, U_m = np.linalg.eig(F_m)
                D_m_est = np.dot(np.dot(U_m.T, covariance_nw), U_m)
                vals_m_est = np.diag(D_m_est)
                bias = bias + vals_m_est / vals_m
            bias = np.sqrt(bias / self.__simu_times)
            bias = self.__para_eigen_adjust * (bias - 1) + 1
            D_0_adjust = np.dot(np.diag(np.square(bias)), D_0)
            f_eigen = np.dot(np.dot(U_0, D_0_adjust), U_0.T)
            factor_volatility_eigen[date] = pd.Series(np.diag(f_eigen), index=factor_covariance_nw.columns)
            if factor_covariance_eigen.empty:
                factor_covariance_eigen = pd.DataFrame(f_eigen,
                                                       index=[[date]*covariance_nw.index.size, covariance_nw.index],
                                                       columns=factor_covariance_nw.columns)
            else:
                factor_covariance_eigen = factor_covariance_eigen.append(
                    pd.DataFrame(f_eigen, index=[[date]*covariance_nw.index.size, covariance_nw.index],
                                 columns=covariance_nw.columns))
        t2 = dt.datetime.now()
        print("Eigenfactor risk adjustment costs", t2 - t1)

        # 进行波动率偏误调整(Volatility Regime Adjustment)
        print("start running volatility Regime adjustment")
        t1 = dt.datetime.now()
        factor_volatility_eigen = np.sqrt(factor_volatility_eigen.T)
        standard_return = factor_return / factor_volatility_eigen.shift(1)
        bias = np.square(standard_return).mean(axis=1)
        regime_adjust = bias.ewm(halflife=vra_halflife).mean()
        factor_covariance = pd.DataFrame([])
        for date in trading_days:
            if date < self.__start_date:
                continue
            covariance_adjust = factor_covariance_eigen.loc[date] * regime_adjust[date]
            covariance_adjust.index = [[date]*covariance_adjust.shape[0], covariance_adjust.index]
            if factor_covariance.empty:
                factor_covariance = covariance_adjust
            else:
                factor_covariance = factor_covariance.append(covariance_adjust)
        t2 = dt.datetime.now()
        print("Volatility regime adjustment costs", t2 - t1)
        factor_covariance.to_csv(save_path + "factor_covariance.csv")
        self.__factor_covariance = factor_covariance
        return factor_covariance

    # 估计股票特异风险方差矩阵（对角阵）
    def estimate_stock_specific_risk(self, volatility_halflife, auto_corr_halflife, auto_corr_lag, vra_halflife):
        stock_specific_return = self.__stock_specific_return
        # Newey-West调整
        stock_specific_std = stock_specific_return.ewm(halflife=volatility_halflife).std()
        specific_corr = stock_specific_return.ewm(halflife=auto_corr_halflife).corr(stock_specific_return)
        for delta in range(1, auto_corr_lag+1):
            specific_corr = specific_corr + 2 * (1 - delta / (auto_corr_lag + 1)) * stock_specific_return.ewm(
                halflife=auto_corr_halflife).corr(stock_specific_return.shift(delta))
        stock_specific_volatility = np.sqrt(specific_corr) * stock_specific_std
        # 此处252为参数，未单独标出
        sigma_robust = (stock_specific_return.rolling(252, min_periods=1).quantile(0.75) -
                        stock_specific_return.rolling(252, min_periods=1).quantile(0.25)) / 1.35
        tail = np.abs(stock_specific_volatility/sigma_robust - 1)
        gamma = np.exp(1-tail).clip_lower(0).clip_upper(1)
        valid_start_date = Dtk.get_n_days_off(self.__start_date, -vra_halflife-2)[0]
        trading_days = Dtk.get_trading_day(valid_start_date, self.__end_date)
        bias = pd.Series([])
        volatility_bs = pd.DataFrame([], index=self.__stock_list)
        for date in trading_days:
            factor_exposure = pd.DataFrame([])
            for factor in self.__factor_return.columns.tolist():
                if factor == 'country':
                    factor_exposure['country'] = 1
                else:
                    factor_exposure[factor] = self.__barra_factors[factor].loc[date, :]
            # 结构化模型调整（structural model）
            y = np.log(stock_specific_volatility.loc[date][gamma.loc[date] == 1])
            X = factor_exposure[gamma.loc[date] == 1]
            X.fillna(0, inplace=True)  # 缺失值的处理
            wls_model = sm.WLS(y, X)
            result = wls_model.fit()
            factor_exposure.fillna(0, inplace=True)
            volatility_str = self.__structural_model_para * np.exp(factor_exposure.dot(result.params))
            volatility_str = gamma.loc[date] * stock_specific_volatility.loc[date] + (1 - gamma.loc[date]) * volatility_str
            # 贝叶斯收缩调整（Bayesian shrinkage）
            volatility_sort = volatility_str.sort_values().dropna()
            stock_cap = self.__mkt_cap.loc[date, volatility_sort.index]
            stock_num = volatility_sort.__len__()
            volatility_prior = volatility_sort.copy()
            volatility_std = volatility_sort.copy()
            for i in range(self.__bayesian_group_num):
                group_start = int(np.round(i / self.__bayesian_group_num * stock_num))
                group_end = int(np.round((i + 1) / self.__bayesian_group_num * stock_num))
                group_mean = (volatility_sort.iloc[group_start: group_end] * stock_cap.iloc[group_start: group_end]
                              ).sum() / stock_cap.iloc[group_start: group_end].sum()
                group_std = np.sqrt(np.square(volatility_sort.iloc[group_start: group_end] - group_mean).sum() /
                                    (group_end - group_start))
                volatility_prior.iloc[group_start: group_end] = group_mean
                volatility_std.iloc[group_start: group_end] = group_std
            intensity = self.__shrinkage_para * np.abs(volatility_sort - volatility_prior) / \
                        (volatility_std + self.__shrinkage_para * np.abs(volatility_sort - volatility_prior))
            volatility_bayesian = intensity * volatility_prior + (1 - intensity) * volatility_sort
            volatility_bs[date] = volatility_bayesian
            # 波动率偏误调整
            standard_return = stock_specific_return.shift(-1).loc[date] / volatility_bayesian
            bias[date] = (stock_cap * np.square(standard_return)).sum() / stock_cap.sum()
        volatility_bs = volatility_bs.T
        regime_adjust = np.sqrt(bias.ewm(halflife=vra_halflife).mean()).shift(1)
        specific_risk = volatility_bs.mul(regime_adjust, axis=0).loc[self.__start_date: self.__end_date]
        self.__stock_specific_risk = specific_risk
        specific_risk.to_csv(save_path + "specific_risk.csv")
        return specific_risk

    # 优化组合权重，如果前面因子协方差和特异风险矩阵的结果已经计算保存，则不需重复计算，这里直接读取即可
    # covariance_calculated表示如果上述因子协方差和特异风险矩阵已经计算，则直接读取，无需再进行计算
    # 输入的pickle文件建议采用的形式为dict格式，key为对应调仓日期的int，每日的存储数值格式为DataFrame，
    # 内容包括股票代码以及对应的预测值（股票代码可以直接作为index），理论上包括备选股票池中所有股票
    # hedge_index 基准指数，zz500或者hs300
    # max_stock_num 组合选入股票数量限制
    # max_single_weight 单股票权重上限
    # style_constrained 组合风格暴露限制，dict格式，如果key为'All'，则对所有风格限制；具体风格后的list对应上下界
    # min_industry_bias, max_industry_bias 组合行业权重偏离基准指数行业权重的限制
    # max_turnover 单次调整换手率限制，如果换手率作为约束项求解
    # return_predicted代表预测模型给出的预测值是否为收益率，默认为True，预测值为概率的取值为False
    # 2019/04/23 增加对于组合中股票相对于对标指数权重股的权重偏离约束，通过single_weight_bias实现
    #            single_weight_bias支持两种参数设定方式：1、list，例如[0.015, 0.5]，第1个数表示指数中超过该权重的股票需要
    #            在组合中约束，第2个数表示受到约束的股票在组合中的权重与其在指数中的权重偏离不超过该幅度（0.5表示50%）；
    #            2、float，例如0.01，表示组合中所有股票的权重相对于这些股票在对标指数中的权重偏离绝对值不超过该值，该方式
    #            下不建议设置max_single_weight
    def optimize_portfolio_weight(self, pickle_name, absolutePath, daily_stock_pool_name, start_date, end_date,
                                  hedge_index, industry_copied=None, max_stock_num=None, single_weight_bias=None,
                                  max_single_weight=None, style_constraint=None, industry_constraint=None,
                                  return_predicted=True, suspend_filtered=True, penalty_risk=1, penalty_cost=0.2):
        with open(absolutePath + pickle_name + ".pickle", 'rb') as f:
            prediction = pickle.load(f)
        if start_date < list(prediction.keys())[0] or end_date > list(prediction.keys())[-1]:
            print("signal does not cover dates")
            exit()

        if platform.system() == "Windows":  # 云桌面环境运行是Windows
            load_path = "S://Apollo//BarraRiskModel//"
        elif os.system("nvidia-smi") == 0:
            load_path = "/vipzrz/Apollo/BarraRiskModel/"
        else:
            load_path = "/app/data/666889/Apollo/BarraRiskModel/"
        index_style_exposure = pd.read_csv(load_path + hedge_index + "_mktstd_index_style_factor_exposure.csv",
                                           index_col=[0])
        index_industry_weight = pd.read_csv(load_path + hedge_index + "_industry_weight.csv", index_col=[0])
        factor_covariance = pd.read_csv(load_path + "factor_covariance.csv", index_col=[0, 1])
        stock_specific_risk = pd.read_csv(load_path + "specific_risk.csv", index_col=[0])

        start_date = Dtk.get_n_days_off(start_date, -2)[0]
        trading_day = Dtk.get_trading_day(start_date, end_date)
        # 如果已经存储的风险矩阵没有覆盖回测信号的日期，终止程序，提示重算
        if start_date < stock_specific_risk.index.tolist()[0] or end_date > stock_specific_risk.index.tolist()[-1]:
            print("pickle date range", start_date, "~", end_date, "is beyond risk date range, please recalculate risk")
            exit()

        if os.path.exists(absolutePath + 'daily_stock_pool_barra/' + daily_stock_pool_name + '.pickle'):
            print('exist daily_stock_pool_barra')
            with open(absolutePath + 'daily_stock_pool_barra/' + daily_stock_pool_name + '.pickle', 'rb') as f:
                daily_stock_pool = pickle.load(f)
            daily_stock_pool_date = np.sort(list(daily_stock_pool.keys()))
            if trading_day[0] > daily_stock_pool_date[-1]:
                last_weight = pd.DataFrame([])
                print('start date is after daily_stock_pool_date')
                exit()
            elif trading_day[0] < daily_stock_pool_date[0]:
                last_weight = pd.DataFrame([])
            else:
                last_weight = daily_stock_pool[trading_day[0]]
        else:
            last_weight = pd.DataFrame([])
            daily_stock_pool_date = None
            print('do not exist daily_stock_pool_barra')
            if not os.path.exists(absolutePath + 'daily_stock_pool_barra/'):
                os.mkdir(absolutePath + 'daily_stock_pool_barra/')
            daily_stock_pool = {}

        # volume用于判断股票当日是否停牌
        volume = Dtk.get_panel_daily_pv_df(self.__stock_list, start_date, end_date, pv_type='volume')
        mkt_cap = Dtk.get_panel_daily_info(self.__stock_list, start_date, end_date, info_type='mkt_cap_ard')
        barra_factors = {}
        industry = Dtk.get_panel_daily_info(self.__stock_list, start_date, end_date, info_type='industry3')
        index_weight = Dtk.get_panel_daily_info(self.__stock_list, start_date, end_date,
                                                info_type='index_weight_' + hedge_index)
        for i in range(1, 32):
            temp_industry = industry.clip_upper(0).copy()
            temp_industry[industry == i] = 1
            barra_factors['industry' + str(i)] = temp_industry
        for factor_name in ['Size', 'Beta', 'Momentum', 'ResidualVolatility', 'NonLinearSize', 'Value', 'Liquidity',
                            'EarningsYield', 'Growth', 'Leverage']:
            print("Loading", factor_name)
            temp_factor = Dtk.get_panel_daily_info(self.__stock_list, start_date, end_date, info_type=factor_name)
            temp_factor = self.outlier_filter(temp_factor)
            barra_factors[factor_name] = self.standardize(temp_factor, mkt_cap)

        for i_date in range(trading_day.__len__()-1):
            date = trading_day[i_date+1]
            print(date)
            if daily_stock_pool_date is not None and date in daily_stock_pool_date:
                daily_portfolio = daily_stock_pool[date]
            else:
                last_date = trading_day[i_date]
                code_list = prediction[date]['infer_result']['Code']
                predict_value = prediction[date]['infer_result']['predict']
                # 当日停牌股票不参与组合构建
                if suspend_filtered:
                    volume_daily = volume.loc[date, code_list]
                    code_list = code_list[volume_daily > 0]
                    predict_value = predict_value[volume_daily > 0]
                # 对特定行业进行全复制
                copied_stock_weight = pd.Series()
                if industry_copied is not None:
                    daily_index_weight = index_weight.loc[last_date, code_list]
                    daily_industry = industry.loc[last_date, code_list]
                    for industry_i in industry_copied:
                        industry_stock_weight = daily_index_weight.loc[daily_industry == industry_i]
                        industry_stock_weight = industry_stock_weight / industry_stock_weight.sum() * \
                                                index_industry_weight.loc[last_date, 'industry' + str(industry_i)]
                        copied_stock_weight = copied_stock_weight.append(industry_stock_weight).fillna(0)
                # 对于预测值非收益率的模型，先取出各行业预测排名前20%（最少4支）的股票
                if not return_predicted:
                    code_prediction = pd.Series(predict_value, index=code_list)
                    code_industry = industry.loc[last_date, code_list]
                    code_list_top = []
                    for i in range(1, 32):
                        temp_codes = code_prediction[code_industry == i].sort_values(ascending=False).index.tolist()
                        if industry_copied is not None and i in industry_copied:
                            code_list_top.extend(temp_codes)
                        else:
                            code_list_top.extend(temp_codes[:max(4, round(temp_codes.__len__()*0.2))])
                    code_list = np.array(code_list_top)
                factor_exposure = pd.DataFrame()
                for factor in barra_factors.keys():
                    factor_exposure[factor] = barra_factors[factor].loc[last_date, code_list]
                factor_exposure['country'] = 1
                factor_exposure.fillna(0, inplace=True)  # 因子值缺失以0代替
                daily_factor_covariance = factor_covariance.loc[last_date]
                daily_specific_risk = stock_specific_risk.loc[last_date, code_list]
                daily_specific_risk.fillna(daily_specific_risk.mean(), inplace=True)  # 个股特异风险缺失值以组合中个股特异风险均值代替
                weight = cp.Variable(code_list.shape[0])
                risk_penalty = cp.Parameter(nonneg=True)
                cost_penalty = cp.Parameter(nonneg=True)
                # 风险项
                # 组合风险由因子协方差矩阵和特异风险矩阵分别计算，降低运算复杂度，提升计算速度
                portfolio_exposure = np.array(factor_exposure).T * weight
                risk = cp.quad_form(portfolio_exposure, daily_factor_covariance) + \
                       cp.quad_form(weight, np.diag(np.square(daily_specific_risk)))
                # 调仓权重差异以及调仓换手成本项
                weight_clear = last_weight.reindex(index=list(set(last_weight.index)-set(code_list)))
                weight0 = last_weight.reindex(index=code_list).fillna(0)
                if weight0.empty:
                    turnover = 1
                elif weight_clear.empty:
                    turnover = cp.norm(weight - cp.reshape(weight0, weight.shape), 1)
                else:
                    turnover = cp.norm(weight_clear, 1) + cp.norm(weight - cp.reshape(weight0, weight.shape), 1)
                # 优化目标
                if return_predicted:
                    # 收益项
                    ret = 100 * weight.T * predict_value
                    obj = cp.Maximize(ret-risk_penalty * risk-cost_penalty * turnover)
                else:
                    obj = cp.Minimize(risk_penalty * risk + cost_penalty * turnover)
                # 约束条件
                constraints = [cp.sum(weight) == 1, weight >= 0]
                # 固定全复制行业的股票权重
                if industry_copied is not None:
                    stock_copied = 1 - copied_stock_weight.reindex(index=code_list).isnull().astype(int)
                    constraints += [cp.multiply(stock_copied, weight) == copied_stock_weight.reindex(index=code_list).fillna(0)]
                if max_stock_num is not None:
                    pass
                # 限制组合与对标指数个股的权重偏离
                if single_weight_bias is not None:
                    daily_index_weight = index_weight.loc[last_date, code_list]
                    if isinstance(single_weight_bias, list):
                        daily_index_weight_major = (daily_index_weight >= single_weight_bias[0]).astype(int)
                        constraints += [cp.multiply(weight, daily_index_weight_major) >= daily_index_weight *
                                        daily_index_weight_major * (1 - single_weight_bias[1]),
                                        cp.multiply(weight, daily_index_weight_major) <= daily_index_weight *
                                        daily_index_weight_major * (1 + single_weight_bias[1])]
                    else:
                        constraints += [cp.abs(weight - daily_index_weight) <= single_weight_bias]
                # 个股权重约束，支持对于个别行业股票单独设置个股最大权重
                # 其中具体行业以industry+行业数字格式表示，数字对应具体行业参见DataToolkit，如industry21为银行
                if max_single_weight is not None:
                    for name in max_single_weight.keys():
                        if name == 'normal':
                            max_weight = np.ones(weight.shape) * max_single_weight[name]
                        else:
                            max_weight[factor_exposure.loc[:, name] == 1] = max_single_weight[name]
                    if industry_copied is not None:
                        stock_to_optimize = copied_stock_weight.reindex(index=code_list).isnull().astype(int)
                        constraints += [cp.multiply(stock_to_optimize, weight) <= max_weight]
                    elif isinstance(single_weight_bias, list):
                        daily_index_weight = index_weight.loc[last_date, code_list]
                        daily_index_weight_minor = (daily_index_weight < single_weight_bias[0]).astype(int)
                        constraints += [cp.multiply(daily_index_weight_minor, weight) <= max_weight]
                    else:
                        constraints += [weight <= max_weight]
                # 行业约束
                if industry_constraint is not None:
                    industry_list = list(industry_constraint.keys())
                    if industry_list[0] == 'All':
                        constraints += [weight.T * factor_exposure.iloc[:, 0:31] - index_industry_weight.loc[
                            last_date] >= industry_constraint['All'][0]]
                        constraints += [weight.T * factor_exposure.iloc[:, 0:31] - index_industry_weight.loc[
                            last_date] <= industry_constraint['All'][1]]
                    else:
                        industry_low_limit = []
                        industry_high_limit = []
                        for industry_i in industry_constraint.keys():
                            industry_low_limit.append(industry_constraint[industry_i][0])
                            industry_high_limit.append(industry_constraint[industry_i][1])
                        industry_list = list(map(lambda _x: "industry" + str(_x), industry_list))
                        constraints += [weight.T * factor_exposure.loc[:, industry_list] - index_industry_weight.loc[
                            last_date, industry_list] >= industry_low_limit]
                        constraints += [weight.T * factor_exposure.loc[:, industry_list] - index_industry_weight.loc[
                            last_date, industry_list] <= industry_high_limit]
                # 风格约束
                if style_constraint is not None:
                    style_list = list(style_constraint.keys())
                    if style_list[0] == 'All':
                        constraints += [weight.T * factor_exposure.iloc[:, 31:-1] - index_style_exposure.loc[
                            last_date] >= style_constraint['All'][0]]
                        constraints += [weight.T * factor_exposure.iloc[:, 31:-1] - index_style_exposure.loc[
                            last_date] <= style_constraint['All'][1]]
                    else:
                        style_low_limit = []
                        style_high_limit = []
                        for style in style_constraint.keys():
                            style_low_limit.append(style_constraint[style][0])
                            style_high_limit.append(style_constraint[style][1])
                        constraints += [weight.T * factor_exposure.loc[:, style_list] - index_style_exposure.loc[
                            last_date, style_list] >= style_low_limit]
                        constraints += [weight.T * factor_exposure.loc[:, style_list] - index_style_exposure.loc[
                            last_date, style_list] <= style_high_limit]
                # 股票数量约束
                problem = cp.Problem(obj, constraints)
                # 此处可以循环对参数取值进行优化
                risk_penalty.value = penalty_risk
                cost_penalty.value = penalty_cost
                problem.solve(solver=cp.ECOS)
                # 这里采用了便捷但不准确的方式处理最大股票数量
                # 将求解后的权重按从大到小排序，超过最大股票数量的权重赋为0，并按之前的权重分配到保留的股票上
                if problem.status != 'optimal':
                    code_list = np.array(last_weight.index.tolist())
                    adjust_weight = np.array(last_weight.iloc[:, 0])
                    daily_portfolio = pd.DataFrame({'Code': code_list[adjust_weight > 0],
                                                    'Weight': adjust_weight[adjust_weight > 0]})
                    daily_stock_pool.update({date: daily_portfolio})
                    print('infeasible, use last weight')
                    continue
                raw_weight = weight.value.round(6)
                raw_weight[raw_weight.argsort()[:-max_stock_num]] = 0
                print("sum of weight of first", max_stock_num, "stocks:", raw_weight.sum())
                adjust_weight = raw_weight / raw_weight.sum()
                print('Num of stocks selected:', (adjust_weight > 0).sum())
                daily_portfolio = pd.DataFrame({'Code': code_list[adjust_weight > 0],
                                                'Weight': adjust_weight[adjust_weight > 0]})
            daily_stock_pool.update({date: daily_portfolio})
            last_weight = daily_portfolio.set_index('Code')
        with open(absolutePath + 'daily_stock_pool_barra/' + daily_stock_pool_name + '.pickle', 'wb') as f:
            pickle.dump(daily_stock_pool, f)
        return daily_stock_pool

    # 组合收益归因
    def portfolio_return_contribution(self):
        pass

    @staticmethod
    def outlier_filter(value_df):
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
    # 进行因子标准化，使得市值加权均值为0，等权标准差为1
    def standardize(value_df, mkt_cap=None):
        if mkt_cap is None:
            factor_mean = value_df.mean(axis=1)
        else:
            factor_mean = (value_df * mkt_cap).sum(axis=1) / mkt_cap.sum(axis=1)
        factor_std = value_df.std(axis=1)
        value_df = value_df.sub(factor_mean, axis=0)
        value_df = value_df.div(factor_std, axis=0)
        return value_df
