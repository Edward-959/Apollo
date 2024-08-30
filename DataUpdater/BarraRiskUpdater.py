"""
用于barra风险模型相关估计结果的更新，每次只需要修改更新数据日期范围line24~25
输出结果包括：
1、factor_covariance: barra因子的协方差矩阵
2、stock_specific_risk: 根据股票残差收益率计算的特异风险
以上分别保存为一个csv文件
同时计算过程中会保存一些中间计算结果，如：
1、factor_return: barra因子收益率
2、stock_specific_return: 股票收益率剔除barra因子风险收益后的残差收益率
3、factor_volatility_eigen: 经过Newey-West和特征根调整后的因子标准差
4、stock_volatility_bs: 经过贝叶斯压缩调整后的股票波动率
注意：T日的factor_return是根据T-1日的factor_exopsure和T日的stock_return计算得到的，该数据T日收盘可用

2019/4/11附注：如需要重算，本脚本的start_date设为20100104为宜，重算后factor_return的起点是20090805
"""
import DataAPI.DataToolkit as Dtk
import pandas as pd
import numpy as np
import datetime as dt
import statsmodels.api as sm
import platform
import os


# 修改更新数据日期范围
start_date = 20100104
end_date = 20190410


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
    value_df = value_df.clip(lower=lower_limit, upper=upper_limit, axis='index')
    return value_df


# 进行因子标准化，使得市值加权均值为0，等权标准差为1
def standardize(value_df, mkt_capital=None):
    if mkt_capital is None:
        factor_mean = value_df.mean(axis=1)
    else:
        factor_mean = (value_df * mkt_capital).sum(axis=1) / mkt_capital.sum(axis=1)
    factor_std = value_df.std(axis=1)
    value_df = value_df.sub(factor_mean, axis=0)
    value_df = value_df.div(factor_std, axis=0)
    return value_df


if platform.system() == "Windows":  # 云桌面环境运行是Windows
    # save_path = "S:\Apollo\BarraRiskModel"
    save_path = "D:\ApolloTestData"
elif os.system("nvidia-smi") == 0:
    save_path = "/vipzrz/Apollo/BarraRiskModel"
else:
    save_path = "/app/data/666889/Apollo/BarraRiskModel"
if not os.path.exists(save_path):
    os.mkdir(save_path)


# 以下参数建议不要修改
country = True  # 计算barra因子时，是否考虑国家因子
# 因子协方差矩阵估计参数
fc_vola_halflife = 42
fc_corr_halflife = 200
fc_corr_lag = 1
fc_vra_halflife = 4
# 股票特异风险估计参数
sr_vola_halflife = 42
sr_auto_corr_halflife = 252
sr_auto_corr_lag = 0
sr_vra_halflife = 4
backward_days = 100  # 回测开始日期向前回滚天数
simu_times = 1000  # 因子协方差Eigenfactor风险调整中的monte-carlo模拟次数
simu_length = 1000  # 因子协方差Eigenfactor风险调整中的每次模拟生成样本数
para_eigen_adjust = 1.2  # 因子协方差Eigenfactor风险调整参数
structural_model_para = 1.05  # 特异风险矩阵估计结构化模型调整参数
bayesian_group_num = 10  # 特异风险矩阵估计中贝叶斯收缩调整分组数
shrinkage_para = 0.05  # 特异风险矩阵估计中贝叶斯收缩参数


stock_list = Dtk.get_complete_stock_list()
# 如果相关数据已经有历史值，则先读取
factor_return = None
stock_specific_return = None
factor_covariance = None
stock_specific_risk = None
factor_return_path = os.path.join(save_path, "factor_return.csv")
stock_specific_return_path = os.path.join(save_path, "stock_specific_return.csv")
factor_covariance_path = os.path.join(save_path, "factor_covariance.csv")
specific_risk_path = os.path.join(save_path, "specific_risk.csv")
if os.path.exists(factor_return_path):
    factor_return = pd.read_csv(factor_return_path, index_col=[0])
    print("factor_return's latest date is", factor_return.index[-1])
    if factor_return.index[-1] < Dtk.get_n_days_off(start_date, -2)[0]:
        print("days are missing between original dates and new start date")
        exit()
    factor_return.to_csv(os.path.join(save_path, "factor_return_old.csv"))
if os.path.exists(stock_specific_return_path):
    stock_specific_return = pd.read_csv(stock_specific_return_path, index_col=[0])
    print("stock_specific_return's latest date is", stock_specific_return.index[-1])
    stock_specific_return.to_csv(os.path.join(save_path, "stock_specific_return_old.csv"))
    # 按新的股票列表做一下变换
    stock_specific_return = stock_specific_return.reindex(columns=stock_list)
if os.path.exists(factor_covariance_path):
    factor_covariance = pd.read_csv(factor_covariance_path, index_col=[0, 1])
    print("factor_covariance's latest date is", factor_covariance.index[-1][0])
    factor_covariance.to_csv(os.path.join(save_path, "factor_covariance_old.csv"))
if os.path.exists(specific_risk_path):
    stock_specific_risk = pd.read_csv(specific_risk_path, index_col=[0])
    print("stock_specific_risk's latest date is", stock_specific_risk.index[-1])
    stock_specific_risk.to_csv(os.path.join(save_path, "specific_risk_old.csv"))
    stock_specific_risk = stock_specific_risk.reindex(columns=stock_list)

# 准备基础数据
valid_start_date = Dtk.get_n_days_off(start_date, -backward_days - 2)[0]
stock_close = Dtk.get_panel_daily_pv_df(stock_list, valid_start_date, end_date, pv_type='close', adj_type='FORWARD')
stock_volume = Dtk.get_panel_daily_pv_df(stock_list, valid_start_date, end_date, pv_type='volume')
stock_return = (stock_close / stock_close.shift(1) - 1).mul(stock_volume).div(stock_volume).mul(100)
mkt_cap = Dtk.get_panel_daily_info(stock_list, valid_start_date, end_date, info_type='mkt_cap_ard')
industry = Dtk.get_panel_daily_info(stock_list, valid_start_date, end_date, info_type='industry3')
estimation_universe = Dtk.get_panel_daily_info(stock_list, valid_start_date, end_date, info_type='risk_universe')
# 取行业信息和barra因子（去极值、标准化）
barra_factors = {}
for i in range(1, 32):
    temp_industry = industry.clip(upper=0).copy()
    temp_industry[industry == i] = 1
    barra_factors['industry' + str(i)] = temp_industry
barra_factor_list = ['Size', 'Beta', 'Momentum', 'ResidualVolatility', 'NonLinearSize', 'Value', 'Liquidity',
                     'EarningsYield', 'Growth', 'Leverage']
for factor_name in barra_factor_list:
    print("Loading factor", factor_name)
    temp_factor = Dtk.get_panel_daily_info(stock_list, valid_start_date, end_date, info_type=factor_name)
    temp_factor = outlier_filter(temp_factor)
    barra_factors[factor_name] = standardize(temp_factor, mkt_cap)


#################################################
# 计算barra因子收益率部分
# 对每一期的股票收益，关于之前最近的因子暴露进行回归，得到每一期对应因子收益
# 基于risk_universe进行样本筛选，并剔除停牌股票
# 对于缺失值的处理：剔除未上市和停牌股票，其余缺失值赋0，这种处理比较粗糙
# 这里行业市值是在进行股票剔除前就算好了，是否要放到剔除后，待商榷
# 输出是一个DataFrame，index为int型的日期，columns为对应的因子名称
#################################################
print("start computing Barra factor return")
t1 = dt.datetime.now()
if country:
    factor_list = list(barra_factors.keys()) + ['country']
else:
    factor_list = list(barra_factors.keys())
if factor_return is None:
    trading_days = Dtk.get_trading_day(valid_start_date, end_date)[1:]
    factor_return = pd.DataFrame(index=trading_days, columns=factor_list)
    stock_specific_return = pd.DataFrame(index=trading_days, columns=stock_list)
else:
    trading_days = Dtk.get_trading_day(start_date, end_date)
for j, date in enumerate(trading_days):
    if j in [50, 100] or j % 200 == 0:
        print("{}; {}/{} days calculated".format(date, j, trading_days.__len__()))
    factor_exposure = pd.DataFrame()
    stock_return_daily = stock_return.loc[date, (estimation_universe.loc[date] == 1)].dropna()
    # 取前一日的因子暴露
    for factor in barra_factors.keys():
        factor_value = barra_factors[factor].shift(1).loc[date, stock_return_daily.index]
        # 如果某行业当日未选出股票，剔除该行业
        if abs(factor_value).sum() > 0:
            factor_exposure[factor] = factor_value
    factor_exposure.fillna(0, inplace=True)
    cap_daily = mkt_cap.loc[date, stock_return_daily.index]
    regression_weight = np.diag(np.sqrt(cap_daily))
    if country:
        factor_exposure['country'] = 1
        exposure_mat = np.array(factor_exposure)
        constrain_mat = np.eye(factor_exposure.shape[1]-1)
        industry_constrain = np.zeros(factor_exposure.shape[1]-1)
        industry_cap = []
        # 将industry1(石油石化行业)表示成其他行业收益率的线性组合
        for industry in factor_exposure.columns:
            if industry[:8] == 'industry':
                industry_cap.append(cap_daily[factor_exposure[industry] == 1].sum())
        industry_constrain[:len(industry_cap)-1] = industry_cap[1:]
        industry_constrain /= -industry_cap[0]
        constrain_mat = np.insert(constrain_mat, 0, industry_constrain, axis=0)
        pure_factor_portfolio = constrain_mat.dot(np.linalg.inv(constrain_mat.T.dot(exposure_mat.T).dot(
            regression_weight).dot(exposure_mat).dot(constrain_mat))).dot(constrain_mat.T).dot(
            exposure_mat.T).dot(regression_weight)
    else:
        exposure_mat = np.array(factor_exposure)
        pure_factor_portfolio = np.linalg.inv(exposure_mat.T.dot(regression_weight).dot(exposure_mat)).dot(
            exposure_mat.T).dot(regression_weight)
    factor_return_daily = pd.Series(pure_factor_portfolio.dot(stock_return_daily), index=factor_exposure.columns)
    factor_return.loc[date] = factor_return_daily
    stock_specific_return.loc[date] = stock_return_daily - exposure_mat.dot(factor_return_daily)
factor_return.fillna(0, inplace=True)
t2 = dt.datetime.now()
print("computing factor return costs", t2 - t1)
# 保存新的结果
factor_return.to_csv(factor_return_path)
stock_specific_return.to_csv(stock_specific_return_path)


#############################################
# 估计barra因子间协方差矩阵
# 月度调整组合的情况中，Newey-West自相关调整这一步会乘上每月大致的交易天数21/22，日度情况下是否要处理？
# Newey-West自相关调整中，相关研报是直接求协方差矩阵，barra原始模型里是分别求相关系数矩阵以及各个因子的标准差
#############################################
print("start computing factor covariance")
t1 = dt.datetime.now()
# Newey-West自相关调整
factor_corr = factor_return.ewm(halflife=fc_corr_halflife).corr()
factor_volatility = factor_return.ewm(halflife=fc_vola_halflife).std()
for delta in range(1, fc_corr_lag + 1):
    factor_return_delta = factor_return.shift(delta)
    corr_plus = pd.DataFrame()
    corr_minus = pd.DataFrame()
    for factor in factor_return.columns.tolist():
        corr_plus[factor] = factor_return_delta.ewm(halflife=fc_corr_halflife).corr(
            factor_return[factor]).stack()
        corr_minus[factor] = factor_return.ewm(halflife=fc_corr_halflife).corr(
            factor_return_delta[factor]).stack()
    factor_corr = factor_corr + (1 - delta / (fc_corr_lag + 1)) * (corr_plus + corr_minus)
# 至少需要84个交易日长度的数据来计算corr和volatility
factor_corr.drop(factor_return.index[:84], inplace=True)
factor_volatility.drop(factor_return.index[:84], inplace=True)
factor_return.drop(factor_return.index[:84], inplace=True)
# 获取之前已经保存的、经过Newey-West自相关调整和Eigenfactor风险调整的因子波动率
factor_volatility_eigen_path = os.path.join(save_path, "factor_volatility_eigen.csv")
if os.path.exists(factor_volatility_eigen_path):
    factor_volatility_eigen = pd.read_csv(factor_volatility_eigen_path, index_col=[0])
    factor_volatility_eigen.to_csv(os.path.join(save_path, "factor_volatility_eigen_old.csv"))
    trading_days = Dtk.get_trading_day(start_date, end_date)
else:
    factor_volatility_eigen = pd.DataFrame(index=factor_return.index, columns=factor_list)
    trading_days = factor_return.index.tolist()
for j, date in enumerate(trading_days):
    if j in [50, 100] or j % 200 == 0:
        print("{}; {}/{} days calculated".format(date, j, trading_days.__len__()))
    corr = factor_corr.loc[date]
    corr = corr.reindex(index=factor_list)
    std = factor_volatility.loc[date]
    cov_nw = np.diag(std).dot(corr).dot(np.diag(std))
    # 进行Eigenfactor风险调整
    vals, U_0 = np.linalg.eig(cov_nw)
    D_0 = np.diag(vals)
    bias = 0
    for i in range(simu_times):
        b_m = np.dot(np.sqrt(D_0), np.random.randn(vals.size, simu_length))
        r_m = np.dot(U_0, b_m)
        F_m = np.cov(r_m)
        vals_m, U_m = np.linalg.eig(F_m)
        D_m_est = U_m.T.dot(cov_nw).dot(U_m)
        vals_m_est = np.diag(D_m_est)
        bias = bias + vals_m_est / vals_m
    bias = np.sqrt(bias / simu_times)
    bias = para_eigen_adjust * (bias - 1) + 1
    D_0_adjust = np.dot(np.diag(np.square(bias)), D_0)
    cov_eigen = U_0.dot(D_0_adjust).dot(U_0.T)
    factor_volatility_eigen.loc[date] = np.sqrt(np.diag(cov_eigen))
    # 进行波动率偏误调整(Volatility Regime Adjustment)
    if date < start_date:
        continue
    standard_return = factor_return.loc[:date] / factor_volatility_eigen.shift(1).loc[:date]
    standard_return.dropna(inplace=True)
    bias = np.square(standard_return).mean(axis=1)
    regime_adjust = bias.ewm(halflife=fc_vra_halflife).mean()
    covariance_adjust = cov_eigen * regime_adjust.iloc[-1]
    if factor_covariance is None:
        factor_covariance = pd.DataFrame(covariance_adjust, index=[[date] * len(factor_list), factor_list],
                                         columns=factor_list)
    elif date in factor_covariance.index.levels[0]:
        factor_covariance.loc[date] = pd.DataFrame(covariance_adjust, index=[[date] * len(factor_list), factor_list],
                                                   columns=factor_list)
    else:
        factor_covariance = factor_covariance.append(
            pd.DataFrame(covariance_adjust, index=[[date] * len(factor_list), factor_list], columns=factor_list))
t2 = dt.datetime.now()
print("computing factor covariance costs", t2 - t1)
factor_volatility_eigen.to_csv(factor_volatility_eigen_path)
factor_covariance.to_csv(factor_covariance_path)


###############################################
# 估计股票特异风险方差矩阵（对角阵）
###############################################
print("start computing stock specific risk")
t1 = dt.datetime.now()
# Newey-West调整
stock_specific_std = stock_specific_return.ewm(halflife=sr_vola_halflife).std()
specific_corr = stock_specific_return.ewm(halflife=sr_auto_corr_halflife).corr(stock_specific_return)
for delta in range(1, sr_auto_corr_lag+1):
    specific_corr = specific_corr + 2 * (1 - delta / (sr_auto_corr_lag + 1)) * stock_specific_return.ewm(
        halflife=sr_auto_corr_halflife).corr(stock_specific_return.shift(delta))
stock_specific_volatility = np.sqrt(specific_corr) * stock_specific_std
# 此处252为参数，未单独标出
sigma_robust = (stock_specific_return.rolling(252, min_periods=1).quantile(0.75) -
                stock_specific_return.rolling(252, min_periods=1).quantile(0.25)) / 1.35
# 至少需要84个交易日长度的数据来计算volatility
stock_specific_volatility.drop(stock_specific_return.index[:84], inplace=True)
sigma_robust.drop(stock_specific_return.index[:84], inplace=True)
stock_specific_return.drop(stock_specific_return.index[:84], inplace=True)
tail = np.abs(stock_specific_volatility/sigma_robust - 1)
gamma = np.exp(1-tail).clip(lower=0, upper=1)
stock_volatility_bs_path = os.path.join(save_path, "stock_volatility_bs.csv")
if os.path.exists(stock_volatility_bs_path):
    stock_volatility_bs = pd.read_csv(stock_volatility_bs_path, index_col=[0])
    stock_volatility_bs.to_csv(os.path.join(save_path, "stock_volatility_bs_old.csv"))
    trading_days = Dtk.get_trading_day(start_date, end_date)
else:
    stock_volatility_bs = pd.DataFrame(index=stock_specific_return.index, columns=stock_list)
    trading_days = stock_specific_return.index.tolist()
for j, date in enumerate(trading_days):
    if j in [50, 100] or j % 200 == 0:
        print("{}; {}/{} days calculated".format(date, j, trading_days.__len__()))
    factor_exposure = pd.DataFrame()
    for factor in factor_return.columns.tolist():
        if factor == 'country':
            factor_exposure['country'] = 1
        else:
            factor_exposure[factor] = barra_factors[factor].loc[date, :]
    # 结构化模型调整（structural model）
    y = np.log(stock_specific_volatility.loc[date][gamma.loc[date] == 1])
    gamma_daily = gamma.loc[date]
    X = factor_exposure.loc[gamma_daily[gamma_daily == 1].index]
    X.fillna(0, inplace=True)  # 缺失值的处理
    wls_model = sm.WLS(y, X)
    result = wls_model.fit()
    factor_exposure.fillna(0, inplace=True)
    volatility_str = structural_model_para * np.exp(factor_exposure.dot(result.params))
    volatility_str = gamma.loc[date] * stock_specific_volatility.loc[date] + (1 - gamma.loc[date]) * volatility_str
    # 贝叶斯收缩调整（Bayesian shrinkage）
    volatility_sort = volatility_str.sort_values().dropna()
    stock_cap = mkt_cap.loc[date, volatility_sort.index]
    stock_num = volatility_sort.__len__()
    volatility_prior = volatility_sort.copy()
    volatility_std = volatility_sort.copy()
    for i in range(bayesian_group_num):
        group_start = int(np.round(i / bayesian_group_num * stock_num))
        group_end = int(np.round((i + 1) / bayesian_group_num * stock_num))
        group_mean = (volatility_sort.iloc[group_start: group_end] * stock_cap.iloc[group_start: group_end]
                      ).sum() / stock_cap.iloc[group_start: group_end].sum()
        group_std = np.sqrt(np.square(volatility_sort.iloc[group_start: group_end] - group_mean).sum() /
                            (group_end - group_start))
        volatility_prior.iloc[group_start: group_end] = group_mean
        volatility_std.iloc[group_start: group_end] = group_std
    intensity = shrinkage_para * np.abs(volatility_sort - volatility_prior) / (
            volatility_std + shrinkage_para * np.abs(volatility_sort - volatility_prior))
    volatility_bayesian = intensity * volatility_prior + (1 - intensity) * volatility_sort
    stock_volatility_bs.loc[date] = volatility_bayesian
# 波动率偏误调整
standard_return = stock_specific_return / stock_volatility_bs.shift(1)
bias = (mkt_cap * np.square(standard_return)).sum(axis=1) / mkt_cap.sum(axis=1)
regime_adjust = np.sqrt(bias.ewm(halflife=sr_vra_halflife).mean())
if stock_specific_risk is None:
    stock_specific_risk = stock_volatility_bs.mul(regime_adjust, axis=0).loc[start_date:end_date]
else:
    stock_specific_risk = pd.concat([stock_specific_risk.loc[:start_date-1],
                                    stock_volatility_bs.mul(regime_adjust, axis=0).loc[start_date:end_date]])
t2 = dt.datetime.now()
print("computing stock specific risk costs", t2 - t1)
stock_volatility_bs.to_csv(stock_volatility_bs_path)
stock_specific_risk.to_csv(specific_risk_path)
