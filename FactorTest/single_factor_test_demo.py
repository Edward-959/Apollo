from FactorTest.SingleFactorTest import SingleFactorTest
import datetime as dt
import platform


if platform.system() == "Windows":  # 云桌面环境运行是Windows
    __ROOT_PATH__ = r"D:\AlphaFactors"
    # __ROOT_PATH__ = r"S:\Apollo\AlphaFactors"
else:
    __ROOT_PATH__ = "/app/data/006566/Apollo/AlphaFactors/"

start_date = 20141229
end_date = 20180630
holding_period = 1
label_type = 'twap'  # 可选twap, vwap或coda，既影响ic测试、也影响分组测试
universe = 'alpha_universe'  # 目前可选范围：alpha_universe, risk_universe, index_300, index_500, index_800或index_50
outlier_filter_method = 'MAD'  # "3Std"或"MAD"
stock_cost_rate = 0.0012  # 交易成本
industry_analysis_group_num = 5  # 若是alpha_universe 建议为5；若是index_800建议为3


for factor_name in ['F_D_WLBF_120_6_10', 'F_D_WLBF_120_6_16', 'F_D_WLBF_120_8_13']:
    group_num = 20
    is_day_factor = True
    # 中性化是无顺序的，故是set，一般必选'size'和'industry3'，现在还新增可选'return20'
    neutral_factors = {'size', 'industry3'}
    # 极速分层测试：不做行业中性，不对冲，不考虑买卖的可行性
    print("Start single factor test", factor_name)
    singleFactorTest = SingleFactorTest(factor_name, start_date, end_date, is_day_factor, holding_period, group_num,
                                        label_type=label_type, universe=universe, neutral_factor_set=neutral_factors,
                                        outlier_filtering_method=outlier_filter_method,
                                        stock_cost_rate=stock_cost_rate,
                                        industry_analysis_group_num=industry_analysis_group_num,
                                        factor_path=__ROOT_PATH__)
    t1 = dt.datetime.now()
    singleFactorTest.launch_test()
    t2 = dt.datetime.now()
    print("Single factor test", factor_name, "costs", t2 - t1)
