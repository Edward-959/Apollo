from TopGroupNav1.HedgedTopGroupNav import TopGroupNav
import datetime as dt
import platform
import os

if platform.system() == "Windows":  # 云桌面环境运行是Windows
    __ROOT_PATH__ = 'D:/Apollo/NeedUpdateFactors/'
    save_path = 'D:/Apollo/TopGroupNav/'
    if not os.path.exists:
        os.makedirs(save_path)
else:
    __ROOT_PATH__ = "/app/data/666889/Apollo/AlphaFactors/AlphaFactors/"
    user_id = os.environ['USER_ID']
    save_path = "/app/data/" + user_id + "/Apollo/TopGroupNav/"
    if not os.path.exists:
        os.makedirs(save_path)

start_date = 20141229
end_date = 20180630
holding_period = 1
label_type = 'twap'  # 可选twap, vwap或coda，既影响ic测试、也影响分组测试
universe = 'index_800'  # 目前可选范围：alpha_universe, risk_universe, index_300, index_500或index_50
outlier_filter_method = 'MAD'  # "3Std"或"MAD"
stock_cost_rate = 0.0012  # 交易成本
if universe == 'alpha_universe':
    group_num = 20
else:
    group_num = 10
is_day_factor = True
# 中性化是无顺序的，故是set，一般必选'size'和'industry3'，现在还新增可选'return20'
neutral_factors = {'size', 'industry3'}
# 极速分层测试：不做行业中性，不对冲，不考虑买卖的可行性
print("Start")
singleFactorTest = TopGroupNav(start_date, end_date, save_path, is_day_factor, holding_period, group_num,
                                    label_type=label_type, universe=universe, neutral_factor_set=neutral_factors,
                                    outlier_filtering_method=outlier_filter_method,
                                    stock_cost_rate=stock_cost_rate, factor_path=__ROOT_PATH__)
t1 = dt.datetime.now()
singleFactorTest.launch_test()
t2 = dt.datetime.now()
print("It costs", t2 - t1)
