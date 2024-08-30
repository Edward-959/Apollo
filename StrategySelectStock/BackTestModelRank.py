# -*- coding: utf-8 -*-
"""
Created on 2018/9/6 13:34

@author: 006547
"""
from ModelSystem.ModelManagement import ModelManagement
from StrategySelectStock.ModelRank import ModelRank
import platform
import pickle

start_date = 20151201
end_date = 20180630
position_window = 1  # 调仓周期，单位天
update_model_period = 5  # 训练模型的周期，单位是多少个持仓周期
train_lag = 5
test_day_factor = ['F_D_DistToMa20', 'F_D_DistToMa10']
test_day_factor_direction = [-1, -1]
test_min_factor = []
test_tag = ['twap']
test_pred_period = 1
universe = 'alpha_universe'  #
hedge_index = 'index_500'
is_day_factor = True  # 如果是没有用到日内数据的日级别因子，在调仓日应该使用前一个交易日的因子值
outlier_filter = True
z_score_standardizer = True
neutralize = True
model_name = 'RankModel-F_D_DistToMa20-F_D_DistToMa10'

para_model = {'test_day_factor': test_day_factor,
              'test_day_factor_direction': test_day_factor_direction,
              'test_min_factor': test_min_factor,
              'universe': universe,
              'hedge_index': hedge_index,
              'is_day_factor': is_day_factor,
              'outlier_filter': outlier_filter,
              'z_score_standardizer': z_score_standardizer,
              'neutralize': neutralize,
              'start_date': start_date,
              'end_date': end_date}  # 回测因子名称和模型可能用到的参数

model_management = ModelManagement(start_date=start_date, end_date=end_date,
                                   position_window=position_window, update_model_period=update_model_period)
single_Factor_Group_Model_1 = ModelRank(para_model, model_name, model_management)
model_management.train()
result = model_management.infer_result
if platform.system() == 'Windows':
    with open('signal_' + model_name + '.pickle', 'wb') as f:
        pickle.dump(result, f)
else:
    from xquant.pyfile import Pyfile
    py = Pyfile()
    with py.open('signal_' + model_name + '.pickle', 'wb') as f:
        pickle.dump(result, f)
