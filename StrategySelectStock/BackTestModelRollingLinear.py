# -*- coding: utf-8 -*-
"""
Created on 2018/9/6 13:34

@author: 006547
"""
from ModelSystem.ModelManagement import ModelManagement
from StrategySelectStock.ModelRollingLinear import ModelRollingLinear
import platform
import pickle

start_date = 20151201
end_date = 20180630
position_window = 1  # 调仓周期，单位天
update_model_period = 5  # 训练模型的周期，单位是多少个持仓周期
train_lag = 5
model_rolling_lag = 20
test_day_factor = ['F_D_DistToMa20', 'F_D_DistToMa10',
                   'F_D_GTJA1',
                   'F_D_GTJA2',
                   'F_D_GTJA3',
                   # 'F_D_GTJA4',
                   'F_D_GTJA5',
                   # 'F_D_GTJA6',
                   'F_D_GTJA7',
                   'F_D_GTJA8',
                   'F_D_GTJA9',
                   'F_D_GTJA10',
                   'F_D_GTJA11Lag6',
                   'F_D_GTJA11Lag12',
                   'F_D_GTJA11Lag18',
                   'F_D_GTJA12Lag5',
                   'F_D_GTJA12Lag10',
                   'F_D_GTJA12Lag15',
                   'F_D_GTJA13',
                   'F_D_GTJA14Lag5',
                   'F_D_GTJA14Lag10',
                   'F_D_GTJA14Lag15',
                   'F_D_GTJA15Lag1',
                   'F_D_GTJA15Lag5',
                   'F_D_GTJA15Lag10',
                   'F_D_GTJA15Lag15',
                   'F_D_GTJA16Lag5',
                   'F_D_GTJA16Lag10',
                   'F_D_GTJA16Lag15',
                   'F_D_GTJA17Lag5Lag5',
                   'F_D_GTJA17Lag10Lag5',
                   'F_D_GTJA17Lag15Lag5',
                   'F_D_GTJA18Lag5',
                   'F_D_GTJA18Lag10',
                   'F_D_GTJA18Lag15',
                   'F_D_GTJA19Lag5',
                   'F_D_GTJA19Lag10',
                   'F_D_GTJA19Lag15',
                   'F_D_GTJA20Lag6',
                   'F_D_GTJA20Lag12',
                   'F_D_GTJA20Lag18',
                   'F_D_GTJA21',
                   'F_D_GTJA22',
                   'F_D_GTJA23',
                   'F_D_GTJA24',
                   'F_D_GTJA25',
                   'F_D_GTJA26',
                   'F_D_GTJA27',
                   'F_D_GTJA28',
                   'F_D_GTJA29',
                   'F_D_GTJA31',
                   'F_D_GTJA32',
                   'F_D_GTJA33',
                   'F_D_GTJA34',
                   'F_D_GTJA35',
                   'F_D_GTJA36',
                   'F_D_GTJA37',
                   'F_D_GTJA38',
                   'F_D_GTJA39',
                   'F_D_GTJA40'
                   ]
test_day_factor_direction = [-1, -1]
test_min_factor = []
test_tag = ['twap']
test_pred_period = 1
universe = 'alpha_universe'  #
hedge_index = 'index_500'
is_day_factor = True  # 如果是没有用到日内数据的日级别因子，在调仓日应该使用前一个交易日的因子值
outlier_filter = False
z_score_standardizer = False
neutralize = False
model_name = 'RollingLinearModel-AllGTJA'

para_model = {'test_day_factor': test_day_factor,
              'test_day_factor_direction': test_day_factor_direction,
              'test_min_factor': test_min_factor,
              'test_tag': test_tag,
              'test_pred_period': test_pred_period,
              'train_lag': train_lag,
              'model_rolling_lag': model_rolling_lag,
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
single_Factor_Group_Model_1 = ModelRollingLinear(para_model, model_name, model_management)
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
