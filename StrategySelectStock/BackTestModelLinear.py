# -*- coding: utf-8 -*-
"""
Created on 2018/9/6 13:34

@author: 006547
"""
import sys
import os

sys.path.append(os.path.abspath(os.path.abspath(__file__) + "/../.."))
from ModelSystem.ModelManagement import ModelManagement
from StrategySelectStock.ModelLinear import ModelLinear
import platform
import pickle
import os
import pandas as pd

start_date = 20140514
end_date = 20190312
position_window = 1  # 调仓周期，单位天
update_model_period = 20  # 训练模型的周期，单位是多少个持仓周期
train_lag = 120

factor_file_name = 'FactorList_csh.xlsx'
model_name = 'LinearModel-FactorList_csh-excess500-update20-lag120-pred5-fillFalse-fillMean-trainAlphastock'

test_min_factor = []
test_tag = ['twap_excess_500']
test_pred_period = 5
universe = 'alpha_universe'  #
hedge_index = 'index_500'
is_day_factor = True  # 如果是没有用到日内数据的日级别因子，在调仓日应该使用前一个交易日的因子值
outlier_filter = False
z_score_standardizer = False
neutralize = False
fill = False

if platform.system() == "Windows":  # 云桌面环境运行是Windows
    absolutePath = 'Model_File_' + model_name + '/'
    factor_dir = "D:\\Apollo\\StrategySelectStock\\FactorList\\"
elif os.system("nvidia-smi") == 0:
    absolutePath = "/data/user/Apollo/StrategySelectStock/" + 'Model_File_' + model_name + '/'
    factor_dir = "/data/user/Apollo/"
else:
    absolutePath = "/app/data/054703/Apollo/StrategySelectStock/" + 'Model_File_' + model_name + '/'
    factor_dir = "/app/data/054703/Apollo/"

test_day_factor = list(
    pd.read_excel(factor_dir + factor_file_name, sheet_name='FactorList', header=None).values.flatten())

para_model = {'test_day_factor': test_day_factor,
              'test_min_factor': test_min_factor,
              'test_tag': test_tag,
              'test_pred_period': test_pred_period,
              'train_lag': train_lag,
              'universe': universe,
              'hedge_index': hedge_index,
              'is_day_factor': is_day_factor,
              'outlier_filter': outlier_filter,
              'z_score_standardizer': z_score_standardizer,
              'neutralize': neutralize,
              'fill': fill,
              'start_date': start_date,
              'end_date': end_date,
              'absolutePath': absolutePath}  # 回测因子名称和模型可能用到的参数

model_management = ModelManagement(start_date=start_date, end_date=end_date,
                                   position_window=position_window, update_model_period=update_model_period)
single_Factor_Group_Model_1 = ModelLinear(para_model, model_name, model_management)
model_management.train()
result = model_management.infer_result
if platform.system() == "Windows":  # 云桌面环境运行是Windows
    with open(absolutePath + 'signal_' + model_name + '.pickle', 'wb') as f:
        pickle.dump(result, f)
elif os.system("nvidia-smi") == 0:
    with open(absolutePath + 'signal_' + model_name + '.pickle', 'wb') as f:
        pickle.dump(result, f)
else:
    with open(absolutePath + 'signal_' + model_name + '.pickle', 'wb') as f:
        pickle.dump(result, f)
