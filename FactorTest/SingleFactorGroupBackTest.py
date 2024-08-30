# -*- coding: utf-8 -*-
"""
Created on 2018/9/6 13:34

@author: 006547
"""
from ModelSystem.ModelManagement import ModelManagement
from FactorTest.SingleFactorGroupModel1 import SingleFactorGroupModel1
import pandas as pd


start_date = 20150601
end_date = 20160630
position_window = 1  # 调仓周期，单位天
update_model_period = 1  # 训练模型的周期，单位是多少个持仓周期
group_num = 10
test_factor = 'F_D_CloseCutGrowth_1'
universe = 'index_500'  # ['all']即为全市场
hedge_index = 'index_300'
whether_day_factor = False  # 如果是没有用到日内数据的日级别因子，在调仓日应该使用前一个交易日的因子值

# for i_group in range(1, group_num+1):
#     para_model = {'test_factor': test_factor, 'group': i_group,
#                   'group_num': group_num, 'universe': universe,
#                   'hedge_index': hedge_index, 'whether_day_factor': whether_day_factor,
#                   'start_date': start_date, 'end_date': end_date}  # 回测因子名称和模型可能用到的参数
#     model_name = 'single_Factor_Group_Model_1'
#
#     model_management = ModelManagement(start_date=start_date, end_date=end_date, position_window=position_window,
#                                        update_model_period=update_model_period)
#     single_Factor_Group_Model_1 = SingleFactorGroupModel1(para_model, model_name, model_management)  # 2是行业中性，1是非行业中性
#     model_management.train()
#     store: pd.HDFStore = pd.HDFStore("./signal/model1/group{}_signal.h5".format(i_group))
#     data_date = list(model_management.infer_result.keys())
#     key_table = pd.DataFrame(data=data_date, columns=["date"])
#     store.put("date", key_table)
#     # code_table = pd.DataFrame(data=single_Factor_Group_Model_1.code_list, columns=['code'])
#     # store.put("code_list", code_table)
#     for key in model_management.infer_result.keys():
#         data = model_management.infer_result.get(key)["infer_result"]
#         store.put("data/D{}S".format(key), data, format="table")
#
#     store.close()
para_model = {'test_factor': test_factor,
              'group_num': group_num,
              'universe': universe,
              'hedge_index': hedge_index,
              'whether_day_factor': whether_day_factor,
              'start_date': start_date,
              'end_date': end_date}  # 回测因子名称和模型可能用到的参数
model_name = 'single_Factor_Group_Model_1'
model_management = ModelManagement(start_date=start_date, end_date=end_date,
                                   position_window=position_window, update_model_period=update_model_period)
single_Factor_Group_Model_1 = SingleFactorGroupModel1(para_model, model_name, model_management)  # 2是行业中性，1是非行业中性
model_management.train()
data_date = list(model_management.infer_result.keys())
key_table = pd.DataFrame(data=data_date, columns=["date"])
for i_group in range(group_num):
    store: pd.HDFStore = pd.HDFStore("./signal/model1/group{}_signal.h5".format(i_group))
    store.put("date", key_table)
    for key in model_management.infer_result.keys():
        data = model_management.infer_result.get(key)["infer_result"]
        store.put("data/D{}S".format(key), data[i_group], format="table")