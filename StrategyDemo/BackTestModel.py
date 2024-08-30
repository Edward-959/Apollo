# -*- coding: utf-8 -*-
"""
Created on 2018/8/8 15:42

@author: 006547
"""
from ModelSystem.ModelManagement import ModelManagement
from StrategyDemo.ModelDemo import ModelDemo
import pandas as pd


start_date = 20180605
end_date = 20180630
position_window = 1  # 调仓周期，单位天
update_model_period = 2  # 训练模型的周期，单位是多少个持仓周期
para_model = {}
model_name = 'demo'

model_management = ModelManagement(start_date=start_date, end_date=end_date, position_window=position_window,
                                   update_model_period=update_model_period)
model_demo = ModelDemo(para_model, model_name, model_management)
model_management.train()
store: pd.HDFStore = pd.HDFStore("./signal.h5")
data_date = list(model_management.infer_result.keys())
key_table = pd.DataFrame(data=data_date, columns=["date"])
store.put("date", key_table)
code_table = pd.DataFrame(data=model_demo.code_list, columns=['code'])
store.put("code_list", code_table)
for key in model_management.infer_result.keys():
    data = model_management.infer_result.get(key)["infer_result"]
    store.put("data/D{}S".format(key), data, format="table")

store.close()
