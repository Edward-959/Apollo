# -*- coding: utf-8 -*-
"""
Created on 2018/11/12 13:31

@author: 006547
"""
import pandas as pd
import logging
from Backtest.PortfolioBackTester import PortfolioBackTester
# from StrategySelectStock.Optimizer import Optimizer
from StrategySelectStock.OptimizerBarra import Optimizer
from StrategySelectStock.SimulationAnalysis import SimulationAnalysis
import platform
from os import mkdir, path, environ
import os
import pickle
import time

# time.sleep(1.5*3600)

signal_file_name = 'signal_RandomForest_lag120_pred5_update10_roll80factors0319_n200_depth10_maxdailylabel0.3_universe300_01'

start_date = 20140514
end_date = 20180630

need_hedge = True
industry_neutral = True
hedge_index_barra = "hs300"
hedge_index = 'index_300'
return_predicted = True
cost_ratio = 0.002
deal_price_type = 'twap'
industry_copied_barra = None  # [21, 29, 30]
max_stock_num_barra = 200
min_stock_num_barra = None
single_weight_bias_barra = [0.015, 0.5]  # 表示指数中权重超过0.015的股票，其在组合中的权重偏离不超过指数中权重的50%
max_single_weight_barra = {'normal': 0.01, 'industry21': 0.02, 'industry29': 0.02, 'industry30': 0.02},
min_single_weight_barra = None
style_constraint_barra = {'All': [-0.2, 0.2]}  # {'Size': [-0.3, 0.3], 'NonLinearSize': [-0.3, 0.3]}
industry_constraint_barra = {'All': [-0.001, 0.001]}  # {21: [-0.001, 0.001], 29: [-0.001, 0.001], 30: [-0.001, 0.001], 31: [-0.001, 0.001]}
penalty_risk_barra = 1
penalty_cost_barra = 0.2

for i in [1]:
    penalty_risk_barra = i
    penalty_cost_barra = round(0.2 * i, 2)

    header = 'risk_' + str(penalty_risk_barra) + \
             '_cost_' + str(penalty_cost_barra) + \
             '_copied_industry' + str(industry_copied_barra) + \
             '_max_weight_' + str(max_single_weight_barra) + \
             '_min_weight_' + str(min_single_weight_barra) + \
             '_return_' + str(return_predicted) + \
             '_style_' + str(style_constraint_barra) + \
             '_industry_' + str(industry_constraint_barra) + \
             '_hedge_barra_' + str(hedge_index_barra)
    header_simulation = 'trade_cost_' + str(cost_ratio) + '_hedge_' + hedge_index

    logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.INFO)
    # optimizer = Optimizer(signal_file_name=signal_file_name, industry_neutral=industry_neutral, hedge_index=hedge_index,
    #                       percent_selected=0.1)
    # daily_stock_pool = optimizer.launch()
    optimizer = Optimizer()

    if platform.system() == "Windows":  # 云桌面环境运行是Windows
        absolutePath = ''
    elif os.system("nvidia-smi") == 0:
        absolutePath = "/data/user/Apollo/StrategySelectStock/" + 'Model_File_' + signal_file_name[7:] + '/'
    else:
        user_id = environ['USER_ID']
        absolutePath = "/app/data/" + user_id + "/Apollo/StrategySelectStock/" + 'Model_File_' + signal_file_name[7:] + '/'

    daily_stock_pool = optimizer.optimize_portfolio_weight(signal_file_name, absolutePath,
                                                           header, start_date, end_date,
                                                           hedge_index=hedge_index_barra,
                                                           industry_copied=industry_copied_barra,
                                                           max_stock_num=max_stock_num_barra,
                                                           # min_stock_num=min_stock_num_barra,
                                                           single_weight_bias=single_weight_bias_barra,
                                                           max_single_weight=max_single_weight_barra,
                                                           # min_single_weight=min_single_weight_barra,
                                                           style_constraint=style_constraint_barra,
                                                           industry_constraint=industry_constraint_barra,
                                                           return_predicted=return_predicted,
                                                           penalty_risk=penalty_risk_barra, penalty_cost=penalty_cost_barra)

    portfolio_back_tester = PortfolioBackTester(start_date, end_date, daily_stock_pool,
                                                cost_ratio=cost_ratio,
                                                group_test_need_hedge=need_hedge,
                                                group_test_hedge_index=hedge_index,
                                                deal_price_type=deal_price_type)
    portfolio_back_tester.run_test()
    csv_result = portfolio_back_tester.get_result()
    pdx = pd.DataFrame(csv_result)

    if platform.system() == "Windows":  # 云桌面环境运行是Windows
        simulation_path = "simulation/" + signal_file_name + '/'
        if not path.exists(simulation_path):
            mkdir(simulation_path)
    elif os.system("nvidia-smi") == 0:
        simulation_path = "/app/data/" + 'user' + "/Apollo/simulation/" + signal_file_name + '/'
        if not path.exists(simulation_path):
            mkdir(simulation_path)
    else:
        user_id = environ['USER_ID']
        simulation_path = "/app/data/" + user_id + "/Apollo/simulation/" + signal_file_name + '/'
        if not path.exists(simulation_path):
            mkdir(simulation_path)

    pdx.to_csv(simulation_path + "simulation_" + header_simulation + '_' + header + ".csv")

    simulation_name = "simulation_" + header_simulation + '_' + header
    simulation_analysis = SimulationAnalysis(signal_file_name, simulation_name)
    simulation_analysis.analysis()
