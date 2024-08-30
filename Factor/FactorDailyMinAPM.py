# -*- coding: utf-8 -*-
# @Time    : 2018/12/3 13:28
# @Author  : 011673
# @File    : FactorDailyMinAPM.py
# 对于特定的股票、最近的 N 个交易日，记逐日上午的股票收益率为 rt_am,指数收益率为 Rt_am,逐日下午的股票收益率为 rt_pm,指数收益率为 Rt_pm。
# 1）将上午与下午的数据汇总，共有 40 组(r,R)的收益率数据，按照以下式子进行回归：
# ri=α+βR+ε
# 其中，α为常数项，β为斜率项，εi为残差序列。
# 2）以上得到的 40 个残差ε中，属于上午的记为ε_am，属于下午的记为ε_pm。计算每日上午与下午残差的差值序列：
# ε=ε_am-ε_pm
# 3）为了衡量上午与下午残差的差异程度，我们设计了统计量stat，计算公式如下[5]：
# stat = u/(σ/sqrt(N))
# 其中，μ为ε均值，σ为ε标准差，N=20。总的来说，统计量 stat反映了剔除市场影响后股价行为上午与下午的差异度。stat 数值大于（小于） 0 越多，则股票在上午的表现越好（差）于下午。
# 4} 为了消除与动量因子的纠缠，我们将统计量 stat 对动量因子进行横截面回归.回归得到的残差为APM因子取值
import DataAPI.DataToolkit as Dtk
from Factor.DailyFactorBase import DailyFactorBase
import os
import copy
from scipy import optimize
import pandas as pd


def get_residual_with_no_inter(ret_1: pd.Series, stat_1: pd.Series):
    ret = copy.deepcopy(ret_1)
    ret.fillna(ret.mean(), inplace=True)
    ret = ret.values
    stat = copy.deepcopy(stat_1)
    stat.fillna(stat.mean(), inplace=True)
    stat = stat.values
    A = optimize.curve_fit(func, ret, stat)[0][0]
    return stat_1 - A * ret_1


def func(x, A):
    return A * x


class FactorDailyMinAPM(DailyFactorBase):
    def __init__(self, alpha_factor_root_path, stock_list, start_date_int, end_date_int, params):
        super().__init__(alpha_factor_root_path)
        self.stock_list = stock_list
        self.start_date = start_date_int
        self.end_date = end_date_int

    def factor_calc(self):
        # ---- 1. 请设定对应的因子原始值文件-------
        corresponding_non_factor_file = "NF_D_TempRetForAPM.h5"
        corresponding_non_factor_path = os.path.join(self.alpha_factor_root_path, "AlphaNonFactors",
                                                     corresponding_non_factor_file)
        factor_original_df = self.get_non_factor_df(corresponding_non_factor_path)
        factor_original_df = Dtk.convert_df_index_type(factor_original_df, 'timestamp', 'date_int')
        ret = factor_original_df.loc[self.start_date: self.end_date]
        ret = ret.astype(float)

        corresponding_non_factor_file = "NF_D_MinTempAPM.h5"
        corresponding_non_factor_path = os.path.join(self.alpha_factor_root_path, "AlphaNonFactors",
                                                     corresponding_non_factor_file)
        factor_original_df = self.get_non_factor_df(corresponding_non_factor_path)
        factor_original_df = Dtk.convert_df_index_type(factor_original_df, 'timestamp', 'date_int')
        stat = factor_original_df.loc[self.start_date: self.end_date]
        stat = stat.astype(float)

        factor_data = pd.DataFrame(index=stat.index, columns=stat.columns)
        for index in stat.index:
            print(index)
            factor_data.loc[index, :] = get_residual_with_no_inter(ret.loc[index, :], stat.loc[index, :])
        ans_df: pd.DataFrame=factor_data.loc[self.start_date: self.end_date]
        ans_df = ans_df.astype(float)
        ans_df = Dtk.convert_df_index_type(ans_df, 'date_int', 'timestamp')
        return ans_df

