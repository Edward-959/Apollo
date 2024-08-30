# -*- coding: utf-8 -*-
"""
Created on 2018/8/28 16:04

@author: 006547
"""
import numpy as np


def remove_void_data(data_list: list):
    temp_judge = np.isnan(data_list[0])
    temp_judge = np.hstack((temp_judge, data_list[0] == np.inf))
    temp_judge = np.hstack((temp_judge, data_list[0] == -np.inf))
    if data_list.__len__() > 1:
        for i in range(1, data_list.__len__()):
            temp_judge = np.hstack((temp_judge, np.isnan(data_list[i])))
            temp_judge = np.hstack((temp_judge, data_list[i] == np.inf))
            temp_judge = np.hstack((temp_judge, data_list[i] == -np.inf))
    judge = temp_judge.any(1)
    data_list_filtration = []
    judge_reverse = judge == False
    for i in range(0, data_list.__len__()):
        data_list_filtration.append(data_list[i][judge_reverse, :])
    return data_list_filtration, judge


def fill_void_data(data_list: list, fill: str):
    data_list_filled = []
    if fill == '0':
        for i in range(data_list.__len__()):
            temp = data_list[i].copy()
            temp[np.isnan(temp)] = 0
            temp[temp == np.inf] = 0
            temp[temp == -np.inf] = 0
            data_list_filled.append(temp)
    elif fill == 'mean':
        for i in range(data_list.__len__()):
            temp = data_list[i].copy()

            temp[temp == np.inf] = np.nan
            temp[temp == -np.inf] = np.nan
            nan_mean = np.nanmean(temp, axis=0)
            is_nan = np.isnan(temp)
            for j in range(temp.shape[1]):
                temp[is_nan[:, j], j] = nan_mean[j]
            data_list_filled.append(temp)
    return data_list_filled
