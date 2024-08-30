# -*- coding: utf-8 -*-
"""
Created on 2018/8/9 15:51

@author: 006547
"""
from DataAPI.DataToolkit import *


class ModelManagement:
    def __init__(self, start_date=None, end_date=None, position_window=None, update_model_period=None):
        self.model = None
        self.start_date = start_date
        self.end_date = end_date
        self.trading_day = get_trading_day(self.start_date, self.end_date)
        self.position_window = position_window
        self.update_model_period = update_model_period
        self.infer_result = {}
        self.model_saved = {}

    def register(self, model):
        self.model = model

    def train(self):
        model = self.model
        for i in range(self.trading_day.__len__()):
            if i % (self.position_window*self.update_model_period) == 0:
                print('training ' + str(self.trading_day[i]))
                self.model_saved.update({self.trading_day[i]: model.train(self.trading_day[i])})
            if i % self.position_window == 0:
                print('inferring ' + str(self.trading_day[i]))
                signal = model.infer(self.trading_day[i])
                if signal is not None:
                    self.infer_result.update({self.trading_day[i]: signal})
