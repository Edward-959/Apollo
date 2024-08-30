# -*- coding: utf-8 -*-
"""
created on 2019/02/13
@author: 006566
"""
from abc import abstractmethod
import os
import pandas as pd


class DailyFactorBase:
    def __init__(self, alpha_factor_root_path):
        self.alpha_factor_root_path = alpha_factor_root_path

    @abstractmethod
    def factor_calc(self):
        pass

    @staticmethod
    def get_non_factor_df(non_factor_path):
        if not os.path.exists(non_factor_path):
            print("Error: the corresponding NonFactor file", non_factor_path, "does not exist!")
            return None
        store = pd.HDFStore(non_factor_path, mode="r")
        factor_original_df = store.select("/factor")
        store.close()
        return factor_original_df
