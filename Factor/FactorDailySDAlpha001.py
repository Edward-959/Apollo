from Factor.DailyFactorBase import DailyFactorBase
import DataAPI.DataToolkit as Dtk
import pandas as pd
import numpy as np


class FactorDailySDAlpha001(DailyFactorBase):
    def __init__(self, alpha_factor_root_path, stock_list, start_date_int, end_date_int, params):
        super().__init__(alpha_factor_root_path)
        self.stock_list = stock_list
        self.start_date = start_date_int
        self.end_date = end_date_int
        self.n = params['n']

    def factor_calc(self):
        start_date_minus_lag_2 = Dtk.get_n_days_off(self.start_date, -self.n-2)[0]
        stock_close: pd.DataFrame = Dtk.get_panel_daily_pv_df(self.stock_list, start_date_minus_lag_2, self.end_date,
                                                            pv_type='close', adj_type='FORWARD')
        stock_high: pd.DataFrame = Dtk.get_panel_daily_pv_df(self.stock_list, start_date_minus_lag_2, self.end_date,
                                                              pv_type='high', adj_type='FORWARD')
        stock_low: pd.DataFrame = Dtk.get_panel_daily_pv_df(self.stock_list, start_date_minus_lag_2, self.end_date,
                                                             pv_type='low', adj_type='FORWARD')

        stock_comp1 = stock_high - stock_low
        stock_comp2 = (stock_high - stock_close.shift(1)).abs()
        stock_comp3 = (stock_low - stock_close.shift(1)).abs()

        sign1 = ((stock_comp1 - stock_comp2) >= 0)
        temp_df = stock_comp2.copy()
        temp_df[sign1] = stock_comp1
        sign2 = ((temp_df- stock_comp3) >= 0)
        factor_data = stock_comp3.copy()
        factor_data[sign2] = temp_df
        factor_data = factor_data.rolling(self.n).mean()
        # ----以下勿改动----
        ans_df = factor_data.loc[self.start_date: self.end_date]
        ans_df = Dtk.convert_df_index_type(ans_df, 'date_int', 'timestamp')
        return ans_df
