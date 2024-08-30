from Factor.DailyFactorBase import DailyFactorBase
import DataAPI.DataToolkit as Dtk
import pandas as pd
import numpy as np


class FactorDailyUclerEwm(DailyFactorBase):
    def __init__(self, alpha_factor_root_path, stock_list, start_date_int, end_date_int, params):
        super().__init__(alpha_factor_root_path)
        self.stock_list = stock_list
        self.start_date = start_date_int
        self.end_date = end_date_int
        self.ewmspan = params['span']

    def factor_calc(self):
        start_date_minus_lag_2 = Dtk.get_n_days_off(self.start_date, -(self.ewmspan + 2))[0]
        # 获取close的后复权数据，是DataFrame，每一列的列名是股票代码，每一行的标签则是日期（例如20180829，是8位数的int）
        stock_close: pd.DataFrame = Dtk.get_panel_daily_pv_df(self.stock_list, start_date_minus_lag_2, self.end_date,
                                                              pv_type='close', adj_type='FORWARD')
        stock_high: pd.DataFrame = Dtk.get_panel_daily_pv_df(self.stock_list, start_date_minus_lag_2, self.end_date,
                                                             pv_type='high', adj_type='FORWARD')

        stock_nday_high = stock_high.shift(1).rolling(self.ewmspan).max()
        factor_data = (stock_close - stock_nday_high) / stock_nday_high
        factor_data = factor_data.ewm(span=self.ewmspan).mean()
        # ----以下勿改动----
        ans_df = factor_data.loc[self.start_date: self.end_date]
        ans_df = Dtk.convert_df_index_type(ans_df, 'date_int', 'timestamp')
        return ans_df
