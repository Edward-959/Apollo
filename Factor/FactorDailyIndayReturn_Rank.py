from Factor.DailyFactorBase import DailyFactorBase
import DataAPI.DataToolkit as Dtk
import pandas as pd
import numpy as np


class FactorDailyIndayReturn_Rank(DailyFactorBase):
    def __init__(self, alpha_factor_root_path, stock_list, start_date_int, end_date_int, params):
        super().__init__(alpha_factor_root_path)
        self.stock_list = stock_list
        self.start_date = start_date_int
        self.end_date = end_date_int
        self.ma = params['ma']

    def factor_calc(self):
        start_date_minus_lag_2 = Dtk.get_n_days_off(self.start_date, -(self.ma + 2))[0]
        # 获取close的后复权数据，是DataFrame，每一列的列名是股票代码，每一行的标签则是日期（例如20180829，是8位数的int）
        stock_close: pd.DataFrame = Dtk.get_panel_daily_pv_df(self.stock_list, start_date_minus_lag_2, self.end_date,
                                                              pv_type='pre_close', adj_type='NONE')
        stock_open: pd.DataFrame = Dtk.get_panel_daily_pv_df(self.stock_list, start_date_minus_lag_2, self.end_date,
                                                             pv_type='open', adj_type='NONE')

        stock_inday_return = (stock_close / stock_open - 1) * 100
        stock_inday_return_rank = stock_inday_return.rank(axis=1)
        factor_data = stock_inday_return_rank.rolling(self.ma).sum()
        # ----以下勿改动----
        ans_df = factor_data.loc[self.start_date: self.end_date]
        ans_df = Dtk.convert_df_index_type(ans_df, 'date_int', 'timestamp')
        return ans_df
