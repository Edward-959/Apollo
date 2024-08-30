import DataAPI.DataToolkit as Dtk
from Factor.DailyFactorBase import DailyFactorBase
import numpy as np


class NonFactorDailyget_n_day_ret(DailyFactorBase):
    def __init__(self, alpha_factor_root_path, stock_list, start_date_int, end_date_int, params):
        super().__init__(alpha_factor_root_path)
        self.stock_list = stock_list
        self.start_date = start_date_int
        self.end_date = end_date_int
        self.n = params['n']

    def factor_calc(self):
        n = self.n
        start_day = Dtk.get_n_days_off(self.start_date, -n - 2)[0]
        close_price = Dtk.get_panel_daily_pv_df(Dtk.get_complete_stock_list(), start_day, self.end_date,
                                                'close', 'FORWARD')
        factor_data = close_price / close_price.shift(n) - 1

        # ----以下勿改动----
        ans_df = factor_data.loc[self.start_date: self.end_date]
        ans_df = Dtk.convert_df_index_type(ans_df, 'date_int', 'timestamp')
        return ans_df
