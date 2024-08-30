import DataAPI.DataToolkit as Dtk
from Factor.DailyFactorBase import DailyFactorBase
import numpy as np


class NonFactorDailyOBV(DailyFactorBase):
    def __init__(self, alpha_factor_root_path, stock_list, start_date_int, end_date_int, params):
        super().__init__(alpha_factor_root_path)
        self.stock_list = stock_list
        self.start_date = start_date_int
        self.end_date = end_date_int

    def factor_calc(self):
        def abs_mean(data):
            data = abs(data)
            return np.nanmean(data)

        n = 30
        start_day = Dtk.get_n_days_off(self.start_date, -n - 2)[0]
        close_price = Dtk.get_panel_daily_pv_df(Dtk.get_complete_stock_list(), start_day, self.end_date,
                                                'close', 'FORWARD')
        high_price = Dtk.get_panel_daily_pv_df(Dtk.get_complete_stock_list(), start_day, self.end_date,
                                               'high', 'FORWARD')
        low_price = Dtk.get_panel_daily_pv_df(Dtk.get_complete_stock_list(), start_day, self.end_date,
                                              'low', 'FORWARD')
        volume = Dtk.get_panel_daily_pv_df(Dtk.get_complete_stock_list(), start_day, self.end_date,
                                           'amt', 'FORWARD')
        ratio = ((close_price - low_price) - (high_price - close_price)) / (high_price - low_price) * volume
        ratio = ratio.rolling(window=n, min_periods=1).sum()
        change = ratio - ratio.shift(5)
        change_mean = change.rolling(window=120, min_periods=1).apply(abs_mean)
        ratio = change / change_mean
        # ratio=np.log(ratio)
        # ----以下勿改动----
        ans_df = ratio.loc[self.start_date: self.end_date]
        ans_df = Dtk.convert_df_index_type(ans_df, 'date_int', 'timestamp')
        return ans_df
