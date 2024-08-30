import DataAPI.DataToolkit as Dtk
from Factor.DailyFactorBase import DailyFactorBase
import pandas as pd
import copy


class NonFactorDailyRSI_14(DailyFactorBase):
    def __init__(self, alpha_factor_root_path, stock_list, start_date_int, end_date_int, params):
        super().__init__(alpha_factor_root_path)
        self.stock_list = stock_list
        self.start_date = start_date_int
        self.end_date = end_date_int

    def factor_calc(self):
        n = 14
        start_day = Dtk.get_n_days_off(self.start_date, -n - 2)[0]
        close_price = Dtk.get_panel_daily_pv_df(Dtk.get_complete_stock_list(), start_day, self.end_date,
                                                'close', 'FORWARD')
        close_price_dif = close_price - close_price.shift(1)
        close_up: pd.DataFrame = copy.deepcopy(close_price_dif)
        close_up[close_up < 0] = 0
        close_up = close_up.rolling(window=n, min_periods=1).sum()
        close_down = close_price_dif
        close_down[close_down > 0] = 0
        close_down = close_down.rolling(window=n, min_periods=1).sum()
        close_down = abs(close_down)
        factor_data = close_up / (close_up + close_down)
        # ----以下勿改动----
        ans_df = factor_data.loc[self.start_date: self.end_date]
        ans_df = Dtk.convert_df_index_type(ans_df, 'date_int', 'timestamp')
        return ans_df
