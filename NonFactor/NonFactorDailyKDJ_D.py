import DataAPI.DataToolkit as Dtk
from Factor.DailyFactorBase import DailyFactorBase
import pandas as pd
import copy


class NonFactorDailyKDJ_D(DailyFactorBase):
    def __init__(self, alpha_factor_root_path, stock_list, start_date_int, end_date_int, params):
        super().__init__(alpha_factor_root_path)
        self.stock_list = stock_list
        self.start_date = start_date_int
        self.end_date = end_date_int

    def factor_calc(self):
        n = 9
        start_day = Dtk.get_n_days_off(self.start_date, -n - 2)[0]
        close_price = Dtk.get_panel_daily_pv_df(Dtk.get_complete_stock_list(), start_day, self.end_date,
                                                'close', 'FORWARD')
        high_price = Dtk.get_panel_daily_pv_df(Dtk.get_complete_stock_list(), start_day, self.end_date,
                                               'high', 'FORWARD')
        low_price = Dtk.get_panel_daily_pv_df(Dtk.get_complete_stock_list(), start_day, self.end_date,
                                              'low', 'FORWARD')
        RSV: pd.DataFrame = (close_price - low_price) / (high_price - low_price)
        K = pd.DataFrame(index=RSV.index, columns=RSV.columns)
        D = pd.DataFrame(index=RSV.index, columns=RSV.columns)
        last_date = None
        for n, date in enumerate(RSV.index):
            # print(n)
            if n == 0:
                last_date = date
                K.loc[date] = 0.5
                D.loc[date] = 0.5
                continue
            K_last_values = K.loc[last_date].fillna(0.5)
            K.loc[date] = K_last_values * 2 / 3 + RSV.loc[date] / 3
            D_last_value = D.loc[last_date].fillna(0.5)
            D.loc[date] = D_last_value * 2 / 3 + K.loc[date] / 3
            last_date = date
        D=D.astype(float)
        # ----以下勿改动----
        ans_df = D.loc[self.start_date: self.end_date]
        ans_df = Dtk.convert_df_index_type(ans_df, 'date_int', 'timestamp')
        return ans_df
