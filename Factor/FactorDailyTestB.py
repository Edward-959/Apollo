from Factor.DailyFactorBase import DailyFactorBase
import DataAPI.DataToolkit as Dtk


class FactorDailyTestB(DailyFactorBase):
    def __init__(self, alpha_factor_root_path, stock_list, start_date_int, end_date_int, params):
        super().__init__(alpha_factor_root_path)
        self.stock_list = stock_list
        self.start_date = start_date_int
        self.end_date = end_date_int
        self.lag1 = params["lag1"]
        self.lag2 = params["lag2"]
        self.close_df = None

    def factor_calc(self):
        valid_start_date = Dtk.get_n_days_off(self.start_date, -(self.lag1+2))[0]
        self.close_df = Dtk.get_panel_daily_pv_df(self.stock_list, valid_start_date, self.end_date, 'close',
                                                  'FORWARD')
        return_df = self.close_df / self.close_df.shift(self.lag1) - self.close_df / self.close_df.shift(self.lag2)
        # ----以下勿改动----
        ans_df = return_df.loc[self.start_date: self.end_date]
        ans_df = Dtk.convert_df_index_type(ans_df, 'date_int', 'timestamp')
        return ans_df
