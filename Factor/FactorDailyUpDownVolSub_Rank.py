from Factor.DailyFactorBase import DailyFactorBase
import DataAPI.DataToolkit as Dtk
import pandas as pd


class FactorDailyUpDownVolSub_Rank(DailyFactorBase):
    def __init__(self, alpha_factor_root_path, stock_list, start_date_int, end_date_int, params):
        super().__init__(alpha_factor_root_path)
        self.stock_list = stock_list
        self.start_date = start_date_int
        self.end_date = end_date_int
        self.lag1 = params["lag1"]
        self.ewmspan = params["span"]

    def factor_calc(self):
        start_date_minus_lag_2 = Dtk.get_n_days_off(self.start_date, -(self.lag1 + 2))[0]
        stock_pct_chg: pd.DataFrame = Dtk.get_panel_daily_pv_df(self.stock_list, start_date_minus_lag_2, self.end_date,
                                                                pv_type='pct_chg')
        stock_amt: pd.DataFrame = Dtk.get_panel_daily_pv_df(self.stock_list, start_date_minus_lag_2, self.end_date,
                                                            pv_type='amt')
        stock_vol_up = stock_amt[stock_pct_chg > 0]
        stock_vol_up[stock_pct_chg <= 0] = 0
        stock_vol_down = stock_amt[stock_pct_chg < 0]
        stock_vol_down[stock_pct_chg >= 0] = 0
        stock_vol_up1 = stock_pct_chg * stock_vol_up
        stock_vol_down1 = stock_pct_chg * stock_vol_down
        factor_data = stock_vol_up1.rolling(self.lag1).sum() + stock_vol_down1.rolling(self.lag1).sum()
        factor_data = factor_data.rank(axis=1)
        factor_data = factor_data.ewm(span=self.ewmspan).mean()
        # ----以下勿改动----
        ans_df = factor_data.loc[self.start_date: self.end_date]
        ans_df = Dtk.convert_df_index_type(ans_df, 'date_int', 'timestamp')
        return ans_df
