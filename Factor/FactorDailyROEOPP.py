""""
013542
营业利润/所有者权益
"""
import DataAPI.DataToolkit as Dtk
from Factor.DailyFactorBase import DailyFactorBase


class FactorDailyROEOPP(DailyFactorBase):
    # 这个因子没有参数，但也须在初始化时预留一个"params"
    def __init__(self, alpha_factor_root_path, stock_list, start_date_int, end_date_int, params):
        super().__init__(alpha_factor_root_path)
        self.stock_list = stock_list
        self.start_date = start_date_int
        self.end_date = end_date_int

    def factor_calc(self):
        op_profit = Dtk.get_daily_wind_quarterly_data(self.stock_list, 'AShareIncome', 'OPER_PROFIT',
                                                   self.start_date, self.end_date, 'ttm')
        equity = Dtk.get_daily_wind_quarterly_data(self.stock_list, 'AShareBalanceSheet', 'TOT_SHRHLDR_EQY_INCL_MIN_INT',
                                                      self.start_date, self.end_date, 'original')
        ans_df = op_profit/equity
        # ----以下勿改动----
        ans_df = ans_df.loc[self.start_date: self.end_date]
        ans_df = Dtk.convert_df_index_type(ans_df, 'date_int', 'timestamp')
        return ans_df