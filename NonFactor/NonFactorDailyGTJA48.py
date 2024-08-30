"""
@Author  : 006566
(-1 * ((RANK(((SIGN((CLOSE - DELAY(CLOSE, 1))) + SIGN((DELAY(CLOSE, 1) - DELAY(CLOSE, 2))))
+ SIGN((DELAY(CLOSE, 2) - DELAY(CLOSE, 3))))) * SUM(VOLUME, 5)) / SUM(VOLUME, 20))

diff1_sign = SIGN((CLOSE - DELAY(CLOSE, 1)))
diff2_sign = SIGN((DELAY(CLOSE, 1) - DELAY(CLOSE, 2)))
diff3_sign = SIGN((DELAY(CLOSE, 2) - DELAY(CLOSE, 3)))

(-1 * ((RANK(((diff1_sign + diff2_sign) + diff_3_sign)) * SUM(VOLUME, 5)) / SUM(VOLUME, 20))

sum_diff = ((diff1_sign + diff2_sign) + diff3_sign))

ans_df = (RANK(sum_diff) * SUM(VOLUME, 5)) / SUM(VOLUME, 20))
"""

import DataAPI.DataToolkit as Dtk
from Factor.DailyFactorBase import DailyFactorBase
import numpy as np


class NonFactorDailyGTJA48(DailyFactorBase):
    def __init__(self, alpha_factor_root_path, stock_list, start_date_int, end_date_int, params):
        super().__init__(alpha_factor_root_path)
        self.stock_list = stock_list
        self.start_date = start_date_int
        self.end_date = end_date_int

    def factor_calc(self):
        start_date_minus_k_2 = Dtk.get_n_days_off(self.start_date, -(20 + 2))[0]
        volume_df = Dtk.get_panel_daily_pv_df(self.stock_list, start_date_minus_k_2, self.end_date, pv_type='volume')
        close = Dtk.get_panel_daily_pv_df(self.stock_list, start_date_minus_k_2, self.end_date, pv_type='close',
                                          adj_type='FORWARD')
        # 计算因子值
        diff1_sign = np.sign(close - close.shift(1))
        diff2_sign = np.sign(close.shift(1) - close.shift(2))
        diff3_sign = np.sign(close.shift(2) - close.shift(3))
        sum_diff = diff1_sign + diff2_sign + diff3_sign
        rank_df = sum_diff.rank(axis=1)
        sum_vol5 = volume_df.rolling(5).sum()
        sum_vol20 = volume_df.rolling(20).sum()
        ans_df = rank_df * sum_vol5 / sum_vol20
        factor_data = -1 * ans_df
        # ----以下勿改动----
        ans_df = factor_data.loc[self.start_date: self.end_date]
        ans_df = Dtk.convert_df_index_type(ans_df, 'date_int', 'timestamp')
        return ans_df
