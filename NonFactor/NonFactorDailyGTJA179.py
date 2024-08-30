"""
Created on 2019/1/22
Revised on 2019/2/25
Rivised on 2019/3/20 fixed bug
@author: 006566

(RANK(CORR(VWAP, VOLUME, 4)) * RANK(CORR(RANK(LOW), RANK(MEAN(VOLUME, 50)), 12)))
corr_df1 = CORR(VWAP, VOLUME, 4)
rank_df1 = RANK(corr_df1) = RANK(CORR(VWAP, VOLUME, 4))

mean_df = MEAN(VOLUME, 50)
rank_df4 = rank(mean_df)
rank_df3 = rank(low)
corr_df2 = CORR(rank_df3, rank_df4, 12) = CORR(RANK(LOW), RANK(MEAN(VOLUME, 50)), 12))
rank_df2 = rank(corr_df2) = RANK(CORR(RANK(LOW), RANK(MEAN(VOLUME, 50)), 12)))

ans_df = corr_df1 * rank_df2
"""

import DataAPI.DataToolkit as Dtk
from Factor.DailyFactorBase import DailyFactorBase
import pandas as pd


class NonFactorDailyGTJA179(DailyFactorBase):
    def __init__(self, alpha_factor_root_path, stock_list, start_date_int, end_date_int, params):
        super().__init__(alpha_factor_root_path)
        self.stock_list = stock_list
        self.start_date = start_date_int
        self.end_date = end_date_int

    def factor_calc(self):
        valid_date_minus_2 = Dtk.get_n_days_off(self.start_date, -(70 + 2))[0]
        amt_df = Dtk.get_panel_daily_pv_df(self.stock_list, valid_date_minus_2, self.end_date, pv_type='amt')
        volume_df = Dtk.get_panel_daily_pv_df(self.stock_list, valid_date_minus_2, self.end_date, pv_type='volume')
        low_df = Dtk.get_panel_daily_pv_df(self.stock_list, valid_date_minus_2, self.end_date, pv_type='low')
        # 计算因子值
        vwap_df: pd.DataFrame = amt_df / volume_df
        corr_df1 = vwap_df.rolling(4).corr(volume_df)
        mean_df = volume_df.rolling(50).mean()
        rank_df3 = mean_df.rank(axis=1)
        rank_df4 = low_df.rank(axis=1)
        corr_df2 = rank_df4.rolling(12).corr(rank_df3)
        rank_df2 = corr_df2.rank(axis=1)
        factor_data = corr_df1 * rank_df2
        # ----以下勿改动----
        ans_df = factor_data.loc[self.start_date: self.end_date]
        ans_df = Dtk.convert_df_index_type(ans_df, 'date_int', 'timestamp')
        return ans_df
