"""
Created on 2018/12/17 by 006566 东吴证券魏建榕-精细反转因子
计算过去n天每日的"平均单笔成交金额"，然后按降序排列，取前n/2组的涨跌幅加总，作为因子值
Wei for 魏
"""
from Factor.DailyFactorBase import DailyFactorBase
import DataAPI.DataToolkit as Dtk
import pandas as pd
import numpy as np
from xquant.multifactor.IO.IO import read_data


class FactorDailyWeiReversalHigh(DailyFactorBase):
    def __init__(self, alpha_factor_root_path, stock_list, start_date_int, end_date_int, params):
        super().__init__(alpha_factor_root_path)
        self.stock_list = stock_list
        self.start_date = start_date_int
        self.end_date = end_date_int
        self.n = params['n']

    def factor_calc(self):
        valid_start_date = Dtk.get_n_days_off(self.start_date, -(self.n + 2))[0]

        # 从XQuant的Wind落地数据库的AShareMoneyFlow这张表读取TRADES_COUNT这个字段
        alt = "AShareMoneyFlow"
        df = read_data([valid_start_date, self.end_date], alt=alt)['TRADES_COUNT']
        # 从XQuant的Wind落地数据库读取到的原始数据是双重索引，且索引类型是时间戳，这里转化为date_int
        df = df.unstack()
        trade_counts_df = Dtk.convert_df_index_type(df, 'timestamp2', 'date_int')

        amt_df = Dtk.get_panel_daily_pv_df(self.stock_list, valid_start_date, self.end_date, 'amt')
        close_df = Dtk.get_panel_daily_pv_df(self.stock_list, valid_start_date, self.end_date, 'close')
        pre_close_df = Dtk.get_panel_daily_pv_df(self.stock_list, valid_start_date, self.end_date, 'pre_close')

        avg_amt_per_trade_df = amt_df / trade_counts_df  # 平均单笔成交金额
        pct_chg_df = close_df / pre_close_df - 1  # 每日涨跌幅

        avg_amt_per_trade_df = avg_amt_per_trade_df.mul(amt_df).div(amt_df)  # 无交易的日期赋值为nan
        pct_chg_df = pct_chg_df.mul(amt_df).div(amt_df)

        m_df = pd.DataFrame()

        for i in range(avg_amt_per_trade_df.__len__()):
            if i <= self.n - 1:
                continue
            temp_avg_amt_per_trade_df = avg_amt_per_trade_df.iloc[i - self.n + 1:i + 1]
            temp_avg_amt_per_trade_rank_df = temp_avg_amt_per_trade_df.rank(ascending=False)
            rank_max = temp_avg_amt_per_trade_rank_df.max(axis=0)
            invalid_column = rank_max < int(self.n * 0.9)
            temp_pct_chg_df = pct_chg_df.iloc[i - self.n + 1:i + 1]
            temp_pct_chg_plus1_df = temp_pct_chg_df + 1
            temp_pct_chg_plus1_df = temp_pct_chg_plus1_df.fillna(1)

            temp_pct_chg_plus_high_df = temp_pct_chg_plus1_df.copy()
            temp_pct_chg_plus_high_df[temp_avg_amt_per_trade_rank_df >= rank_max / 2] = 1
            high_return_df = temp_pct_chg_plus_high_df.cumprod()
            # 如最后一天没交易，再变为nan
            high_return_df = high_return_df * temp_avg_amt_per_trade_df / temp_avg_amt_per_trade_df

            high_return_df = high_return_df - 1
            high_return_df = high_return_df.iloc[-1]
            high_return_df[invalid_column] = np.nan
            m_df = m_df.append(high_return_df)

        # 保留start_date至end_date（前闭后闭）期间的数据
        factor_data = m_df.loc[self.start_date: self.end_date].copy()
        # ----以下勿改动----
        ans_df = factor_data.loc[self.start_date: self.end_date]
        ans_df = Dtk.convert_df_index_type(ans_df, 'date_int', 'timestamp')
        return ans_df
