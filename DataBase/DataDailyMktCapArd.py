from Factor.DailyFactorBase import DailyFactorBase
import DataAPI.DataToolkit as Dtk
import xquant.multifactor.IO.IO as xIO


class DataDailyMktCapArd(DailyFactorBase):
    # 这个因子没有参数，但也须在初始化时预留一个"params"
    def __init__(self, alpha_factor_root_path, start_date_int, end_date_int, params):
        super().__init__(alpha_factor_root_path)
        self.start_date = start_date_int
        self.end_date = end_date_int

    def factor_calc(self):
        # 添加股票和指数
        alt = "MD_CHINA_STOCK_DAILY_WIND"
        df1 = xIO.read_data([self.start_date, self.end_date], alt=alt)
        df1 = df1['mkt_cap_ard']
        df1 = df1.unstack()
        df1 = Dtk.convert_df_index_type(df1, 'timestamp2', 'date_int')
        df1.columns.name = ''
        ans_df = df1
        # ----以下勿改动----
        ans_df = ans_df.loc[self.start_date: self.end_date]
        return ans_df
