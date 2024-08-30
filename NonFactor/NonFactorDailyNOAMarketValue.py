# -*- coding: utf-8 -*-
"""
Created on 2019/4/4
@author: Xiu Zixing
根据国盛证券研报《对价值因子的思考和改进》
NOA计算公式出自研报
目的是剔除公司净资产中金融资产的影响，提炼出经营性净资产
"""
import DataAPI.DataToolkit as Dtk
from Factor.DailyFactorBase import DailyFactorBase
import numpy as np
import pandas as pd
import os


def data_df_convert(data_df, industry_bool, volume_df):
    data_df = data_df.fillna(0)
    data_df = data_df * industry_bool
    volume_df = volume_df.replace(0, float('nan'))
    data_df = data_df * volume_df / volume_df
    return data_df


class NonFactorDailyMarketValueAdjusted2(DailyFactorBase):
    # 这个因子没有参数，但也须在初始化时预留一个"params"
    def __init__(self, alpha_factor_root_path, stock_list, start_date_int, end_date_int, params):
        super().__init__(alpha_factor_root_path)
        self.stock_list = stock_list
        self.start_date = start_date_int
        self.end_date = end_date_int

    def factor_calc(self):
        valid_start_date = Dtk.get_n_days_off(self.start_date, -20)[0]
        market_value = Dtk.get_panel_daily_info(self.stock_list, valid_start_date, self.end_date, info_type='mkt_cap_ard')
        volume_df = Dtk.get_panel_daily_pv_df(self.stock_list, valid_start_date, self.end_date, "volume")
        industry3 = Dtk.get_panel_daily_info(self.stock_list, valid_start_date, self.end_date, info_type='industry3')
        condition1 = (industry3 < 29) + 0
        condition2 = (industry3 != 21) + 0
        industry_bool = condition1 * condition2
        alt1 = 'AShareBalanceSheet'
        # 以下为金融资产
        monetary_cap = Dtk.get_daily_wind_quarterly_data(self.stock_list, alt1, 'monetary_cap', valid_start_date, self.end_date)
        derivative_fin_assets = Dtk.get_daily_wind_quarterly_data(self.stock_list, alt1, 'derivative_fin_assets', valid_start_date, self.end_date)
        tradable_fin_assets = Dtk.get_daily_wind_quarterly_data(self.stock_list, alt1, 'tradable_fin_assets', valid_start_date, self.end_date)
        red_monetary_cap_for_sale = Dtk.get_daily_wind_quarterly_data(self.stock_list, alt1, 'red_monetary_cap_for_sale', valid_start_date, self.end_date)
        fin_assets_avail_for_sale = Dtk.get_daily_wind_quarterly_data(self.stock_list, alt1, 'fin_assets_avail_for_sale', valid_start_date, self.end_date)
        held_to_mty_invest = Dtk.get_daily_wind_quarterly_data(self.stock_list, alt1, 'held_to_mty_invest', valid_start_date, self.end_date)
        dvd_rct = Dtk.get_daily_wind_quarterly_data(self.stock_list, alt1, 'dvd_rcv', valid_start_date, self.end_date)
        invest_real_estate = Dtk.get_daily_wind_quarterly_data(self.stock_list, alt1, 'invest_real_estate', valid_start_date, self.end_date)
        int_rct = Dtk.get_daily_wind_quarterly_data(self.stock_list, alt1, 'int_rcv', valid_start_date, self.end_date)
        time_deposits = Dtk.get_daily_wind_quarterly_data(self.stock_list, alt1, 'time_deposits', valid_start_date, self.end_date)
        # 以下为流动负债
        tot_cur_liab = Dtk.get_daily_wind_quarterly_data(self.stock_list, alt1, 'tot_cur_liab', valid_start_date, self.end_date)
        # 以下为无息流动负债
        acct_payable = Dtk.get_daily_wind_quarterly_data(self.stock_list, alt1, 'acct_payable', valid_start_date, self.end_date)
        adv_from_cust = Dtk.get_daily_wind_quarterly_data(self.stock_list, alt1, 'adv_from_cust', valid_start_date, self.end_date)
        empl_ben_payable = Dtk.get_daily_wind_quarterly_data(self.stock_list, alt1, 'empl_ben_payable', valid_start_date, self.end_date)
        taxes_surcharges_payable = Dtk.get_daily_wind_quarterly_data(self.stock_list, alt1, 'taxes_surcharges_payable', valid_start_date, self.end_date)
        oth_payable = Dtk.get_daily_wind_quarterly_data(self.stock_list, alt1, 'oth_payable', valid_start_date, self.end_date)
        acc_exp = Dtk.get_daily_wind_quarterly_data(self.stock_list, alt1, 'acc_exp', valid_start_date, self.end_date)
        deferred_inc = Dtk.get_daily_wind_quarterly_data(self.stock_list, alt1, 'deferred_inc', valid_start_date, self.end_date)
        oth_cur_liab = Dtk.get_daily_wind_quarterly_data(self.stock_list, alt1, 'oth_cur_liab', valid_start_date, self.end_date)
        # 以下为带息非流动负债
        lt_borrow = Dtk.get_daily_wind_quarterly_data(self.stock_list, alt1, 'lt_borrow', valid_start_date, self.end_date)
        bonds_payable = Dtk.get_daily_wind_quarterly_data(self.stock_list, alt1, 'bonds_payable', valid_start_date, self.end_date)

        # 以下为数据处理
        # 流动负债
        tot_cur_liab = data_df_convert(tot_cur_liab, industry_bool, volume_df)
        # 无息流动负债
        acct_payable = data_df_convert(acct_payable, industry_bool, volume_df)
        adv_from_cust = data_df_convert(adv_from_cust, industry_bool, volume_df)
        empl_ben_payable = data_df_convert(empl_ben_payable, industry_bool, volume_df)
        taxes_surcharges_payable = data_df_convert(taxes_surcharges_payable, industry_bool, volume_df)
        oth_payable = data_df_convert(oth_payable, industry_bool, volume_df)
        acc_exp = data_df_convert(acc_exp, industry_bool, volume_df)
        deferred_inc = data_df_convert(deferred_inc, industry_bool, volume_df)
        oth_cur_liab = data_df_convert(oth_cur_liab, industry_bool, volume_df)
        # 带息非流动负债
        lt_borrow = data_df_convert(lt_borrow, industry_bool, volume_df)
        bonds_payable = data_df_convert(bonds_payable, industry_bool, volume_df)
        # 金融资产
        dvd_rct = data_df_convert(dvd_rct, industry_bool, volume_df)
        int_rct = data_df_convert(int_rct, industry_bool, volume_df)
        time_deposits = data_df_convert(time_deposits, industry_bool, volume_df)
        monetary_cap = data_df_convert(monetary_cap, industry_bool, volume_df)
        derivative_fin_assets = data_df_convert(derivative_fin_assets, industry_bool, volume_df)
        tradable_fin_assets = data_df_convert(tradable_fin_assets, industry_bool, volume_df)
        red_monetary_cap_for_sale = data_df_convert(red_monetary_cap_for_sale, industry_bool, volume_df)
        fin_assets_avail_for_sale = data_df_convert(fin_assets_avail_for_sale, industry_bool, volume_df)
        held_to_mty_invest = data_df_convert(held_to_mty_invest, industry_bool, volume_df)
        invest_real_estate = data_df_convert(invest_real_estate, industry_bool, volume_df)
        # 以下为合计
        fin_asset = monetary_cap + derivative_fin_assets + tradable_fin_assets + red_monetary_cap_for_sale + \
                    fin_assets_avail_for_sale + held_to_mty_invest + invest_real_estate + dvd_rct + \
                    int_rct + time_deposits
        fin_liab = tot_cur_liab + lt_borrow + bonds_payable - acct_payable -  adv_from_cust - empl_ben_payable - \
                   taxes_surcharges_payable - oth_payable - acc_exp - deferred_inc - oth_cur_liab
        ans_df = (fin_liab - fin_asset)/10000 + market_value
        ans_df = pd.DataFrame(ans_df, dtype=np.float)
        # ----以下勿改动----
        ans_df = ans_df.loc[self.start_date: self.end_date]
        ans_df = Dtk.convert_df_index_type(ans_df, 'date_int', 'timestamp')
        return ans_df
