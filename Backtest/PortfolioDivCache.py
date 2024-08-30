"""
Created on 2018/8/16 by 006566
Updated on 2018/9/18 by 006566 重写了__query_div_info
Updated on 2018/10/22 by 006566 修正了__query_div_info中的错误

self.__stock_div_container = {} 用于暂时存储股票的分红送转信息。

对于组合净值计算模块来说，每日结算时需查询每只股票的分红送转信息，这一查询本质是基于SQL的，非常慢。
引入本模块后，对于输入的stock_code和query_date，查询缓存容器中是否有指定股票报告期的分红送转信息，如没有则去查一遍，
如有则再判断除权除息日（ex_dt）是否是查询日（query_date），如是则返回完整的除权除息信息，如不是，则返回的内容全是0。

注意，如查询日query_date发生在上半年，则报告期只会是上年年报期；如查询日发生在下半年，则需依次查当年半年报和上年年报。

对于函数get_query_day_div_info返回的内容，其格式与Dtk.get_query_day_div_info 和 Dtk.get_report_day_div_info一致
如查询日query_date恰好是除权除息日ex_dt，则返回的内容格式如下：
    {'per_div_trans': 0.3, 'per_cashpaidaftertax': 0.2, 'ex_dt': 20180619}
如查询日query_date不是除权除息日ex_dt，则返回的内容格式如下：
    {'per_div_trans': 0, 'per_cashpaidaftertax': 0, 'ex_dt': 0}

注意：PortfolioDivCache是一个Singleton，即只有一个实例；为了避免多次查询
"""
import DataAPI.DataToolkit as Dtk
import platform
from Utils.SingletonMeta import Singleton

if platform.system() == "Windows":  # 云桌面环境是Windows
    import DataAPI.quant_api as xq
else:  # XQuant环境是Linux
    import xquant.quant as xq
    from xquant.factor import FactorData
    xqf = FactorData()


class PortfolioDivCache(metaclass=Singleton):
    def __init__(self):
        self.__stock_div_container = {}
        # 缓存的容器是个字典，key的范例是"600549.SH20171231"
        # 内容的范例是{'per_div_trans': 0.3, 'per_cashpaidaftertax': 0.2, 'ex_dt': 20180619}
        self.__complete_stock_code = Dtk.get_complete_stock_list()

    def get_query_day_div_info(self, stock_code, query_date):
        query_date_year, query_date_monthday = divmod(query_date, 10000)
        query_date_month, _ = divmod(query_date_monthday, 100)
        # 如查询日期发生在上半年（例如20180531），那么分红的报告期仅可能是上年年报（例如20171231）
        if query_date_month <= 6:
            report_date = (query_date_year - 1) * 10000 + 1231
            report_day_div_info = self.__query_div_info(stock_code, report_date)
            if report_day_div_info['ex_dt'] == query_date:
                query_day_div_info = report_day_div_info
            else:
                query_day_div_info = {'per_cashpaidaftertax': 0, 'per_div_trans': 0, 'ex_dt': 0}
        else:  # 如查询日期发生在下半年，先尝试查询当年半年报期有无分红信息
            report_date = query_date_year * 10000 + 630
            report_day_div_info = self.__query_div_info(stock_code, report_date)
            # 如半年报有分红送转信息，就返回
            if report_day_div_info['ex_dt'] > 0 and report_day_div_info['ex_dt'] == query_date:
                query_day_div_info = report_day_div_info
            else:  # 如没有，则还要查一遍上年年报
                report_date = (query_date_year - 1) * 10000 + 1231
                report_day_div_info = self.__query_div_info(stock_code, report_date)
                if report_day_div_info['ex_dt'] == query_date:
                    query_day_div_info = report_day_div_info
                else:
                    query_day_div_info = {'per_cashpaidaftertax': 0, 'per_div_trans': 0, 'ex_dt': 0}
        return query_day_div_info

    def __query_div_info0(self, stock_code, report_date):  # 这是旧版，每只股票都查一遍；若需要查所有股票的，太慢了
        code_report_date = stock_code + str(report_date)  # 将代码和查询期变成"601688.SH20171231"这种key
        if code_report_date in self.__stock_div_container:  # 先查【容器】中有无记录
            report_day_div_info = self.__stock_div_container[code_report_date]  # 如有则直接返回
        else:  # 如没有，则试着查询
            temp_div_info = Dtk.get_report_day_div_info(stock_code, report_date)  # 并将查到的信息记录下来
            self.__stock_div_container[code_report_date] = temp_div_info
            report_day_div_info = temp_div_info
        return report_day_div_info

    def __query_div_info(self, stock_code, report_date):
        code_report_date_query = stock_code + str(report_date)  # 将代码和查询期变成"601688.SH20171231"这种key
        if code_report_date_query in self.__stock_div_container:  # 先查【容器】中有无记录
            report_day_div_info = self.__stock_div_container[code_report_date_query]  # 如有则直接返回
        else:  # 如没有，则试着查询；把所有股票在该report_date的都直接查一遍
            mass_div_info = xq.hfactor(self.__complete_stock_code, [xq.Factors.per_cashpaidaftertax, xq.Factors.ex_dt,
                                                                    xq.Factors.per_div_trans], report_date)
            stock_code_list = mass_div_info[2]
            mass_div_info = mass_div_info[0]
            for item in mass_div_info:
                if item[0] == 'per_cashpaidaftertax':
                    per_cashpaidaftertax_list = item[1]
                elif item[0] == 'ex_dt':
                    ex_dt_list = item[1]
                elif item[0] == 'per_div_trans':
                    per_div_trans_list = item[1]
            for i, stock_code in enumerate(stock_code_list):
                code_report_date_temp = stock_code + str(report_date)
                # 如果没有分红送转，那么ex_dt就是None；另外可能存在有分红送转的信息，但是ex_dt为''的情况，要排除这种坑
                if ex_dt_list[i][0] is not None and ex_dt_list[i][0].__len__() > 0:
                    temp_div_info = {'ex_dt': int(ex_dt_list[i][0]), 'per_div_trans': per_div_trans_list[i][0],
                                     'per_cashpaidaftertax': per_cashpaidaftertax_list[i][0]}
                else:
                    temp_div_info = {'ex_dt': 0, 'per_cashpaidaftertax': 0, 'per_div_trans': 0}
                self.__stock_div_container[code_report_date_temp] = temp_div_info
            report_day_div_info = self.__stock_div_container[code_report_date_query]
        return report_day_div_info
