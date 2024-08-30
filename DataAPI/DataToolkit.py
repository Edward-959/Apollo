"""
Created on 2018/8/8 by 006566 提供各种常用数据接口，
Latest updated on 8/27: 将云桌面版与XQuant版融合为一版
Latest updated on 8/31: 修复amt输出为千元的bug;  价量和adjfactor等数据优先读取保存于硬盘的
Latest updated on 9/6, by 006688 & 011673，新增对中信一级行业（industry3）和股票池的支持
Latest updated on 10/16: 对于get_panel_daily_info, 如要获取的end_date比从硬盘中读取的数据还要更晚，那么将从行情中心
        的接口在线获取差额数据（得到temp_df），temp_df的列要经过排序、与data_df相同，方可append
Latest updated on 10/23: 删去get_index_component这个函数
Latest updated on 11/8: get_panel_daily_pv_df新增twap
Latest updated on 11/20, 11/21: 彻底改造get_panel_daily_info的数据来源，支持xquant和云桌面两版
Latest updated on 11/23: 因xq.tradingDay在云桌面突然无法运行，故紧急修改了提取交易日的函数
Latest updated on 11/29: 新增对3个指数的支持：000001.SH, 399001.SZ, 399006.SZ
Latest updated on 2018/12/8: 为get_trading_day新增月频支持
Latest updated on 2019/1/31: 将XQuant的路径中006566改为666889, 新增GPU环境支持
Latest updated on 2019/2/17: 新增get_single_stock_minute_data2，读取分钟数据时可通过缓存读取
Latest updated on 2019/2/19: 将文件路径放到模块初始
Latest updated on 2019/2/21: 若在Windows环境运行，则get_single_stock_minute_data2强制转为用get_single_stock_minute_data
Latest updated on 2019/2/26: 新增2006-2008年交易日
Latest updated on 2019/2/28: 修正了get_panel_daily_info和start_date_backfill的错误;
                             get_panel_daily_info中Windows和XQuant代码合并; 支持的字段名根据新版h5因子库而调整
Latest updated on 2019/3/2: XQuant的朝阳永续库，更换路径
Latest updated on 2019/3/3: get_panel_daily_info修改bug: barra & complete_stock_list & convert_df_index_type
Latest updated on 2019/3/6: 为了顺应XQuant的变化，将'industry_citiccode'改为'CITIC_I'
Latest updated on 2019/3/13: 更改Barra因子路径；新增对buy_twap, buy_twap_fill_rate, sell_twap, sell_twap_fill_rate的支持
Latest updated on 2019/3/20: 更改朝阳永续数据路径
Latest updated on 2019/3/29: get_panel_daily_info新增optm_self_made: index_800
Latest updated on 2019/4/4: 新增get_daily_wind_quarterly_data，从XQuant的Wind落地库获取季频数据，并根据发布日转为日频
Latest updated on 2019/4/9: 修正back_fill函数的bug
Latest updated on 2019/4/12: get_panel_daily_info新增optm_self_made: alpha_uni_large, alpha_uni_mid, alpha_uni_small
Latest updated on 2019/4/17: 将原DataFrame.replace({None: np.nan, True: 1.0, False: 0.0})替换为DataFrame.add(0.0)
Latest updated on 2019/4/22: 为适应不同版本的py和pd，在get_panel_daily_info最后，强制将数据类型全部转为float，
                             以避免*df/df时除数为0报错的情况
Latest updated on 2019/4/24: 为get_daily_wind_quarterly_data新增qfa（单季数据）的类型
Latest updated on 2019/4/28: 指数权重和指数判定前移1天，若要提取当日（key为下一交易日）的数据，则利用量化平台xq
"""

import pandas as pd
import numpy as np
import os
import time
import datetime as dt
import platform
import DataAPI.GetTradingDay
from Utils.SingletonMeta import Singleton
from copy import deepcopy

if platform.system() == "Windows":  # 云桌面环境是Windows
    import DataAPI.quant_api as xq
    database_dir = "S:\\xquant_data_backup\AlphaDataBase"
    minute_data_root_path = "S:\\UnadjustedStockMinData\\MINUTE"
    barra_dir = "S:\Apollo\BarraFactors"
    running_platform = "Windows"
else:  # XQuant环境是Linux
    import xquant.quant as xq
    from xquant.factor import FactorData
    from xquant.multifactor.IO.IO import read_data

    xqf = FactorData()
    if os.system("nvidia-smi") == 0:
        database_dir = "/vipzrz/Apollo/AlphaDataBase"
        minute_data_root_path = "/vipzrz/UnadjustedStockMinData/MINUTE"
        barra_dir = "/app/data/666889/Apollo/BarraFactors/"
        running_platform = "Linux-GPU"
    else:
        database_dir = "/app/data/666889/Apollo/AlphaDataBase"
        minute_data_root_path = "/app/data/666889/UnadjustedStockMinData/MINUTE"
        barra_dir = "/app/data/666889/Apollo/BarraFactors/"
        running_platform = "Linux-CPU"


def get_trading_day(start_date, end_date, freq='D') -> list:
    """
    :param start_date: 起始日期
    :param end_date: 结束日期
    :param freq: 查询频率。默认是日频（'D'）。
                  如查询月频（'M'），则仿照Wind的“插入日期”的规则，输出start_date和end_date之间每个月末的最后一个
                  交易日，如end_date不是当月的最后一个交易日，那么再加上end_date（含end_date）前的最后一个交易日；
                  如start_date和end_date同属一个月、且其中包含超过一个交易日，那么输出之间的最后一个交易日。
                  参考以下3个例子：其中20181108和20181109是周四周五、是交易日；20181110和20181111是周末、不是交易日。
                  例1: t_d0 = trading_day(20181110, 20181111, 'M')  --- t_d0 = []
                  例2: t_d1 = trading_day(20181108, 20181111, 'M')  --- t_d1 = [20181109]
                  例3: t_d2 = trading_day(20181001, 20181111, 'M')  --- t_d2 = [20181031, 20181109]
    :return: 输出list，已按升序排列；如start_date和end_date都是交易日，则都将被包含进来（前闭后闭）
    """
    answer_list = DataAPI.GetTradingDay.trading_day(start_date, end_date, freq)
    return answer_list


def get_n_days_off(key_date, n_days_off):
    """
    2019/3/14 -- 011673重写了本函数，为了修改当n_days_off < -200的时候的bug
    2019/3/22 -- 006566修复了bug -- 如n_days_off > 0, 应当包含key_date
    """
    # 给定key_date, 取其前后n_days_off天的数据，并返回list，list的长度等于n_days_off，日期数值类型为int；
    # 如key_date是交易日，那么key_date应当被包含进来
    # 例如输入20180503, 3, 返回[20180503, 2080504, 20180507]
    # 或输入20180503, -3, 返回[20180427, 20180502, 20180503]
    # 如key_date是非交易日，先按照n_days_off的正负符号位移到最近的交易日，如n_days_off<0，则往前取到最近的交易日，反之
    # 则往后取到最近的交易日；然后再按n_days_off天数来涵盖
    # 例如输入2080501, 3；往后最近的交易日是20180502，故返回[20180502, 20180503, 20180504]
    # 或输入20180501, -3，往前最近的交易日是20180427，故返回[20180425, 20180426, 20180427]

    trading_day_list = DataAPI.GetTradingDay.get_complete_trading_day_list()

    def find_date(number: int, list_data: list):
        if number not in list_data:
            if (number >= list_data[-1]) or (number <= list_data[0]):
                raise Exception('input date is out of boundary')
            for i in range(0, (len(list_data) - 1)):
                if (list_data[i] < number) and (number < list_data[i + 1]):
                    return i
            raise Exception('input date is out of boundary')
        else:
            return list_data.index(number)

    if n_days_off < 0:
        index_number = find_date(key_date, trading_day_list)
        start_index = index_number + n_days_off
        if start_index + 1 >= 0:
            selected_list = trading_day_list[start_index + 1:index_number + 1]
        else:
            selected_list = []
    elif n_days_off == 0:
        selected_list = []
    else:
        if key_date in trading_day_list:
            index_number = find_date(key_date, trading_day_list)
        else:
            index_number = find_date(key_date, trading_day_list) + 1
        end_index = index_number + n_days_off
        if end_index + 1 < len(trading_day_list):
            selected_list = trading_day_list[index_number:end_index]
        else:
            selected_list = []
    return selected_list


def convert_date_or_time_int_to_datetime(date_time_input):
    # 将int型的日期或日期时间，转化为datetime型的；如输入单一值、则返回单一值；如输入list，则返回list
    # 可支持输入8位数的日期、例如20180509，也可支持输入14位数的日期时间、例如20180509145559
    # 但若输入的是list, 则list中元素的格式必须一样，不能8位和14位混淆
    if isinstance(date_time_input, list):
        original_input_is_list = True
    else:
        original_input_is_list = False
        date_time_input = [date_time_input]
    if (isinstance(date_time_input[0], int) or isinstance(date_time_input[0], np.int64)) \
            and str(date_time_input[0]).__len__() == 8:
        date_list_str = [str(i_date) for i_date in date_time_input]
        answer_list = [dt.datetime(int(i_date[0:4]), int(i_date[4:6]), int(i_date[6:8])) for i_date in date_list_str]
    elif (isinstance(date_time_input[0], int) or isinstance(date_time_input[0], np.int64)) \
            and str(date_time_input[0]).__len__() == 14:
        date_time_list_str = [str(i_date_time) for i_date_time in date_time_input]
        answer_list = [
            dt.datetime(int(i_date_time[0:4]), int(i_date_time[4:6]), int(i_date_time[6:8]), int(i_date_time[8:10]),
                        int(i_date_time[10:12]), int(i_date_time[12:14])) for i_date_time in date_time_list_str]
    else:
        print("function convert_date_or_time_int_to_datetime: input type or format error")
        answer_list = []
    if not original_input_is_list:
        answer_list = answer_list[0]
    return answer_list


def get_query_day_div_info(stock_code: str, query_date: int) -> dict:
    """
    返回查询日的除权除息信息，如查询日不是除权除息日（注意多数时候都不是），则所有值都是0
    :param stock_code: string, eg. '600549.SH' —— 股票代码
    :param query_date: int, eg. 20180619 —— 查询分红送转信息的日期
    :return: 返回一个如下例的字典，如不是除权除息日（注意多数时候都不是），则所有值都是0
    {'per_div_trans': 0.3, 'per_cashpaidaftertax': 0.2, 'ex_dt': 20180619}
    查询红利信息，得到的hfactor的格式如下，这是一个长度为3的list，其中第1个list的3个内容分别是现金分红金额、除权除息日、
    和分红送转比例（但顺序不一定）；第2个list是报告期；第3个list是股票代码
    [['per_cashpaidaftertax',[[0.2]]],['ex_dt', [['20180619']]], ['per_div_trans',[[0.3]]]], [20171231], ['600549.SH']
    """
    query_date_year, query_date_monthday = divmod(query_date, 10000)
    query_date_month, _ = divmod(query_date_monthday, 100)
    if query_date_month <= 6:  # 如查询日期发生在上半年（例如20180531），那么分红的报告期仅可能是上年年报（例如20171231）
        report_date = (query_date_year - 1) * 10000 + 1231
        factor_data = xq.hfactor([stock_code],
                                 [xq.Factors.per_cashpaidaftertax, xq.Factors.ex_dt, xq.Factors.per_div_trans],
                                 [report_date])
    else:
        report_date = query_date_year * 10000 + 630  # 如查询日期发生在下半年，先尝试查询当年半年报期有无分红信息
        factor_data = xq.hfactor([stock_code],
                                 [xq.Factors.per_cashpaidaftertax, xq.Factors.ex_dt, xq.Factors.per_div_trans],
                                 [report_date])
        if factor_data[0].__len__() == 0:  # 如当年半年报期无分红信息，则查询上年年报期
            report_date = (query_date_year - 1) * 10000 + 1231
            factor_data = xq.hfactor([stock_code],
                                     [xq.Factors.per_cashpaidaftertax, xq.Factors.ex_dt, xq.Factors.per_div_trans],
                                     [report_date])
    # 因为factorData的形式（如有值的话），且顺序不定；故要逐个循环放进字典divInfoReportDate中
    # 下例分别是现金红利（每10股2元），送转比例（每10股3股），除权除息日（2018年6月19日）
    # [['per_cashpaidaftertax', [[0.2]]], ['per_div_trans', [[0.3]]], ['ex_dt', [['20180619']]]]
    if factor_data[0].__len__() == 0:  # 如无分红送转信息
        return {'per_cashpaidaftertax': 0, 'per_div_trans': 0, 'ex_dt': 0}
    div_info_report_date = {}
    for content in factor_data[0]:
        if content[1][0][0] == str('') and content[0] == 'ex_dt':  # 如只是公告了送转、但没有公告除权除息日
            return {'per_cashpaidaftertax': 0, 'per_div_trans': 0, 'ex_dt': 0}
        elif content[0] == 'ex_dt' and int(content[1][0][0]) > query_date:  # 如除权除息日晚于查询日
            return {'per_cashpaidaftertax': 0, 'per_div_trans': 0, 'ex_dt': 0}
        elif isinstance(content[1][0][0], float):
            div_info_report_date[content[0]] = content[1][0][0]
        else:
            div_info_report_date[content[0]] = int(content[1][0][0])  # 得到的日期是str, 要转为int
    if div_info_report_date['ex_dt'] == query_date:  # 如查询日恰好是除权除息日
        return div_info_report_date
    else:
        return {'per_cashpaidaftertax': 0, 'per_div_trans': 0, 'ex_dt': 0}


def get_report_day_div_info(stock_code: str, report_date: int) -> dict:
    """
    返回报告期的除权除息信息，注意除权除息日ex_date（乃至公告ex_date的日期）都可能晚于查询日，所以可能会引入未来信息
    使用时务必小心
    :param stock_code: string, eg. '600549.SH' —— 股票代码
    :param report_date: int, eg. 20180619 —— 查询分红送转信息的日期
    :return: 返回一个如下例的字典，有些股票可能会先公布送转或分红比例、但未公布除权除息日，这种情况返回的值为全0
    {'per_div_trans': 0.3, 'per_cashpaidaftertax': 0.2, 'ex_dt': 20180619}
    """
    factor_data = xq.hfactor([stock_code],
                             [xq.Factors.per_cashpaidaftertax, xq.Factors.ex_dt, xq.Factors.per_div_trans],
                             [report_date])
    if factor_data[0].__len__() == 0:  # 如无分红送转信息
        return {'per_cashpaidaftertax': 0, 'per_div_trans': 0, 'ex_dt': 0}
    div_info_report_date = {}
    for content in factor_data[0]:
        if content[1][0][0] == str('') and content[0] == 'ex_dt':  # 如只是公告了送转、但没有公告除权除息日
            return {'per_cashpaidaftertax': 0, 'per_div_trans': 0, 'ex_dt': 0}
        elif isinstance(content[1][0][0], float):
            div_info_report_date[content[0]] = content[1][0][0]
        else:
            div_info_report_date[content[0]] = int(content[1][0][0])  # 得到的日期是str, 要转为int
    return div_info_report_date


def get_listed_stocks(query_date: int, market_type='ALLA') -> list:
    # 返回在query_date当天上市的A股股票列表，默认是全A（ALL），还支持SHA、SZA、SME（中小板）和GEM（创业板）等另外4种
    # query_date如不是交易日，则会上溯至最近的一个交易日
    query_date = get_n_days_off(query_date, -1)[0]
    if market_type == 'ALLA':
        factor_data = xq.hset(xq.PlateType.MARKET, query_date, xq.MarketType.ALLA)
    elif market_type == 'SHA':
        factor_data = xq.hset(xq.PlateType.MARKET, query_date, xq.MarketType.SHA)
    elif market_type == 'SZA':
        factor_data = xq.hset(xq.PlateType.MARKET, query_date, xq.MarketType.SZA)
    elif market_type == 'SME':
        factor_data = xq.hset(xq.PlateType.MARKET, query_date, xq.MarketType.SME)
    elif market_type == 'GEM':
        factor_data = xq.hset(xq.PlateType.MARKET, query_date, xq.MarketType.GEM)
    else:
        factor_data = xq.hset(xq.PlateType.MARKET, query_date, xq.MarketType.ALLA)
    stock_code_list = factor_data[0]
    return stock_code_list


def get_complete_stock_list(end_date=None, drop_delisted_stocks=False) -> list:
    # v1: 获取所有股票的并集（从2013年至今），返回一个股票代码list，其中含有退市股票；为了简化起见，股票列表固定为3542只
    # 2018年8月14日以后的新股也不更新
    # v2: 获取所有股票的并集（从2009年至今），新增end_date变量，若end_date不为None,则返回上市日和退市日都在end_date之前
    # 的股票列表
    complete_stock_list = []
    if running_platform == "Windows":
        complete_stock_list_path = "S:\\xquant_data_backup\AlphaDataBase\CompleteStockList.csv"
    elif running_platform == "Linux-GPU":
        complete_stock_list_path = "/vipzrz/Apollo/AlphaDataBase/CompleteStockList.csv"
    else:
        complete_stock_list_path = "/app/data/666889/Apollo/AlphaDataBase/CompleteStockList.csv"
    if os.path.exists(complete_stock_list_path):
        df = pd.read_csv(complete_stock_list_path)
        df = df.fillna(0)
        if drop_delisted_stocks:
            df = df[df.Delisting_date < 1]
        if end_date is None:
            complete_stock_list = df['Stock_code'].tolist()
        else:
            df = df[(df.Listing_date <= end_date) & (df.Delisting_date <= end_date)]
            complete_stock_list = df['Stock_code'].tolist()
    else:
        print("Error: cannot find the CompleteStockList file")
    return complete_stock_list


def get_panel_daily_pv_df(stock_list, start_date_int, end_date_int, pv_type='close', adj_type='NONE') -> pd.DataFrame:
    """
    获取日级别的价量（pv - price & volume）行情，形式是panel data，即[股票list * 日期list]
    :param stock_list:  股票代码列表，e.g., ['000001.SZ', '600000.SH', '601688.SH']
    :param start_date_int: 查询起始日，e.g., 20180801，如当天是交易日，则将被包括进来
    :param end_date_int: 查询终止日， e.g., 20180808，如当天是交易日，则将被包括进来
    :param pv_type: 查询类型，目前支持'close', 'open', 'high', 'low', 'pre_close', 'volume', 'amt', 'pct_chg',
                                       'turn', 'twap', 'buy_twap', 'buy_twap_fill', 'sell_twap', 'sell_twap_fill'
                                        等共14种; updated on 2019/3/13
    :param adj_type: 复权类型，'NONE'-不复权，'FORWARD'-（从上市日）向后复权， 'BACKWARD2' - 从end_date_int日向前复权；
                      仅对close, open, high, low, pre_close有效，对volume, amt和pct_chg无意义；对指数无意义
    :return: 返回类型为DataFrame, 行为日期，列为股票代码
    """
    if pv_type in ['close', 'open', 'high', 'low', 'pre_close', 'volume', 'amt', 'pct_chg', 'turn', 'twap',
                   'buy_twap', 'buy_twap_fill_rate', 'sell_twap', 'sell_twap_fill_rate']:
        pass
    else:
        raise TypeError
    available_index_list = ["000001.SH", "000016.SH", "000300.SH", "000905.SH", "000906.SH", "399001.SZ",
                            "399006.SZ"]
    for available_index in available_index_list:  # 如要取指数的数据，但涉及到复权因子或复权价格
        if available_index in stock_list and adj_type is not 'NONE':
            raise Exception('Adjustment price is not applicable to index data')
    query_date_list = get_trading_day(start_date_int, end_date_int)  # 获取起止日期list
    start_date_int = query_date_list[0]  # 确保start_date_int是交易日
    end_date_int = query_date_list[-1]  # 确保end_date_int是交易日，以利于后续计算BACKWARD2（前复权）时定位用
    data_file = "Data_" + pv_type + ".h5"
    data_full_path = os.path.join(database_dir, data_file)
    store = pd.HDFStore(data_full_path, mode='r')
    data_df = store.select("/factor")
    store.close()
    data_df = data_df.loc[start_date_int: end_date_int]
    data_df = data_df.reindex(columns=stock_list)
    if adj_type == "FORWARD" and pv_type in ['open', 'high', 'low', 'close', 'pre_close', 'twap', 'buy_twap',
                                             'sell_twap']:
        adj_df = get_panel_daily_info(stock_list, start_date_int, end_date_int, 'adjfactor')
        data_df = data_df * adj_df
    elif adj_type == "BACKWARD2" and pv_type in ['open', 'high', 'low', 'close', 'pre_close', 'twap', 'buy_twap',
                                                 'sell_twap']:
        adj_df = get_panel_daily_info(stock_list, start_date_int, end_date_int, 'adjfactor')
        data_df = data_df * adj_df / adj_df.loc[end_date_int]
    # 因为Python原生的None在pandas/numpy中兼容性不好，影响读写以及在其他模块中的调用，这里转为np.nan
    data_df = data_df.add(0.0)
    if pv_type == 'volume':  # 因得到的VOLUME单位是“手”，这里转为“股”
        data_df = data_df.mul(100)
    elif pv_type == 'amt':  # 因得到的AMOUNT单位是“千元”，这里转为“元”
        data_df = data_df.mul(1000)
    data_df = data_df.reindex(columns=stock_list)  # 使输出的列名顺序等于输入的顺序
    return data_df


def get_panel_daily_info(stock_list, start_date_int, end_date_int, info_type, output_index_type='date_int') \
        -> pd.DataFrame:
    """
    updated 2019/2/24 by 011672 修正了stock_universe和industry两类数据的S盘及XQuant接口
    updated 2018/9/6 by 006688, 新增中信一级行业（info_type是industry3）
    updated 2018/11/20 by 006566，彻底更改数据来源，只有adjfactor是自己维护的，其他字段不再维护
    updated 2019/2/28 by 006566，适应新版XQuant的h5因子库; 另外，对应md范畴的14个字段，以及alpha_universe和
                                 risk_universe直接读取Data_文件
    获取日级别的信息，形式是panel data，即[股票list * 日期list]；因为不是为取价量而设计的，所以这里没加入复权选项
    :param stock_list:  股票代码列表，e.g., ['000001.SZ', '600000.SH', '601688.SH']
    :param start_date_int: 查询起始日，e.g., 20180801，如当天是交易日，则将被包括进来
    :param end_date_int: 查询终止日， e.g., 20180808，如当天是交易日，则将被包括进来
    :param info_type: 查询的信息类型
    :param output_index_type: 返回的DataFrame的索引的类型，默认是'date_int'，可改为'timestamp'
    :return: 返回类型为DataFrame, 行为日期，列为股票代码；如查询不到，则返回空的DataFrame
    """
    query_date_list = get_trading_day(start_date_int, end_date_int)  # 获取起止日期list
    start_date_int = query_date_list[0]  # 确保start_date_int是交易日
    end_date_int = query_date_list[-1]  # 确保end_date_int是交易日
    all_key_set = return_panel_info_complete_key_set()
    if info_type in all_key_set:
        pass
    else:
        raise TypeError
    # 对应md范畴的14个字段，以及alpha_universe和risk_universe直接读取Data_文件
    if info_type in ['pre_close', 'open', 'high', 'low', 'close', 'vwap', 'pct_chg', 'turn', 'volume', 'amt',
                     'adjfactor', 'mkt_cap_ard', 'free_float_shares', 'total_shares', 'alpha_universe',
                     'risk_universe']:
        data_file = "Data_" + info_type + ".h5"
        data_full_path = os.path.join(database_dir, data_file)
        store = pd.HDFStore(data_full_path, mode='r')
        data_df = store.select("/factor")
        store.close()
        data_df = data_df.loc[start_date_int: end_date_int]
        data_df = data_df.reindex(columns=stock_list)
    elif info_type in ['Beta', 'EarningsYield', 'Growth', 'Leverage', 'Liquidity', 'Momentum', 'NonLinearSize',
                       'ResidualVolatility', 'Size', 'Value']:
        data_file = "F_B_" + info_type + ".h5"
        data_full_path = os.path.join(barra_dir, data_file)
        store = pd.HDFStore(data_full_path, mode='r')
        data_df = store.select("/factor")
        store.close()
        data_df = convert_df_index_type(data_df, 'timestamp', 'date_int')
        data_df = data_df.loc[start_date_int: end_date_int]
        data_df = data_df.reindex(columns=stock_list)
    else:
        data_df = pd.DataFrame()
        data_path, key_list = key_related_search(info_type)
        if info_type == 'industry3':  # 将原来的industry3改为CITIC_I
            info_type = 'CITIC_I'
        if running_platform in ["Linux-CPU", "Linux-GPU"] and key_list == "barra":
            data_df = xqf.getData([info_type], (str(start_date_int), str(end_date_int)), [])
            data_df = unfold_df(data_df)
            data_df = convert_df_index_type(data_df, 'str', 'date_int')
            data_df = data_df.reindex(query_date_list)  # 因getData中会引入非交易日，故要以交易日来reindex
        elif key_list in ["industry", "fdd_d", "universe_complete", "barra"]:
            store = pd.HDFStore(data_path, mode='r')
            data_df = store.select("/" + info_type)
            store.close()
            data_df = unfold_df(data_df)
            data_df = convert_df_index_type(data_df, 'timestamp2', 'date_int')
            data_df = data_df.loc[start_date_int: end_date_int]
            data_df = data_df.add(0.0)  # 将Bool值转成1/0，否则后续在*universe/universe时会出错
        elif key_list == "optm":  # optm与industry, fdd和universe_complete等表不同，每列无列名，因此不能用unfold_df函数
            if info_type in ['index_300', 'index_50', 'index_500', 'index_weight_hs300', 'index_weight_sh50',
                             'index_weight_zz500']:
                store = pd.HDFStore(data_path, mode='r')
                data_df = store.select("/" + info_type)
                store.close()
                data_df = data_df.unstack()  # optm表的每列没有列名（字段名），因此直接unstack即可
                data_df = convert_df_index_type(data_df, 'timestamp2', 'date_int')
                data_df = data_df.fillna(0)
                data_df = data_df.shift(-1)  # 指数信息要用下一天的数据
                data_df = data_df.loc[start_date_int: end_date_int]
                data_df = data_df.add(0.0)  # 将Bool值转成1/0，否则后续在*universe/universe时会出错

                # 如end_date最后一行是nan，则要以下一天为key从量化平台取权重
                if np.isnan(data_df.iloc[-1, 0]):
                    last_day = list(data_df.index)[-1]
                    last_day_int_plus1 = get_n_days_off(last_day, 2)[-1]
                    xq_info = __return_xq_type_param(info_type)  # 将info_type转化为量化平台的支持的对象
                    xq_data = xq.hset(xq.PlateType.INDEX, last_day_int_plus1, xq_info)  # 从量化平台提取指数成分
                    if xq_data.__len__() > 1:
                        temp_df = pd.DataFrame(xq_data[2], index=xq_data[0], columns=[last_day])
                        temp_df = temp_df.T
                        temp_df = temp_df.div(100)
                        if info_type in ['index_300', 'index_50', 'index_500']:
                            temp_df = temp_df.div(temp_df)  # 这3者输出的是1/0
                        temp_df = temp_df.reindex(columns=data_df.columns)
                        temp_df = temp_df.fillna(0)
                        data_df.iloc[-1] = temp_df.iloc[0]
            else:
                store = pd.HDFStore(data_path, mode='r')
                data_df = store.select("/" + info_type)
                store.close()
                data_df = data_df.unstack()  # optm表的每列没有列名（字段名），因此直接unstack即可
                data_df = convert_df_index_type(data_df, 'timestamp2', 'date_int')
                data_df = data_df.loc[start_date_int: end_date_int]
                data_df = data_df.add(0.0)  # 将Bool值转成1/0，否则后续在*universe/universe时会出错
        elif key_list == "optm_self_made":
            if info_type == "index_800":
                store = pd.HDFStore(data_path, mode='r')
                data_df0 = store.select("/" + "index_300")
                data_df1 = store.select("/" + "index_500")
                store.close()
                data_df0 = data_df0.unstack()   # optm表的每列没有列名（字段名），因此直接unstack即可
                data_df1 = data_df1.unstack()
                data_df = data_df0 + data_df1
                data_df = convert_df_index_type(data_df, 'timestamp2', 'date_int')
                data_df = data_df.shift(-1)  # 指数信息要用下一天的数据
                data_df = data_df.loc[start_date_int: end_date_int]
                data_df = data_df.add(0.0)  # 将Bool值转成1/0，否则后续在*universe/universe时会出错

                # 如end_date最后一行是nan，则要以下一天为key从量化平台取权重
                if np.isnan(data_df.iloc[-1, 0]):
                    last_day = list(data_df.index)[-1]
                    last_day_int_plus1 = get_n_days_off(last_day, 2)[-1]
                    xq_info0 = __return_xq_type_param('index_weight_hs300')  # 将info_type转化为量化平台的支持的对象
                    xq_data0 = xq.hset(xq.PlateType.INDEX, last_day_int_plus1, xq_info0)  # 从量化平台提取指数成分
                    xq_info1 = __return_xq_type_param('index_weight_zz500')  # 将info_type转化为量化平台的支持的对象
                    xq_data1 = xq.hset(xq.PlateType.INDEX, last_day_int_plus1, xq_info1)  # 从量化平台提取指数成分
                    if xq_data0.__len__() > 1:
                        temp_df0 = pd.DataFrame(xq_data0[2], index=xq_data0[0], columns=[last_day])
                        temp_df0 = temp_df0.T
                        temp_df0 = temp_df0.div(100)
                        temp_df1 = pd.DataFrame(xq_data1[2], index=xq_data1[0], columns=[last_day])
                        temp_df1 = temp_df1.T
                        temp_df1 = temp_df1.div(100)
                        if info_type in ['index_800']:
                            temp_df0 = temp_df0.div(temp_df0)  # 这3者输出的是1/0
                            temp_df1 = temp_df1.div(temp_df1)
                        temp_df0 = temp_df0.reindex(columns=data_df.columns)
                        temp_df1 = temp_df1.reindex(columns=data_df.columns)
                        temp_df0 = temp_df0.fillna(0)
                        temp_df1 = temp_df1.fillna(0)
                        temp_df = temp_df0 + temp_df1
                        data_df.iloc[-1] = temp_df.iloc[0]

            elif info_type in ["alpha_uni_large", "alpha_uni_mid", "alpha_uni_small"]:
                # 在alpha_universe中，按市值大小三等分
                # 以下分别读取mkt_cap_ard和alpha_universe
                data_file = "Data_mkt_cap_ard.h5"
                data_full_path = os.path.join(database_dir, data_file)
                store = pd.HDFStore(data_full_path, mode='r')
                mkt_cap_ard = store.select("/factor")
                store.close()
                mkt_cap_ard = mkt_cap_ard.loc[start_date_int: end_date_int]

                data_file = "Data_alpha_universe.h5"
                data_full_path = os.path.join(database_dir, data_file)
                store = pd.HDFStore(data_full_path, mode='r')
                alpha_universe = store.select("/factor")
                store.close()
                alpha_universe = alpha_universe.reindex(index=mkt_cap_ard.index, columns=mkt_cap_ard.columns)

                # 将总市值按alpha_universe过滤，然后排序
                mkt_cap_ard = mkt_cap_ard * alpha_universe / alpha_universe
                mkt_cap_ard_rank = mkt_cap_ard.rank(axis=1)
                univ_stock_num = mkt_cap_ard_rank.max(axis=1)  # 每日alpha_universe中的股票数量

                # 三等分标记
                small_cap_df = mkt_cap_ard_rank.sub(univ_stock_num/3, axis=0)
                large_cap_df = mkt_cap_ard_rank.sub(univ_stock_num*2/3, axis=0)

                if info_type == "alpha_uni_small":  # 三等分小市值
                    data_df = small_cap_df < 0
                elif info_type == "alpha_uni_large":  # 三等分大市值
                    data_df = large_cap_df > 0
                else:  # 三等分中市值
                    data_df = (small_cap_df >= 0) & (large_cap_df <= 0)
                data_df = data_df.add(0.0)
                data_df = data_df.reindex(columns=stock_list)
            else:
                raise TypeError
        elif key_list == 'fdd_q':
            store = pd.HDFStore(data_path, mode='r')
            data_df_info = store.select("/" + info_type)
            ann_df = store.select("/stm_issuingdate")
            store.close()
            ann_df = unfold_df(ann_df)
            data_df_info = unfold_df(data_df_info)
            ann_df = convert_df_index_type(ann_df, 'timestamp2', 'date_int')
            data_df_info = convert_df_index_type(data_df_info, 'timestamp2', 'date_int')
            ann_df = ann_df.fillna(0)
            data_df_info = data_df_info.reindex(columns=ann_df.columns)
            last_report_date = start_date_backfill(start_date_int)
            trading_day_1 = get_trading_day(last_report_date, end_date_int)
            data_df_raw = pd.DataFrame(index=trading_day_1, columns=stock_list)
            data_df = back_fill(data_df_raw, data_df_info, ann_df)
            data_df = data_df.loc[start_date_int:]
    if output_index_type == 'timestamp':
        data_df = convert_df_index_type(data_df, 'date_int', 'timestamp')
    # 因为Python原生的None在pandas/numpy中兼容性不好，影响读写以及在其他模块中的调用，这里转为np.nan
    data_df = data_df.add(0.0)
    data_df = data_df.reindex(columns=stock_list)
    data_df[stock_list] = data_df[stock_list].astype('float') # 将1/0转化为1.0/0.0，否则在*universe/universe时会出错
    data_df.index.name = 'index'  # 使输出的index的名字是'index'，其实没有实际意义
    return data_df


def __return_xq_type_param(info_type):
    info_type_dict = {'close': xq.Factors.close, 'open': xq.Factors.open, 'high': xq.Factors.high,
                      'low': xq.Factors.low, 'pre_close': xq.Factors.pre_close, 'amt': xq.Factors.amt,
                      'volume': xq.Factors.volume, 'pct_chg': xq.Factors.pct_chg,
                      'index_weight_hs300': xq.IndexType.HS300, 'index_300': xq.IndexType.HS300,
                      'index_weight_sh50': xq.IndexType.SH50, 'index_50': xq.IndexType.SH50,
                      'index_weight_zz500': xq.IndexType.ZZ500, 'index_500': xq.IndexType.ZZ500}
    if info_type in info_type_dict.keys():
        info_type_param = info_type_dict[info_type]
    else:  # 如传入的Key不对，则选择'close'
        info_type_param = info_type_dict['close']
    return info_type_param


def __return_adj_type_param(adj_type):
    adj_type_dict = {'NONE': xq.FactorType.UNADJUSTED, 'BACKWARD': xq.FactorType.FORWARD,
                     'FORWARD': xq.FactorType.BACKWARD, 'BACKWARD2': xq.FactorType.UNADJUSTED}
    # 注，BACKWARD指从最新日向前复权（最新日价格不变，以前的价格复权），此处wind API命名有误
    # FORWARD指从上市日起向后复权（起始日价格不变，后续的价格复权），此处wind API命名有误
    # BACKWARD2指从查询期间的最后一天向前复权，wind API中没有这种方式，这是自行开发的
    if adj_type in adj_type_dict.keys():
        adj_type_param = adj_type_dict[adj_type]
    else:
        adj_type_param = adj_type_dict['NONE']  # 如传入的Key不对，则选择不复权
    return adj_type_param


def __return_info_type_param(query_info_type):
    info_type_dict = {'vwap': xq.Factors.vwap, 'chg': xq.Factors.chg, 'pct_chg': xq.Factors.pct_chg,
                      'turn': xq.Factors.turn, 'free_turn': xq.Factors.free_turn, 'dealnum': xq.Factors.dealnum,
                      'swing': xq.Factors.swing, 'lastradeday_s': xq.Factors.lastradeday_s,
                      'last_trade_day': xq.Factors.last_trade_day, 'adjfactor': xq.Factors.adjfactor,
                      'maxupordown': xq.Factors.maxupordown, 'total_shares': xq.Factors.total_shares,
                      'free_float_shares': xq.Factors.free_float_shares, 'float_a_shares': xq.Factors.float_a_shares,
                      'share_totala': xq.Factors.share_totala, 'mkt_cap_ard': xq.Factors.mkt_cap_ard,
                      'a_mkt_cap': xq.Factors.a_mkt_cap, 'pe_ttm': xq.Factors.pe_ttm, 'pe_lyr': xq.Factors.pe_lyr,
                      's_val_pb_new': xq.Factors.s_val_pb_new, 's_price_div_dps': xq.Factors.s_price_div_dps,
                      'ps_ttm': xq.Factors.ps_ttm, 'ps_lyr': xq.Factors.ps_lyr, 'pcf_ocf_ttm': xq.Factors.pcf_ocf_ttm,
                      'pcf_ncf_ttm': xq.Factors.pcf_ncf_ttm, 'pcf_ocflyr': xq.Factors.pcf_ocflyr,
                      'pcf_ncflyr': xq.Factors.pcf_nflyr}
    if query_info_type in info_type_dict.keys():
        info_type_param = info_type_dict[query_info_type]
    else:
        print(query_info_type, "not supported")
        info_type_param = []
    return info_type_param


def get_single_stock_minute_data(stock_code: str, start_date: int, end_date: int, fill_nan: bool = True,
                                 append_pre_close: bool = False, adj_type: str = 'NONE', drop_nan: bool = False,
                                 full_length_padding: bool = True) \
        -> pd.DataFrame:
    """
    更新：2018/8/26 by 006566, 引入full_length_padding这一属性.
    更新：2018/8/29 by 006566 & 011673, 考虑了fill_nan, drop_nan和full_length_padding之间的冲突，对冲突的情况做了定义.
    更新：2018/9/4 by 006566, 追加了对指数分钟行情的支持, 对指数而言, adj_type不能是'NONE'之外的其他值.
    更新：2018/9/8 by 006566，对输出结果做了一个修正：如股票当天全天无成交，则当天全天的价量都为nan
    :param stock_code: 股票代码，e.g., '601688.SH'.
    :param start_date: 开始日期，e.g., 20180701，数值类型是int，如当天是交易日，则将被包括进来.
    :param end_date: 结束日期，e.g., 20180720，数值类型是int，如当天是交易日，则将被包括进来.
    :param fill_nan: 填充nan值，如为False - 行情中若有nan值则不作任何填充；
                      如为True - 行情中遇到nan值的，若nan值是价格则以前值填充，若nan值是成交量或成交额以0填充；
                      但如遇到第1天第1分钟就是nan的，则上溯至前1交易日的行情，但如前1交易日全天都为nan（例如未上市），
                      则只能取到nan，不会再往前无限回溯.
                      【注意】如果fill_nan和drop_nan同时为True，逻辑上是矛盾的，这时我们会报错.
    :param append_pre_close: 加入pre_close（当然是不复权的）, 不强制加入是为了节约时间.
    :param adj_type: 复权方式，'NONE'-不复权，'FORWARD'-（从上市日）向后复权，'BACKWARD2' - 从end_date_int日向前复权；
                      所有复权仅对5种价格有效，对volume和amt无意义.
    :param drop_nan: 若为True, 则当遇到有nan值或None值的记录（行）时删去这一条记录（这一行）.
                     【注意】如果fill_nan和drop_nan同时为True，逻辑上是矛盾的，这时我们会报错.
    :param full_length_padding: 全长补齐，强行确保输出的DataFrame的行数等于start_date至end_date期间交易日的天数*242，
                                （头尾两天如是交易日的话则都包含），那么对于未上市或已退市的股票，在未上市或已退市期间
                                的值将全由nan填充.
                                【注意】如drop_nan也为True, 那么因drop_nan被删掉的记录（行）会被重新以nan补齐.
    :return: 返回一个双重索引的DataFrame, 索引是日期(dt)和分钟(minute)，内容是open, high, low, close, volume, amt等，
              每天都是242根Bar线.
    """
    if fill_nan and drop_nan:
        raise Exception('Logic error: fill_nan and drop_nan were True at the same time')
    # 目前支持7个指数：上证50、沪深300、中证500和中证800、上证综指、深证成指和创业板指
    if stock_code in ["000001.SH", "000016.SH", "000300.SH", "000905.SH", "000906.SH", "399001.SZ", "399006.SZ"]:
        dir_path = os.path.join(minute_data_root_path, "index")
        file_name = "indexMinute_" + stock_code[0:6] + ".pkl"
    else:
        dir_path = os.path.join(minute_data_root_path, "stock")
        file_name = "UnAdjstedStockMinute_" + stock_code[0:6] + ".pkl"
    if os.path.exists(os.path.join(dir_path, file_name)):
        minute_data = pd.read_pickle(os.path.join(dir_path, file_name), compression='gzip')
    elif not full_length_padding:
        print("minute data file of", stock_code, "does not exist")
        minute_data = pd.DataFrame()
        return minute_data
    else:
        print("minute data file of", stock_code, "does not exist; but a DataFrame fulfilled with nan will be returned")
        date_list = get_trading_day(start_date, end_date)
        complete_minute_list = get_complete_minute_list()  # 每天242根完整的分钟Bar线的分钟值
        i_stock_minute_data_full_length = date_list.__len__() * 242
        index_tuple = [np.repeat(date_list, len(complete_minute_list)), complete_minute_list * len(date_list)]
        mi_index = pd.MultiIndex.from_tuples(list(zip(*index_tuple)), names=['dt', 'minute'])
        minute_data = pd.DataFrame(index=mi_index)  # 新建一个空的DataFrame, 且先设好了索引
        temp_array = np.empty(shape=[i_stock_minute_data_full_length, ])
        temp_array[:] = np.nan
        if not append_pre_close:
            for col in ['open', 'high', 'low', 'close', 'volume', 'amt']:
                minute_data[col] = temp_array
        else:
            for col in ['open', 'high', 'low', 'close', 'volume', 'amt', 'pre_close']:
                minute_data[col] = temp_array
        return minute_data
    date_list_of_stock_file = list(set(minute_data.index.get_level_values(level=0)))  # 从数据文件中可取到的日期列表
    date_list_of_stock_file.sort()
    requested_date_list = get_trading_day(start_date, end_date)  # 希望获取的交易日列表
    # 找出希望获取的交易日列表中、数据文件中没有的日期，记为missing_trade_dates
    missing_trade_dates = list(set(requested_date_list).difference(set(date_list_of_stock_file)))
    requested_date_list.sort()
    missing_trade_dates.sort()
    compressed_missing_trade_dates = __compress_missing_dates(requested_date_list, missing_trade_dates)
    if missing_trade_dates.__len__() > 0:
        print(stock_code, "no minute data for these days:", compressed_missing_trade_dates)  # 这些交易日没有分钟数据
    if fill_nan and not drop_nan:  # 如需要填充NaN值
        start_date_minus_1 = get_n_days_off(int(start_date), -2)[0]  # 取start_date前1个交易日
        # 取出start_date_minus_1——往前取1天是为了：如第1天开盘即有缺失的，上溯至前一交易日行情填充
        minute_data = minute_data.loc[start_date_minus_1: end_date].copy()
    else:  # 如不填充NaN值
        minute_data = minute_data.loc[start_date: end_date].copy()
    if fill_nan and not drop_nan:
        # 填充缺失值，价格用前值填充，成交量和成交额用0填充
        minute_data['close'] = minute_data['close'].fillna(method='ffill')
        minute_data['open'] = minute_data['open'].fillna(method='ffill')
        minute_data['high'] = minute_data['high'].fillna(method='ffill')
        minute_data['low'] = minute_data['low'].fillna(method='ffill')
        minute_data['amt'] = minute_data['amt'].fillna(0)
        minute_data['volume'] = minute_data['volume'].fillna(0)
    elif drop_nan:  # 如遇到nan的，就把整行记录删掉
        minute_data = minute_data.dropna(how='any')
    value_counts = pd.value_counts(minute_data['minute'])
    minute_data = minute_data.reset_index().drop('Ticker', axis=1)  # 因读到的原始数据是multi-index，这里重设一下
    # 如要“全长补齐”，则输出的日期等于start_date和end_date期间的日期，若未上市期间或已退市期间的，也会以nan补齐
    if full_length_padding:
        date_list = requested_date_list
    else:  # 如无需“全长补齐”，则输出的日期等于分钟数据文件中有的日期
        date_list = list(set(minute_data['dt']))
    date_list.sort()
    day_amt = minute_data.groupby('dt')['amt'].sum().to_frame()
    day_amt.columns = ['day_amt']
    minute_data = minute_data.set_index(['dt', 'minute'])
    # 如有分钟数据缺失且不drop_nan，或需要“全长补齐”
    if (value_counts.max() > value_counts.min() and not drop_nan) or full_length_padding:
        complete_minute_list = get_complete_minute_list()  # 每天242根完整的分钟Bar线的分钟值
        index_tuple = [np.repeat(date_list, len(complete_minute_list)), complete_minute_list * len(date_list)]
        # 构建一个逐日、逐分钟的双重索引
        mi_index = pd.MultiIndex.from_tuples(list(zip(*index_tuple)), names=['dt', 'minute'])
        # 对数据重建索引，如不在索引中的，默认就是NaN
        minute_data = minute_data.reindex(index=mi_index)
    if minute_data.__len__() > 0:  # 如有值再筛选，以免报错
        minute_data = minute_data.loc[start_date: end_date].copy()  # 筛选start_date至end_date期间的行情
    # 如有分钟行情，
    if missing_trade_dates.__len__() < requested_date_list.__len__():
        minute_data = minute_data.join(day_amt, on=None, how='left')
        minute_data['close'] = minute_data['close'] * minute_data['day_amt'] / minute_data['day_amt']
        minute_data['open'] = minute_data['open'] * minute_data['day_amt'] / minute_data['day_amt']
        minute_data['high'] = minute_data['high'] * minute_data['day_amt'] / minute_data['day_amt']
        minute_data['low'] = minute_data['low'] * minute_data['day_amt'] / minute_data['day_amt']
        minute_data['volume'] = minute_data['volume'] * minute_data['day_amt'] / minute_data['day_amt']
        minute_data = minute_data.drop('day_amt', axis=1)  # 将列day_amt删掉
    if append_pre_close and missing_trade_dates.__len__() < requested_date_list.__len__():  # 如要pre_close且有分钟行情
        pre_close_df = get_panel_daily_pv_df([stock_code], start_date, end_date, 'pre_close')
        pre_close_df.columns = ['pre_close']
        pre_close_df.index.name = 'dt'
        minute_data = minute_data.join(pre_close_df, on=None, how='left')
    # 如涉及复权且有分钟行情
    if not adj_type == 'NONE' and missing_trade_dates.__len__() < requested_date_list.__len__():
        adj_f = get_panel_daily_info([stock_code], start_date, end_date, 'adjfactor')
        if adj_f.__len__() == 0:
            return pd.DataFrame()
        adj_f.index.name = 'dt'
        # 将复权因子（频率是日级）并入分钟行情数据集
        minute_data = minute_data.join(adj_f, on=None, how='left')
        minute_data = minute_data.rename(columns={stock_code: 'adjfactor'})
        if append_pre_close:
            price_type_list = ['close', 'open', 'high', 'low', 'pre_close']
        else:
            price_type_list = ['close', 'open', 'high', 'low']
        if adj_type == 'FORWARD':  # 从上市日向后复权
            for price_type in price_type_list:
                minute_data[price_type] = minute_data[price_type] * minute_data['adjfactor']
        elif adj_type == 'BACKWARD2':  # 从end_date向前复权
            for price_type in price_type_list:
                minute_data[price_type] = minute_data[price_type] * minute_data['adjfactor'] / \
                                          minute_data.iloc[-1]['adjfactor']
        minute_data = minute_data.drop(['adjfactor'], 1)
    # 因为Python原生的None在pandas/numpy中兼容性不好，影响读写以及在其他模块中的调用，这里转为np.nan
    minute_data = minute_data.add(0.0)
    return minute_data


def get_complete_minute_list():
    complete_minute_list = [925, 930, 931, 932, 933, 934, 935, 936, 937, 938, 939, 940, 941, 942, 943, 944, 945, 946,
                            947, 948, 949, 950, 951, 952, 953, 954, 955, 956, 957, 958, 959, 1000, 1001, 1002, 1003,
                            1004, 1005, 1006, 1007, 1008, 1009, 1010, 1011, 1012, 1013, 1014, 1015, 1016, 1017, 1018,
                            1019, 1020, 1021, 1022, 1023, 1024, 1025, 1026, 1027, 1028, 1029, 1030, 1031, 1032, 1033,
                            1034, 1035, 1036, 1037, 1038, 1039, 1040, 1041, 1042, 1043, 1044, 1045, 1046, 1047, 1048,
                            1049, 1050, 1051, 1052, 1053, 1054, 1055, 1056, 1057, 1058, 1059, 1100, 1101, 1102, 1103,
                            1104, 1105, 1106, 1107, 1108, 1109, 1110, 1111, 1112, 1113, 1114, 1115, 1116, 1117, 1118,
                            1119, 1120, 1121, 1122, 1123, 1124, 1125, 1126, 1127, 1128, 1129, 1300, 1301, 1302, 1303,
                            1304, 1305, 1306, 1307, 1308, 1309, 1310, 1311, 1312, 1313, 1314, 1315, 1316, 1317, 1318,
                            1319, 1320, 1321, 1322, 1323, 1324, 1325, 1326, 1327, 1328, 1329, 1330, 1331, 1332, 1333,
                            1334, 1335, 1336, 1337, 1338, 1339, 1340, 1341, 1342, 1343, 1344, 1345, 1346, 1347, 1348,
                            1349, 1350, 1351, 1352, 1353, 1354, 1355, 1356, 1357, 1358, 1359, 1400, 1401, 1402, 1403,
                            1404, 1405, 1406, 1407, 1408, 1409, 1410, 1411, 1412, 1413, 1414, 1415, 1416, 1417, 1418,
                            1419, 1420, 1421, 1422, 1423, 1424, 1425, 1426, 1427, 1428, 1429, 1430, 1431, 1432, 1433,
                            1434, 1435, 1436, 1437, 1438, 1439, 1440, 1441, 1442, 1443, 1444, 1445, 1446, 1447, 1448,
                            1449, 1450, 1451, 1452, 1453, 1454, 1455, 1456, 1457, 1458, 1459, 1500]
    return complete_minute_list


def get_stock_latest_info(stock_code_list, basic_info_type) -> dict:
    # 从Wind落地数据库中返回股票最新信息，注意只有查询日的最新信息，无历史信息
    basic_info_type_dict = {'Short_name': xq.Factors.Short_name, 'Listing_date': xq.Factors.Listing_date,
                            'Listing_place': xq.Factors.Listing_place, 'Listing_board': xq.Factors.Listing_board,
                            'Delisting_date': xq.Factors.Delisting_date}
    requested_basic_info = basic_info_type_dict[basic_info_type]
    today_int = int(time.strftime("%Y%m%d"))
    # 这里加today_int纯粹是因为不得不输入，但其实查询的是最新信息，输入哪天都一样
    factor_data, _, stock_list = xq.hfactor(stock_code_list, [requested_basic_info], [today_int])
    factor_data = factor_data[0][1]
    if "601313.SH" in stock_code_list and basic_info_type == 'Listing_date':  # 江南嘉捷要特殊对待
        idx = stock_list.index("601313.SH")
        factor_data[idx] = ['20010316']
    if "601313.SH" in stock_code_list and basic_info_type == 'Delisting_date':  # 江南嘉捷要特殊对待
        idx = stock_list.index("601313.SH")
        factor_data[idx] = ['20180228']
    factor_data = [item[0] for item in factor_data]
    output_list = []
    if basic_info_type == 'Listing_date':
        output_list = [int(item) for item in factor_data]
    elif basic_info_type == 'Delisting_date':
        for item in factor_data:
            if item is None or len(item) == 0:
                output_list.append(None)
            else:
                output_list.append(int(item))
    output_dict = {}
    if basic_info_type == 'Delisting_date':
        for symbol, item in zip(stock_list, output_list):
            if item is not None:
                output_dict.update({symbol: item})
    else:
        for symbol, item in zip(stock_list, output_list):
            output_dict.update({symbol: item})
    return output_dict


def __compress_missing_dates(trading_date_list: list, missing_date_list: list, need_print: bool = False):
    """
    Created on 2018/8/27 by 011673 袁诗林
    :param trading_date_list: 交易日列表（排序后）
    :param missing_date_list: 缺失数据的交易日列表（排序后）
    :param need_print: 是否打印结果
    :return: 简化后的缺失交易日信息
    """
    if missing_date_list.__len__() == 0 or trading_date_list.__len__() == 0 \
            or missing_date_list.__len__() > trading_date_list.__len__():
        return []
    index_list = [trading_date_list.index(missing_date) for missing_date in missing_date_list]
    result_index = []
    temp = []
    # 切分缺失数据交易日的index列表，如果是连续的则放在一个列表中，建立双重列表
    for i in range(0, len(index_list)):
        if i == 0:
            temp.append(index_list[i])
            continue
        if index_list[i] - index_list[i - 1] <= 1:
            temp.append(index_list[i])
        else:
            result_index.append(temp)
            temp = [index_list[i]]
    if temp.__len__() != 0:
        result_index.append(temp)
    # 连续的缺失数据交易日的用一头一尾的交易日替代，并改成字符串格式方便输出
    for i in range(0, len(result_index)):
        if result_index[i].__len__() > 2:
            result_index[i] = [result_index[i][0], result_index[i][-1]]
    for i in range(0, len(result_index)):
        if result_index[i].__len__() == 0:
            continue
        elif result_index[i].__len__() == 1:
            result_index[i] = str(trading_date_list[result_index[i][0]])
        else:
            result_index[i] = str(trading_date_list[result_index[i][0]]) + ' ~ ' + \
                              str(trading_date_list[result_index[i][-1]])
    if need_print:
        print(result_index)
    return result_index


def __date_segments(date_list, segment_base):
    list_num, remainder = divmod(date_list.__len__(), segment_base)
    if remainder > 0:
        list_num += 1  # 将query_date_list分为list_num段
    list_count = 0
    date_lists = []
    while list_num > list_count:
        date_lists.append(date_list[list_count * segment_base: list_count * segment_base + segment_base])
        list_count += 1
    return date_lists


def convert_df_index_type(df_input, index_type_input, index_type_output) -> pd.DataFrame:
    """
    转化dataframe的索引的数据类型, timestamp是数字型的timestamp, timestamp2是类似datetime型的
    """
    df = df_input.copy()
    index_name = df.index.name
    if index_type_input == 'date_int' and index_type_output == 'timestamp':
        index_list = list(df.index)
        index_list_datetime = convert_date_or_time_int_to_datetime(index_list)
        index_list_timestamp = [i.timestamp() for i in index_list_datetime]
        df['timestamp'] = index_list_timestamp
        df = df.set_index(['timestamp'])
    elif index_type_input == 'date_int' and index_type_output == 'datetime':
        index_list = list(df.index)
        index_list_datetime = convert_date_or_time_int_to_datetime(index_list)
        df['datetime'] = index_list_datetime
        df = df.set_index(['datetime'])
    elif index_type_input == 'timestamp' and index_type_output == 'date_int':
        index_list = list(df.index)
        index_date_time_list = [dt.datetime.fromtimestamp(i) for i in index_list]
        date_int_list = [int((i.year * 10000 + i.month * 100 + i.day)) for i in index_date_time_list]
        df['date'] = date_int_list
        df = df.set_index(['date'])
    elif index_type_input == 'timestamp2' and index_type_output == 'date_int':
        index_list = list(df.index)
        date_int_list = [int((i.year * 10000 + i.month * 100 + i.day)) for i in index_list]
        df['date'] = date_int_list
        df = df.set_index(['date'])
    elif index_type_input == 'str' and index_type_output == 'date_int':
        df = df.reset_index()
        df['date'] = df[index_name].astype(int)
        df = df.set_index(['date'])
    if index_name in list(df.columns):  # 原有的index列可能残留，删除之
        df = df.drop(index_name, axis=1)
    return df


def return_industry3_chinese_name(key_type):
    if key_type == 'str':
        industry3_dict = {'1': '石油石化', '2': '煤炭', '3': '有色金属', '4': '电力及公用事业', '5': '钢铁',
                          '6': '基础化工', '7': '建筑', '8': '建材', '9': '轻工制造', '10': '机械', '11': '电力设备',
                          '12': '国防军工', '13': '汽车', '14': '商贸零售', '15': '餐饮旅游', '16': '家电',
                          '17': '纺织服装', '18': '医药', '19': '食品饮料', '20': '农林牧渔', '21': '银行',
                          '22': '房地产', '23': '交通运输', '24': '电子元器件', '25': '通信', '26': '计算机',
                          '27': '传媒', '28': '综合', '29': '证券Ⅱ', '30': '保险Ⅱ', '31': '信托及其他'}
    else:
        industry3_dict = {1: '石油石化', 2: '煤炭', 3: '有色金属', 4: '电力及公用事业', 5: '钢铁', 6: '基础化工',
                          7: '建筑', 8: '建材', 9: '轻工制造', 10: '机械', 11: '电力设备', 12: '国防军工', 13: '汽车',
                          14: '商贸零售', 15: '餐饮旅游', 16: '家电', 17: '纺织服装', 18: '医药', 19: '食品饮料',
                          20: '农林牧渔', 21: '银行', 22: '房地产', 23: '交通运输', 24: '电子元器件', 25: '通信',
                          26: '计算机', 27: '传媒', 28: '综合', 29: '证券Ⅱ', 30: '保险Ⅱ', 31: '信托及其他'}
    return industry3_dict


def key_related_search(key):
    # updated on 2019/2/28 更改了支持的字段名和表名
    fdd_q_list = ['eps_basic', 'roa2', 'roa', 'roic', 'grossprofitmargin', 'optoebt', 'profittogr', 'optogr',
                  'operateexpensetogr', 'ebitdatosales', 'operateincometoebt', 'investincometoebt',
                  'nonoperateprofittoebt', 'ocftocf', 'fcftocf', 'ocftosales', 'ocftoop', 'ocftoassets',
                  'ocftodividend', 'debttoassets', 'longdebttolongcaptial', 'longcapitaltoinvestment', 'assetstoequity',
                  'catoassets', 'currentdebttoequity', 'intdebttototalcap', 'currentdebttodebt', 'ncatoequity',
                  'current', 'quick', 'ocftointerest', 'ocftodebt', 'longdebttodebt', 'cashtostdebt', 'invturn',
                  'arturn', 'caturn', 'faturn', 'apturn', 'yoyeps_basic', 'yoyocfps', 'yoy_tr', 'yoyop', 'yoyprofit',
                  'yoynetprofit', 'yoyocf', 'yoyroe', 'yoy_equity', 'yoydebt', 'yoy_assets', 'yoy_cash',
                  'yoy_fixedassets', 'stm_issuingdate', 'tot_assets', 'tot_non_cur_liab', 'tot_liab', 'tot_equity',
                  'roe_basic', 'assetsturn1', 'fcff', 'fcfe', 'qfa_operateincome', 'qfa_roe', 'qfa_roa',
                  'qfa_grossprofitmargin', 'qfa_profittogr', 'qfa_operateincometoebt', 'qfa_ocftosales', 'qfa_ocftoor',
                  'qfa_yoygr', 'qfa_yoysales', 'qfa_yoyop', 'qfa_yoyprofit', 'roe_ttm2', 'roa2_ttm2',
                  'netprofittoassets', 'roic_ttm2', 'netprofitmargin_ttm2', 'grossprofitmargin_ttm2', 'profittogr_ttm2',
                  'optogr_ttm2', 'gctogr_ttm2', 'netprofittoor_ttm', 'operateincometoebt_ttm2', 'ebttoor_ttm',
                  'ocftoor_ttm2', 'gr_ttm2', 'gc_ttm2', 'grossmargin_ttm2', 'interestexpense_ttm', 'profit_ttm2',
                  'operatecashflow_ttm2', 'cashflow_ttm2', 'operatecaptialturn', 'ebittoassets2', 'qfa_yoyeps',
                  'qfa_grossmargin', 'qfa_yoyocf', 'qfa_yoycf', 'qfa_netprofitmargin']
    fdd_d_list = ['pb_lf', 'pcf_ocf_ttm', 'pe_ttm', 'ps_ttm', 'dividendyield2']
    optm_list = ['Listing_date', 'alpha_universe', 'index_300', 'index_50', 'index_500', 'index_weight_hs300',
                 'index_weight_sh50', 'index_weight_zz500', 'risk_universe']
    universe_complete_list = ['HS300', 'OPENDOWNLIMIT', 'OPENUPLIMIT', 'SH50', 'ZZ500']
    industry_list = ['industry3', 'CITIC_I', 'KNN_I']
    barra_list = ['Beta', 'EarningsYield', 'Growth', 'Industry', 'Leverage', 'Liquidity', 'Momentum', 'NonLinearSize',
                  'ResidualVolatility', 'Size', 'Value']
    optm_self_made_list = ['index_800', 'alpha_uni_large', 'alpha_uni_mid', 'alpha_uni_small']

    if running_platform == "Windows":
        if key in fdd_q_list:
            path = r"S:\xquant_data_backup\new\fdd_q\FDD_CHINA_STOCK_QUARTERLY_WIND.h5"
            item = "fdd_q"
        elif key in fdd_d_list:
            path = r"S:\xquant_data_backup\new\fdd_d\FDD_CHINA_STOCK_DAILY_WIND.h5"
            item = "fdd_d"
        elif key in optm_list:
            path = r"S:\xquant_data_backup\new\optm\UNIV_CHINA_STOCK_DAILY_OPTM.h5"
            item = "optm"
        elif key in industry_list:
            path = r"S:\xquant_data_backup\new\industry\INDUSTRY_CHINA_STOCK_DAILY_WIND.h5"
            item = "industry"
        elif key in universe_complete_list:
            path = r"S:\xquant_data_backup\new\universe_complete\universe_complete.h5"
            item = "universe_complete"
        elif key in barra_list:
            path = r"S:\xquant_data_backup\backup\risk\risk_CHINA_STOCK_DAILY_STYLEFACTOR.h5"
            item = "barra"
        elif key in optm_self_made_list:
            path = r"S:\xquant_data_backup\new\optm\UNIV_CHINA_STOCK_DAILY_OPTM.h5"
            item = "optm_self_made"
        else:
            raise TypeError
    else:
        if key in fdd_q_list:
            path = r"/app/data/wdb_h5/WIND/FDD_CHINA_STOCK_QUARTERLY_WIND/FDD_CHINA_STOCK_QUARTERLY_WIND.h5"
            item = "fdd_q"
        elif key in fdd_d_list:
            path = r"/app/data/wdb_h5/WIND/FDD_CHINA_STOCK_DAILY_WIND/FDD_CHINA_STOCK_DAILY_WIND.h5"
            item = "fdd_d"
        elif key in optm_list:
            path = r"/app/data/wdb_h5/WIND/UNIV_CHINA_STOCK_DAILY_OPTM/UNIV_CHINA_STOCK_DAILY_OPTM.h5"
            item = "optm"
        elif key in industry_list:
            path = r"/app/data/wdb_h5/WIND/INDUSTRY_CHINA_STOCK_DAILY_WIND/INDUSTRY_CHINA_STOCK_DAILY_WIND.h5"
            item = "industry"
        elif key in universe_complete_list:
            path = r"/app/data/wdb_h5/WIND/universe_complete/universe_complete.h5"
            item = "universe_complete"
        elif key in barra_list:
            path = ""
            item = "barra"
        elif key in optm_self_made_list:
            path = r"/app/data/wdb_h5/WIND/UNIV_CHINA_STOCK_DAILY_OPTM/UNIV_CHINA_STOCK_DAILY_OPTM.h5"
            item = "optm_self_made"
        else:
            raise TypeError
    return path, item


def return_panel_info_complete_key_set():
    # updated on 2019/2/28 更改了支持的字段名
    key_set = ('pre_close', 'open', 'high', 'low', 'close', 'vwap', 'pct_chg', 'turn', 'volume', 'amt',
               'adjfactor', 'mkt_cap_ard', 'free_float_shares', 'total_shares', 'alpha_universe',
               'risk_universe',

               'eps_basic', 'roa2', 'roa', 'roic', 'grossprofitmargin', 'optoebt', 'profittogr', 'optogr',
               'operateexpensetogr', 'ebitdatosales', 'operateincometoebt', 'investincometoebt',
               'nonoperateprofittoebt', 'ocftocf', 'fcftocf', 'ocftosales', 'ocftoop', 'ocftoassets',
               'ocftodividend', 'debttoassets', 'longdebttolongcaptial', 'longcapitaltoinvestment', 'assetstoequity',
               'catoassets', 'currentdebttoequity', 'intdebttototalcap', 'currentdebttodebt', 'ncatoequity',
               'current', 'quick', 'ocftointerest', 'ocftodebt', 'longdebttodebt', 'cashtostdebt', 'invturn',
               'arturn', 'caturn', 'faturn', 'apturn', 'yoyeps_basic', 'yoyocfps', 'yoy_tr', 'yoyop', 'yoyprofit',
               'yoynetprofit', 'yoyocf', 'yoyroe', 'yoy_equity', 'yoydebt', 'yoy_assets', 'yoy_cash',
               'yoy_fixedassets', 'stm_issuingdate', 'tot_assets', 'tot_non_cur_liab', 'tot_liab', 'tot_equity',
               'roe_basic', 'assetsturn1', 'fcff', 'fcfe', 'qfa_operateincome', 'qfa_roe', 'qfa_roa',
               'qfa_grossprofitmargin', 'qfa_profittogr', 'qfa_operateincometoebt', 'qfa_ocftosales', 'qfa_ocftoor',
               'qfa_yoygr', 'qfa_yoysales', 'qfa_yoyop', 'qfa_yoyprofit', 'roe_ttm2', 'roa2_ttm2',
               'netprofittoassets', 'roic_ttm2', 'netprofitmargin_ttm2', 'grossprofitmargin_ttm2', 'profittogr_ttm2',
               'optogr_ttm2', 'gctogr_ttm2', 'netprofittoor_ttm', 'operateincometoebt_ttm2', 'ebttoor_ttm',
               'ocftoor_ttm2', 'gr_ttm2', 'gc_ttm2', 'grossmargin_ttm2', 'interestexpense_ttm', 'profit_ttm2',
               'operatecashflow_ttm2', 'cashflow_ttm2', 'operatecaptialturn', 'ebittoassets2', 'qfa_yoyeps',
               'qfa_grossmargin', 'qfa_yoyocf', 'qfa_yoycf', 'qfa_netprofitmargin',

               'pb_lf', 'pcf_ocf_ttm', 'pe_ttm', 'ps_ttm', 'dividendyield2',

               'alpha_universe', 'index_300', 'index_50', 'index_500', 'index_weight_hs300',
               'index_weight_sh50', 'index_weight_zz500', 'risk_universe',

               'Listing_date', 'HS300', 'OPENDOWNLIMIT', 'OPENUPLIMIT', 'SH50', 'ZZ500',

               'industry3', 'CITIC_I', 'KNN_I',

               'Beta', 'EarningsYield', 'Growth', 'Industry', 'Leverage', 'Liquidity', 'Momentum', 'NonLinearSize',
               'ResidualVolatility', 'Size', 'Value',

               'index_800', 'alpha_uni_large', 'alpha_uni_mid', 'alpha_uni_small')
    return key_set


def unfold_df(df_input):
    """将双重索引的dataframe展开(unstack)，并将列索引的第1层去掉，只留下股票代码"""
    df = df_input.copy()
    df = df.unstack()
    col_list = list(df.columns)
    col_list2 = [item[1] for item in col_list]
    df.columns = col_list2
    return df


def read_h5_gogoal_data(table_name, key_name, start_date_int, end_date_int, stock_list, max_contype=2) -> pd.DataFrame:
    """
    从h5文件中读取朝阳永续的数据（数据表或字段可能不全）
    :param table_name: 朝阳永续的表名
    :param key_name: 在朝阳永续中的字段名，注意要全大写
    :param start_date_int: 开始日期，应当是8位整数，例如20180604
    :param end_date_int: 结束日期，应当是8位整数，例如20180630
    :param stock_list: 股票列表，例如["600000.SH", "000002.SZ"]
    :param max_contype: con_type的最大值（可以等于这个值），对于没有Contype的字段则不起作用
    """
    if platform.system() == "Windows":
        gogoal_data_path = r"S:\xquant_data_backup\backup\fcd\DAILY\HTSC"
        if table_name == 'FCD_CHINA_STOCK_DAILY_SUNTIME':
            table_path = os.path.join(gogoal_data_path, str("FCD_CHINA_STOCK_DAILY_SUNTIME.h5"))
        else:
            table_path = os.path.join(gogoal_data_path, str(table_name + ".h5"))
    else:
        gogoal_data_path = "/app/data/wdb_h5/WIND/"
        if table_name == 'FCD_CHINA_STOCK_DAILY_SUNTIME':
            table_path = os.path.join(gogoal_data_path, str("FCD_CHINA_STOCK_DAILY_SUNTIME"),
                                      str("FCD_CHINA_STOCK_DAILY_SUNTIME.h5"))
        else:
            table_path = os.path.join(gogoal_data_path, table_name, str(table_name + ".h5"))
    if table_name in ["con_forecast_schedule", "con_stock_deviation3", "stock_concern_level", "stock_order2",
                      "stock_order3", "stock_report_adjustment2"]:
        store = pd.HDFStore(table_path, mode="r")
        data_df = store.select(str("/" + table_name), columns=[key_name])
        store.close()
    elif table_name in ["FCD_CHINA_STOCK_DAILY_SUNTIME"]:
        data_df = pd.DataFrame()
        # 不知合适的start_line是多少，试着取一个chunk的df，然后估算距离、逐渐逼近
        start_line = 0
        chunk_size = 100000
        start_date_timestamp = pd.Timestamp(str(start_date_int))
        end_date_timestamp = pd.Timestamp(str(end_date_int))
        store = pd.HDFStore(table_path, mode="r")

        def get_a_chunk_of_df(store0, table_name0, chunk_size0, start_line0, key_name0):
            unique_date_list0 = []
            for data_frame in store0.select(table_name0, chunksize=chunk_size0, start=start_line0, columns=[key_name0]):
                data_frame1 = data_frame.copy()
                unique_date_list0 = list(set(data_frame1.index.get_level_values(level=0)))
                unique_date_list0.sort()
                break
            return unique_date_list0

        unique_date_list = get_a_chunk_of_df(store, table_name, chunk_size, start_line, key_name)
        lines_per_day = chunk_size / unique_date_list.__len__()
        diff_days = (start_date_timestamp - unique_date_list[-1]).days
        while diff_days > 20:  # 逼近到前20天即可
            distance_in_lines = diff_days * lines_per_day
            start_line += int(distance_in_lines)
            unique_date_list = get_a_chunk_of_df(store, table_name, chunk_size, start_line, key_name)
            diff_days = (start_date_timestamp - unique_date_list[-1]).days
        if diff_days < 0:  # 若超过了，再返回一点
            while unique_date_list[0] > start_date_timestamp:
                start_line -= chunk_size
                unique_date_list = get_a_chunk_of_df(store, table_name, chunk_size, start_line, key_name)
        # 至此得到合适的start_line
        for df in store.select(table_name, chunksize=chunk_size, start=start_line,
                               columns=['CON_TYPE', 'RPT_DATE', key_name]):
            if df.index.get_level_values(level=0)[-1] < start_date_timestamp:
                continue
            if df.index.get_level_values(level=0)[0] > end_date_timestamp:
                break
            df1 = df.copy()
            df1['date_timestamp'] = df1.index.get_level_values(level=0)
            unique_date = list(set(list(df1['date_timestamp'])))
            report_year_dict = {}
            # 若在3月以后，则采用下一年的预期值，否则用当年预期值；另外，CON_TYPE选用1（加权计算）或2（手工计算）的
            for i_date in unique_date:
                if i_date <= pd.Timestamp(str(i_date.year) + "0228"):
                    report_year_dict.update({i_date: i_date.year})
                else:
                    report_year_dict.update({i_date: i_date.year + 1})
            date_time_stamp_list = df1['date_timestamp']
            report_year_list = [report_year_dict[i] for i in date_time_stamp_list]
            df1['report_year'] = report_year_list
            df1 = df1.loc[df1['RPT_DATE'] == df1['report_year']]
            df1 = df1.loc[df1['CON_TYPE'] <= max_contype]
            df1 = df1[key_name]
            df1 = df1.to_frame()
            data_df = data_df.append(df1)
        store.close()
    data_df = unfold_df(data_df)
    data_df = convert_df_index_type(data_df, 'timestamp2', 'date_int')
    data_df = data_df.loc[start_date_int: end_date_int]
    data_df = data_df.reindex(columns=stock_list)
    data_df.index.name = 'index'  # 使输出的index的名字是'index'，其实没有实际意义，只不过是为了和一些原始factor一致
    return data_df


def get_single_stock_minute_data2(stock_code: str, start_date: int, end_date: int, fill_nan: bool = True,
                                  append_pre_close: bool = False, adj_type: str = 'NONE', drop_nan: bool = False,
                                  full_length_padding: bool = True, print_when_nodata: bool = True):
    # 函数与get_single_stock_minute_data几乎一模一样，唯一不一样的地方是——读取数据时先通过一个缓存，
    # 如数据在缓存中有，则返回缓存中的数据；另外，返回缓存中的数据时，print_when_nodata将设为False
    # 注意仅在单进程中有效，多进程无效
    if running_platform == "Windows":
        ans_df = get_single_stock_minute_data(stock_code, start_date, end_date, fill_nan, append_pre_close,
                                              adj_type, drop_nan, full_length_padding)
        return ans_df
    if fill_nan and drop_nan:
        raise Exception('Logic error: fill_nan and drop_nan were True at the same time')
    # 目前支持7个指数：上证50、沪深300、中证500和中证800、上证综指、深证成指和创业板指
    if stock_code in ["000001.SH", "000016.SH", "000300.SH", "000905.SH", "000906.SH", "399001.SZ", "399006.SZ"]:
        dir_path = os.path.join(minute_data_root_path, "index")
        file_name = "indexMinute_" + stock_code[0:6] + ".pkl"
        if adj_type == "NONE":
            pass
        else:
            raise Exception('Adjustment is not applicable to index data')
    else:
        dir_path = os.path.join(minute_data_root_path, "stock")
        file_name = "UnAdjstedStockMinute_" + stock_code[0:6] + ".pkl"
    ori_stock_min_data_cache = OriginalStockMinuteDataCache()
    if ori_stock_min_data_cache.if_stock_cached(stock_code):
        # 如股票已经在缓存中了，则直接获取
        minute_data = ori_stock_min_data_cache.get_original_stock_min_data(stock_code, start_date, end_date)
        print_when_nodata = False
    elif os.path.exists(os.path.join(dir_path, file_name)):
        # 如股票不在缓存中，但对应股票的文件存在，则也访问之
        minute_data = ori_stock_min_data_cache.get_original_stock_min_data(stock_code, start_date, end_date)
    elif not full_length_padding:
        if print_when_nodata:
            print("minute data file of", stock_code, "does not exist")
        minute_data = pd.DataFrame()
        return minute_data
    else:
        if print_when_nodata:
            print("minute data file of", stock_code,
                  "does not exist; but a DataFrame fulfilled with nan will be returned")
        date_list = get_trading_day(start_date, end_date)
        complete_minute_list = get_complete_minute_list()  # 每天242根完整的分钟Bar线的分钟值
        i_stock_minute_data_full_length = date_list.__len__() * 242
        index_tuple = [np.repeat(date_list, len(complete_minute_list)), complete_minute_list * len(date_list)]
        mi_index = pd.MultiIndex.from_tuples(list(zip(*index_tuple)), names=['dt', 'minute'])
        minute_data = pd.DataFrame(index=mi_index)  # 新建一个空的DataFrame, 且先设好了索引
        temp_array = np.empty(shape=[i_stock_minute_data_full_length, ])
        temp_array[:] = np.nan
        if not append_pre_close:
            for col in ['open', 'high', 'low', 'close', 'volume', 'amt']:
                minute_data[col] = temp_array
        else:
            for col in ['open', 'high', 'low', 'close', 'volume', 'amt', 'pre_close']:
                minute_data[col] = temp_array
        return minute_data
    date_list_of_stock_file = list(set(minute_data.index.get_level_values(level=0)))  # 从数据文件中可取到的日期列表
    date_list_of_stock_file.sort()
    requested_date_list = get_trading_day(start_date, end_date)  # 希望获取的交易日列表
    # 找出希望获取的交易日列表中、数据文件中没有的日期，记为missing_trade_dates
    missing_trade_dates = list(set(requested_date_list).difference(set(date_list_of_stock_file)))
    requested_date_list.sort()
    missing_trade_dates.sort()
    compressed_missing_trade_dates = __compress_missing_dates(requested_date_list, missing_trade_dates)
    if missing_trade_dates.__len__() > 0:
        # 这些交易日没有分钟数据
        if print_when_nodata:
            print(stock_code, "no minute data for these days:", compressed_missing_trade_dates)
    if fill_nan and not drop_nan:  # 如需要填充NaN值
        start_date_minus_1 = get_n_days_off(int(start_date), -2)[0]  # 取start_date前1个交易日
        # 取出start_date_minus_1——往前取1天是为了：如第1天开盘即有缺失的，上溯至前一交易日行情填充
        minute_data = minute_data.loc[start_date_minus_1: end_date].copy()
    else:  # 如不填充NaN值
        minute_data = minute_data.loc[start_date: end_date].copy()
    if fill_nan and not drop_nan:
        # 填充缺失值，价格用前值填充，成交量和成交额用0填充
        minute_data['close'] = minute_data['close'].fillna(method='ffill')
        minute_data['open'] = minute_data['open'].fillna(method='ffill')
        minute_data['high'] = minute_data['high'].fillna(method='ffill')
        minute_data['low'] = minute_data['low'].fillna(method='ffill')
        minute_data['amt'] = minute_data['amt'].fillna(0)
        minute_data['volume'] = minute_data['volume'].fillna(0)
    elif drop_nan:  # 如遇到nan的，就把整行记录删掉
        minute_data = minute_data.dropna(how='any')
    value_counts = pd.value_counts(minute_data['minute'])
    minute_data = minute_data.reset_index().drop('Ticker', axis=1)  # 因读到的原始数据是multi-index，这里重设一下
    # 如要“全长补齐”，则输出的日期等于start_date和end_date期间的日期，若未上市期间或已退市期间的，也会以nan补齐
    if full_length_padding:
        date_list = requested_date_list
    else:  # 如无需“全长补齐”，则输出的日期等于分钟数据文件中有的日期
        date_list = list(set(minute_data['dt']))
    date_list.sort()
    day_amt = minute_data.groupby('dt')['amt'].sum().to_frame()
    day_amt.columns = ['day_amt']
    minute_data = minute_data.set_index(['dt', 'minute'])
    # 如有分钟数据缺失且不drop_nan，或需要“全长补齐”
    if (value_counts.max() > value_counts.min() and not drop_nan) or full_length_padding:
        complete_minute_list = get_complete_minute_list()  # 每天242根完整的分钟Bar线的分钟值
        index_tuple = [np.repeat(date_list, len(complete_minute_list)), complete_minute_list * len(date_list)]
        # 构建一个逐日、逐分钟的双重索引
        mi_index = pd.MultiIndex.from_tuples(list(zip(*index_tuple)), names=['dt', 'minute'])
        # 对数据重建索引，如不在索引中的，默认就是NaN
        minute_data = minute_data.reindex(index=mi_index)
    if minute_data.__len__() > 0:  # 如有值再筛选，以免报错
        minute_data = minute_data.loc[start_date: end_date].copy()  # 筛选start_date至end_date期间的行情
    # 如有分钟行情，
    if missing_trade_dates.__len__() < requested_date_list.__len__():
        minute_data = minute_data.join(day_amt, on=None, how='left')
        minute_data['close'] = minute_data['close'] * minute_data['day_amt'] / minute_data['day_amt']
        minute_data['open'] = minute_data['open'] * minute_data['day_amt'] / minute_data['day_amt']
        minute_data['high'] = minute_data['high'] * minute_data['day_amt'] / minute_data['day_amt']
        minute_data['low'] = minute_data['low'] * minute_data['day_amt'] / minute_data['day_amt']
        minute_data['volume'] = minute_data['volume'] * minute_data['day_amt'] / minute_data['day_amt']
        minute_data = minute_data.drop('day_amt', axis=1)  # 将列day_amt删掉
    if append_pre_close and missing_trade_dates.__len__() < requested_date_list.__len__():  # 如要pre_close且有分钟行情
        pre_close_df = get_panel_daily_pv_df([stock_code], start_date, end_date, 'pre_close')
        pre_close_df.columns = ['pre_close']
        pre_close_df.index.name = 'dt'
        minute_data = minute_data.join(pre_close_df, on=None, how='left')
    # 如涉及复权且有分钟行情
    if not adj_type == 'NONE' and missing_trade_dates.__len__() < requested_date_list.__len__():
        adj_f = get_panel_daily_info([stock_code], start_date, end_date, 'adjfactor')
        if adj_f.__len__() == 0:
            return pd.DataFrame()
        adj_f.index.name = 'dt'
        # 将复权因子（频率是日级）并入分钟行情数据集
        minute_data = minute_data.join(adj_f, on=None, how='left')
        minute_data = minute_data.rename(columns={stock_code: 'adjfactor'})
        if append_pre_close:
            price_type_list = ['close', 'open', 'high', 'low', 'pre_close']
        else:
            price_type_list = ['close', 'open', 'high', 'low']
        if adj_type == 'FORWARD':  # 从上市日向后复权
            for price_type in price_type_list:
                minute_data[price_type] = minute_data[price_type] * minute_data['adjfactor']
        elif adj_type == 'BACKWARD2':  # 从end_date向前复权
            for price_type in price_type_list:
                minute_data[price_type] = minute_data[price_type] * minute_data['adjfactor'] / \
                                          minute_data.iloc[-1]['adjfactor']
        minute_data = minute_data.drop(['adjfactor'], 1)
    # 因为Python原生的None在pandas/numpy中兼容性不好，影响读写以及在其他模块中的调用，这里转为np.nan
    minute_data = minute_data.add(0.0)
    return minute_data


def get_stock_listing_date(end_date=None, drop_delisted_stocks=False):
    # 获取股票上市股票及对应的上市日期
    complete_stock_list = []
    if running_platform == "Windows":
        complete_stock_list_path = "S:\\xquant_data_backup\AlphaDataBase\CompleteStockList.csv"
    elif running_platform == "Linux-GPU":
        complete_stock_list_path = "/vipzrz/Apollo/AlphaDataBase/CompleteStockList.csv"
    else:
        complete_stock_list_path = "/app/data/666889/Apollo/AlphaDataBase/CompleteStockList.csv"
    if os.path.exists(complete_stock_list_path):
        df = pd.read_csv(complete_stock_list_path, index_col=0)
        df = df.fillna(0)
        if drop_delisted_stocks:
            df = df[df.Delisting_date < 1]
        if end_date is None:
            return df
        else:
            df = df[(df.Listing_date <= end_date) & (df.Delisting_date <= end_date)]
            return df


def back_fill(df_fill, df_qfa, df_ann, fill_na=True):
    # 将财务数据按交易日向后填充到complete_stock_list的空表内
    """
    :param df_fill: 全样本股票列表及交易日的空表
    :param df_qfa: 财务数据表
    :param df_ann: 实际披露日期
    :param fill_na: 是否将nan值填充为0。目前万得数据库中一个报表数据不存在或值为0都会被填充为nan
    :return: 将df_fill填充完成后的Dataframe
    """
    import math
    df_fill_new = df_fill.copy()
    trading_days = list(df_fill_new.index)
    start_date = trading_days[0]
    end_date = trading_days[-1]
    trading_days_np = np.array(trading_days)
    columns = df_fill_new.columns
    index = df_fill_new.index
    df_fill_np = df_fill_new.values  # 计算时涉及到循环，采用numpy计算提高速度
    stock_listing_date = get_stock_listing_date()
    for col_i, col in enumerate(columns):
        # 如果个股在报表内
        if col in df_ann and col in df_qfa:
            listing_date = stock_listing_date.at[col, 'Listing_date']
            s_ann = df_ann[col].values
            s_ann = np.array([int(x) for x in s_ann])
            s_qfa = df_qfa[col].values
            # 将报告期list和数据list置于规定的开始日期和结束日期之内
            s_ann_temp = s_ann[(s_ann <= end_date) & ((s_ann >= start_date) & (s_ann > listing_date))]
            s_qfa_temp = s_qfa[(s_ann <= end_date) & ((s_ann >= start_date) & (s_ann > listing_date))]
            for idate in range(len(s_ann_temp)):
                # 如果非nan且非inf，则填充到披露期那一天，如果披露期非交易日，则顺延到最近的一个交易日
                if not math.isnan(s_ann_temp[idate]) and not math.isnan(s_qfa_temp[idate]) and not math.isinf(
                        s_qfa_temp[idate]):
                    ann_date = int(s_ann_temp[idate])
                    # 因为要填充到下一个发布日，所以首先判断是不是最后一个发布日
                    if idate < len(s_ann_temp) - 1:
                        # 下一个发布日的int
                        ann_date_next = int(s_ann_temp[idate + 1])
                        # 如果下一个发布日之后还有交易日的话（避免出现下一个发布日与最后一个交易日重叠的情况）
                        if np.where(trading_days_np > ann_date_next)[0].tolist() != []:
                            # 当前发布日在交易日list中的位置
                            ann_date_id = np.where(trading_days_np > ann_date)[0][0]
                            # 下一个发布日在交易日list中的位置
                            ann_date_next_id = np.where(trading_days_np > ann_date_next)[0][0]
                            # 将此两个位置之间的交易日填充为新值
                            df_fill_np[ann_date_id:ann_date_next_id, col_i] = s_qfa_temp[idate]
                        # 如果下一个交易日之后没有交易日了，也要进行填充，不然在下一步也不会填充了。
                        else:
                            # 有时年报和下一年一季报在同一天发布
                            if ann_date != ann_date_next:
                                # 当前发布日在交易日list中的位置
                                ann_date_id = np.where(trading_days_np > ann_date)[0][0]
                                # 将当前交易日之后的交易日填充
                                df_fill_np[ann_date_id:, col_i] = s_qfa_temp[idate]
                    # 如果到了最后一个发布日
                    else:
                        # 如果当前发布日之后还有交易日的话（避免出现当前发布日与最后一个交易日重叠的情况）
                        if np.where(trading_days_np > ann_date)[0].tolist() != []:
                            # 当前发布日在交易日list中的位置
                            ann_date_id = np.where(trading_days_np > ann_date)[0][0]
                            # 将当前交易日之后的交易日填充
                            df_fill_np[ann_date_id:, col_i] = s_qfa_temp[idate]
                        else:
                            pass
    df_fill_new = pd.DataFrame(df_fill_np, index=index, columns=columns)
    df_fill_new.sort_index(inplace=True)
    if fill_na:
        df_fill_new = df_fill_new.fillna(0)
    return df_fill_new


def start_date_backfill(start_date_int, back_years=0):
    # by 011672 - 向前回溯2个季度
    # revised by 006566 on 2019/4/4 - 添加back_years
    start_month = int(str(start_date_int)[4:6])
    start_year = int(str(start_date_int)[0:4])
    if 1 <= start_month <= 3:
        last_report_date = int(str(start_year - back_years - 1) + '0630')
    elif 4 <= start_month <= 6:
        last_report_date = int(str(start_year - back_years - 1) + '0930')
    elif 7 <= start_month <= 9:
        last_report_date = int(str(start_year - back_years - 1) + '1231')
    elif 10 <= start_month <= 12:
        last_report_date = int(str(start_year - back_years) + '0331')
    else:
        raise Exception('Start date error')
    last_report_date = get_n_days_off(last_report_date, -1)[0]
    return last_report_date


class OriginalStockMinuteDataCache(metaclass=Singleton):
    def __init__(self):
        # self.__ori_stock_min_data是个字典，key是股票代码，value是一个list ---- 这个list的值中[0]是start_date, [1]是
        # end_date, [2]是start_date到end_date期间的分钟行情的dataframe; 但注意dataframe的index的首尾很可能不是
        # start_date和end_date, 因为可能这只股票在start_date时还没上市、也就没有分钟行情
        self.__ori_stock_min_data = {}

    def get_original_stock_min_data(self, stock_code, start_date, end_date):
        # 将start_date和end_date都转化为交易日
        trading_day_list = get_trading_day(start_date, end_date)
        start_date = trading_day_list[0]
        end_date = trading_day_list[-1]
        if stock_code in ["000001.SH", "000016.SH", "000300.SH", "000905.SH", "000906.SH", "399001.SZ",
                          "399006.SZ"]:
            dir_path = os.path.join(minute_data_root_path, "index")
            file_name = "indexMinute_" + stock_code[0:6] + ".pkl"
        else:
            dir_path = os.path.join(minute_data_root_path, "stock")
            file_name = "UnAdjstedStockMinute_" + stock_code[0:6] + ".pkl"

        if stock_code in self.__ori_stock_min_data.keys():
            # 如果start_date和end_date在缓存时间区间的范围之内，则直接获取
            if start_date >= self.__ori_stock_min_data[stock_code][0] and \
                            end_date <= self.__ori_stock_min_data[stock_code][1]:
                ans_df = self.__ori_stock_min_data[stock_code][2].loc[start_date: end_date]
            # 否则就要去硬盘中读取
            else:
                temp_df = pd.read_pickle(os.path.join(dir_path, file_name), compression='gzip')
                self.__ori_stock_min_data[stock_code][0] = start_date
                self.__ori_stock_min_data[stock_code][1] = end_date
                self.__ori_stock_min_data[stock_code][2] = temp_df.loc[start_date: end_date]
                ans_df = self.__ori_stock_min_data[stock_code][2]
        else:
            self.__ori_stock_min_data.update({stock_code: []})
            temp_df = pd.read_pickle(os.path.join(dir_path, file_name), compression='gzip')
            self.__ori_stock_min_data[stock_code].append(start_date)
            self.__ori_stock_min_data[stock_code].append(end_date)
            self.__ori_stock_min_data[stock_code].append(temp_df.loc[start_date: end_date])
            ans_df = self.__ori_stock_min_data[stock_code][2]
        return ans_df

    def return_cached_stock_list(self):
        return list(self.__ori_stock_min_data.keys())

    def if_stock_cached(self, stock_code):
        if stock_code in self.__ori_stock_min_data.keys():
            return True
        else:
            return False


def return_statement_type_filtered_df(alt, column, start_date_int, end_date_int):
    # 若STATEMENT_TYPE有不止一种，则保留合并报表，若报表修改过，则保留修改前的报表数据（类型408005000）
    # 返回的列包括：STATEMENT_TYPE（如有），column和'ANN_DT'
    input_columns = deepcopy(column)
    if isinstance(input_columns, list):
        factor = input_columns
        factor.extend(['ANN_DT'])
    else:
        factor = [input_columns, 'ANN_DT']
    if alt in ['AShareBalanceSheet', 'AShareIncome', 'AShareCashFlow']:
        factor.extend(['STATEMENT_TYPE'])
        df = read_data([start_date_int, end_date_int], columns=factor, alt=alt)
        # 仅保留合并报表的数据
        df = df[(df['STATEMENT_TYPE'] == '408001000') | (df['STATEMENT_TYPE'] == '408005000')]
        # 如报表有修改的，那么保留修改前的数据 （408005000）
        df = df[(~df.index.duplicated(False)) | (df['STATEMENT_TYPE'] == '408005000')]
    # 若没有STATEMENT_TYPE这一重复项，则无需过滤
    else:
        df = read_data([start_date_int, end_date_int], columns=factor, alt=alt)
    return df


def df_unstack_and_filter(df_input, filter_stock_list, report_date_list):
    # 将输入的DataFrame的多重索引展开，将时间轴转化为date_int的格式，并在股票列和行上分别做过滤(reindex)
    df_output = df_input.copy()
    df_output = df_output.unstack()
    df_output = convert_df_index_type(df_output, 'timestamp2', 'date_int')
    df_output = df_output.reindex(columns=filter_stock_list)
    df_output = df_output.reindex(index=report_date_list)
    return df_output


def get_daily_wind_quarterly_data(stock_list, alt, column, start_date_int, end_date_int, data_type='original'):
    """
    author: 011672, 006566
    从XQuant的Wind落地数据库，获取季频数据，并根据发布日转化为日频数据
    :param stock_list: 需要过滤的股票列表
    :param alt: 提取数据的表名
    :param column: 提取数据的字段名，可以是一个字段，也可以是list
    :param start_date_int: 起始日期
    :param end_date_int: 终止日期
    :param data_type: 'original': 原始数据; 'ttm': 滚动12个月; 'qfa': 单季值
    :return: 输出DataFrame格式的数据，行为int格式的交易日，列为输入的stock_list；如输入的column是一个字符串，则输出一个
              DataFrame, 如输入一个list, 则输出一个list, 其中内容对应
    examples:
    get_daily_wind_quarterly_data(stock_list, 'AShareBalanceSheet', 'MONETARY_CAP', 20150101, 20180630, 'original')
    get_daily_wind_quarterly_data(stock_list, 'AShareIncome', 'NET_PROFIT_EXCL_MIN_INT_INC', 20150101, 20180630, 'ttm')
    get_daily_wind_quarterly_data(stock_list, 'AShareIncome', 'NET_PROFIT_EXCL_MIN_INT_INC', 20150101, 20180630, 'qfa')
    get_daily_wind_quarterly_data(stock_list, 'AShareBalanceSheet', 'MONETARY_CAP', 20150101, 20180630, 'yoy')
    get_daily_wind_quarterly_data(stock_list, 'AShareIncome', 'NET_PROFIT_EXCL_MIN_INT_INC', 20150101, 20180630,
                                  'ttm_yoy')
    get_daily_wind_quarterly_data(stock_list, 'AShareIncome', 'NET_PROFIT_EXCL_MIN_INT_INC', 20150101, 20180630,
                                  'qfa_yoy')

    """
    if data_type in ['original', 'ttm', 'qfa', 'yoy', 'ttm_yoy', 'qfa_yoy']:
        if alt == 'AShareBalanceSheet' and data_type in ['ttm', 'qfa', 'ttm_yoy', 'qfa_yoy']:
            # 'ttm'或'qfa'不适用于资产负债表
            raise Exception('ttm or qfa is not suitable for AShareBalanceSheet')
        else:
            pass
    else:
        raise TypeError

    if isinstance(column, list):
        column = [_.upper() for _ in column]
    else:
        column = column.upper()

    if data_type == 'original':  # original向前回溯2个季度
        last_report_date0 = start_date_backfill(start_date_int, back_years=0)
    elif data_type in ['ttm', 'qfa', 'yoy']:  # ttm, qfa或yoy向前回溯1年+2个季度
        last_report_date0 = start_date_backfill(start_date_int, back_years=1)
    elif data_type in ['ttm_yoy', 'qfa_yoy']:  # ttm_yoy或qfa_yoy向前回溯2年+2个季度
        last_report_date0 = start_date_backfill(start_date_int, back_years=2)
    else:
        raise TypeError

    # 获取经STATEMENT_TYPE过滤后的数据DataFrame
    df = return_statement_type_filtered_df(alt, column, last_report_date0, end_date_int)
    # 获取报告期数据，用于后续仅保留报告期数据
    report_dates_list = DataAPI.GetTradingDay.get_quarterly_report_dates_list(last_report_date0, end_date_int)

    ann_df = df['ANN_DT']
    ann_df = df_unstack_and_filter(ann_df, stock_list, report_dates_list)
    if isinstance(column, list):
        data_df_or_dflist = []
        for i_column in column:
            temp_data_df = df[i_column]
            temp_data_df = df_unstack_and_filter(temp_data_df, stock_list, report_dates_list)
            data_df_or_dflist.append(temp_data_df)
    else:
        data_df_or_dflist = df[column]
        data_df_or_dflist = df_unstack_and_filter(data_df_or_dflist, stock_list, report_dates_list)

    if data_type == 'original':  # 如original，则无需做任何处理
        data_result = data_df_or_dflist
    elif data_type == 'yoy':
        report_dates_list = report_dates_list[4:]
        if isinstance(column, list):  # 如输入的column是list, 则代表一次需要查询不止一个字段
            data_result = []
            for i_data_df in data_df_or_dflist:
                i_data_result = i_data_df / i_data_df.shift(4) - 1
                i_data_result = i_data_result.reindex(index=report_dates_list)
            data_result.append(i_data_result)
        else:
            data_result = data_df_or_dflist / data_df_or_dflist.shift(4) - 1
    elif data_type in ['ttm', 'ttm_yoy']:
        report_dates_list = report_dates_list[4:]  # 排除第1年的数据
        if isinstance(column, list):  # 如输入的column是list, 则代表一次需要查询不止一个字段
            data_result = []
            for i_data_df in data_df_or_dflist:
                i_data_result = pd.DataFrame(index=report_dates_list, columns=stock_list)
                # 计算ttm数据
                for i_report_date in report_dates_list:
                    if str(i_report_date)[4:8] == '1231':
                        i_data_result.loc[i_report_date] = i_data_df.loc[i_report_date]
                    else:
                        i_data_result.loc[i_report_date] = i_data_df.loc[i_report_date] + i_data_df.loc[
                            int(str(i_report_date - 10000)[0:4] + '1231')] - i_data_df.loc[i_report_date - 10000]
                data_result.append(i_data_result)
            if data_type == 'ttm_yoy':
                data_result2 = []
                for i_data_result in data_result:
                    i_data_result = i_data_result / i_data_result.shift(4) - 1
                    i_data_result = i_data_result.iloc[4:]
                    data_result2.append(i_data_result)
                data_result = data_result2
        else:  # 如输入的不是list, 则代表一次只查询一个字段
            data_result = pd.DataFrame(index=report_dates_list, columns=stock_list)
            # 计算ttm数据
            for i_report_date in report_dates_list:
                if str(i_report_date)[4:8] == '1231':
                    data_result.loc[i_report_date] = data_df_or_dflist.loc[i_report_date]
                else:
                    data_result.loc[i_report_date] = data_df_or_dflist.loc[i_report_date] + data_df_or_dflist.loc[
                        int(str(i_report_date - 10000)[0:4] + '1231')] - data_df_or_dflist.loc[i_report_date - 10000]
            if data_type == 'ttm_yoy':
                data_result = data_result / data_result.shift(4) - 1
                data_result = data_result.iloc[4:]
    elif data_type in ['qfa', 'qfa_yoy']:
        report_dates_list = report_dates_list[4:]  # 排除第1年的数据
        if isinstance(column, list):  # 如输入的column是list, 则代表一次需要查询不止一个字段
            data_result = []
            for i_data_df in data_df_or_dflist:
                i_data_result = pd.DataFrame(index=report_dates_list, columns=stock_list)
                # 计算qfa数据
                for i_report_date in report_dates_list:
                    if str(i_report_date)[4:8] == '0331':
                        i_data_result.loc[i_report_date] = i_data_df.loc[i_report_date]
                    else:
                        if str(i_report_date)[4:8] == '0630':
                            last_quarter = '0331'
                        elif str(i_report_date)[4:8] == '0930':
                            last_quarter = '0630'
                        elif str(i_report_date)[4:8] == '1231':
                            last_quarter = '0930'
                        else:
                            raise TypeError
                        i_data_result.loc[i_report_date] = i_data_df.loc[i_report_date] - i_data_df.loc[
                            int(str(i_report_date)[0:4] + last_quarter)]
                data_result.append(i_data_result)
            if data_type == 'qfa_yoy':
                data_result2 = []
                for i_data_result in data_result:
                    i_data_result = i_data_result / i_data_result.shift(4) - 1
                    i_data_result = i_data_result.iloc[4:]
                    data_result2.append(i_data_result)
                data_result = data_result2
        else:  # 如输入的不是list, 则代表一次只查询一个字段
            data_result = pd.DataFrame(index=report_dates_list, columns=stock_list)
            # 计算qfa数据
            for i_report_date in report_dates_list:
                if str(i_report_date)[4:8] == '0331':
                    data_result.loc[i_report_date] = data_df_or_dflist.loc[i_report_date]
                else:
                    if str(i_report_date)[4:8] == '0630':
                        last_quarter = '0331'
                    elif str(i_report_date)[4:8] == '0930':
                        last_quarter = '0630'
                    elif str(i_report_date)[4:8] == '1231':
                        last_quarter = '0930'
                    else:
                        raise TypeError
                    data_result.loc[i_report_date] = data_df_or_dflist.loc[i_report_date] - data_df_or_dflist.loc[
                        int(str(i_report_date)[0:4] + last_quarter)]
            if data_type == 'qfa_yoy':
                data_result = data_result / data_result.shift(4) - 1
                data_result = data_result.iloc[4:]
    else:
        raise TypeError

    # 获取完整的交易日列表，随后建立空表
    last_report_date1 = start_date_backfill(start_date_int, back_years=0)
    trading_day_list = get_trading_day(last_report_date1, end_date_int)
    if isinstance(column, list):
        answer = []
        for i_data_result in data_result:
            data_df_empty = pd.DataFrame(index=trading_day_list, columns=stock_list)
            # 将季频数据根据发布日填充成日频数据，填充前需要把发布日的na按0填充
            ann_df2 = ann_df.reindex(i_data_result.index)
            ann_df2 = ann_df2.fillna(0)
            i_answer = back_fill(data_df_empty, i_data_result, ann_df2)
            i_answer = i_answer.loc[start_date_int:]
            i_answer = i_answer.reindex(columns=stock_list)
            answer.append(i_answer)
    else:
        data_df_empty = pd.DataFrame(index=trading_day_list, columns=stock_list)
        # 将季频数据根据发布日填充成日频数据，填充前需要把发布日的na按0填充
        ann_df2 = ann_df.reindex(data_result.index)
        ann_df2 = ann_df2.fillna(0)
        answer = back_fill(data_df_empty, data_result, ann_df2)
        answer = answer.loc[start_date_int:]
        answer = answer.reindex(columns=stock_list)
    return answer
