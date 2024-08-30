import DataAPI.DataToolkit as Dtk
from importlib import import_module
import os
import pandas as pd
import platform
import datetime as dt
import numpy as np
import time


# -----------------------------------------------------------------------------
# 需要更新的因子名写在下列列表中，内容依次是模块名、参数列表、因子文件名
# 若是因子，以Factor开头；若是非因子（即因子原始值），以NonFactor开头
# 这部分可以写成类似AlgoConfig的形式
factors_need_updated_list = \
    [
        ['FactorDailyIBBias', {'n': 8, 'x': 'PROJ_MATL', 'y': 'TOT_PROFIT'}, 'F_D_GPX0003.h5'],
        ['FactorDailyIBBias', {'n': 8, 'x': 'DEFERRED_TAX_LIAB', 'y': 'NET_PROFIT_EXCL_MIN_INT_INC'}, 'F_D_GPX0004.h5'],
        ['FactorDailyIBBias', {'n': 8, 'x': 'INT_PAYABLE', 'y': 'INC_TAX'}, 'F_D_GPX0005.h5'],
        ['FactorDailyIBBias', {'n': 8, 'x': 'NOTES_PAYABLE', 'y': 'NET_PROFIT_AFTER_DED_NR_LP'}, 'F_D_GPX0008.h5'],
        ['FactorDailyIBBias', {'n': 8, 'x': 'ST_BORROW', 'y': 'NET_PROFIT_AFTER_DED_NR_LP'}, 'F_D_GPX0010.h5'],
        ['FactorDailyIBBias', {'n': 8, 'x': 'NOTES_RCV', 'y': 'TOT_COMPREH_INC'}, 'F_D_GPX0013.h5'],
        ['FactorDailyIBBias', {'n': 8, 'x': 'LT_BORROW', 'y': 'TOT_PROFIT'}, 'F_D_GPX0014.h5'],
        ['FactorDailyIBBias', {'n': 8, 'x': 'INT_RCV', 'y': 'S_FA_EPS_DILUTED'}, 'F_D_GPX0019.h5'],
        ['FactorDailyIBBias', {'n': 8, 'x': 'DVD_PAYABLE', 'y': 'TOT_COMPREH_INC'}, 'F_D_GPX0022.h5'],
        ['FactorDailyIBBias', {'n': 8, 'x': 'INT_PAYABLE', 'y': 'LESS_TAXES_SURCHARGES_OPS'}, 'F_D_GPX0023.h5'],
        ['FactorDailyIBBias', {'n': 8, 'x': 'DEFERRED_TAX_LIAB', 'y': 'LESS_OPER_COST'}, 'F_D_GPX0027.h5'],
        ['FactorDailyIBBias', {'n': 8, 'x': 'DEFERRED_TAX_LIAB', 'y': 'LESS_TAXES_SURCHARGES_OPS'}, 'F_D_GPX0029.h5'],
        ['FactorDailyIBBias', {'n': 8, 'x': 'NOTES_PAYABLE', 'y': 'LESS_OPER_COST'}, 'F_D_GPX0033.h5'],
        ['FactorDailyIBBias', {'n': 8, 'x': 'OTH_NON_CUR_ASSETS', 'y': 'INC_TAX'}, 'F_D_GPX0035.h5'],
        ['FactorDailyIBBias', {'n': 8, 'x': 'FIN_ASSETS_AVAIL_FOR_SALE', 'y': 'TOT_COMPREH_INC'}, 'F_D_GPX0036.h5'],
        ['FactorDailyIBBias', {'n': 8, 'x': 'DEFERRED_TAX_ASSETS', 'y': 'LESS_GERL_ADMIN_EXP'}, 'F_D_GPX0037.h5'],
        ['FactorDailyIBBias', {'n': 8, 'x': 'ST_BORROW', 'y': 'INC_TAX'}, 'F_D_GPX0039.h5'],
        ['FactorDailyIBBias', {'n': 8, 'x': 'EMPL_BEN_PAYABLE', 'y': 'TOT_OPER_COST'}, 'F_D_GPX0040.h5'],
        ['FactorDailyIBBias', {'n': 8, 'x': 'OTH_NON_CUR_ASSETS', 'y': 'TOT_OPER_COST'}, 'F_D_GPX0041.h5'],
        ['FactorDailyIBBias', {'n': 8, 'x': 'TAXES_SURCHARGES_PAYABLE', 'y': 'S_FA_EPS_DILUTED'}, 'F_D_GPX0046.h5'],
        ['FactorDailyIBBias', {'n': 8, 'x': 'EMPL_BEN_PAYABLE', 'y': 'LESS_TAXES_SURCHARGES_OPS'}, 'F_D_GPX0047.h5'],
        ['FactorDailyIBBias', {'n': 8, 'x': 'INT_RCV', 'y': 'TOT_OPER_REV'}, 'F_D_GPX0050.h5'],
        ['FactorDailyIBBias', {'n': 8, 'x': 'INT_PAYABLE', 'y': 'S_FA_EPS_DILUTED'}, 'F_D_GPX0051.h5'],
        ['FactorDailyIBBias', {'n': 8, 'x': 'FIN_ASSETS_AVAIL_FOR_SALE', 'y': 'INC_TAX'}, 'F_D_GPX0052.h5'],
        ['FactorDailyIBBias', {'n': 8, 'x': 'OTH_RCV', 'y': 'NET_PROFIT_AFTER_DED_NR_LP'}, 'F_D_GPX0059.h5'],
        ['FactorDailyIBBias', {'n': 8, 'x': 'ADV_FROM_CUST', 'y': 'NET_PROFIT_AFTER_DED_NR_LP'}, 'F_D_GPX0062.h5'],
        ['FactorDailyIBBias', {'n': 8, 'x': 'INT_RCV', 'y': 'NET_PROFIT_AFTER_DED_NR_LP'}, 'F_D_GPX0063.h5'],
        ['FactorDailyIBBias', {'n': 8, 'x': 'TAXES_SURCHARGES_PAYABLE', 'y': 'INC_TAX'}, 'F_D_GPX0064.h5'],
        ['FactorDailyIBBias', {'n': 8, 'x': 'NOTES_RCV', 'y': 'S_FA_EPS_BASIC'}, 'F_D_GPX0065.h5'],
        ['FactorDailyIBBias', {'n': 8, 'x': 'INVENTORIES', 'y': 'LESS_GERL_ADMIN_EXP'}, 'F_D_GPX0066.h5'],
        ['FactorDailyIBBias', {'n': 8, 'x': 'R_AND_D_COSTS', 'y': 'NET_PROFIT_AFTER_DED_NR_LP'}, 'F_D_GPX0068.h5'],
        ['FactorDailyIBBias', {'n': 8, 'x': 'NOTES_PAYABLE', 'y': 'LESS_GERL_ADMIN_EXP'}, 'F_D_GPX0069.h5'],
        ['FactorDailyIBBias', {'n': 8, 'x': 'NOTES_RCV', 'y': 'INC_TAX'}, 'F_D_GPX0072.h5'],
        ['FactorDailyIBBias', {'n': 8, 'x': 'OTH_NON_CUR_ASSETS', 'y': 'TOT_OPER_REV'}, 'F_D_GPX0080.h5'],
        ['FactorDailyIBBias', {'n': 8, 'x': 'DVD_PAYABLE', 'y': 'INC_TAX'}, 'F_D_GPX0081.h5'],
        ['FactorDailyIBBias', {'n': 8, 'x': 'TOT_CUR_ASSETS', 'y': 'LESS_GERL_ADMIN_EXP'}, 'F_D_GPX0082.h5'],
        ['FactorDailyIBBias', {'n': 8, 'x': 'NOTES_PAYABLE', 'y': 'LESS_TAXES_SURCHARGES_OPS'}, 'F_D_GPX0083.h5'],
        ['FactorDailyIBBias', {'n': 8, 'x': 'R_AND_D_COSTS', 'y': 'OPER_PROFIT'}, 'F_D_GPX0085.h5'],
        ['FactorDailyIBBias', {'n': 8, 'x': 'INVENTORIES', 'y': 'TOT_COMPREH_INC'}, 'F_D_GPX0087.h5'],
        ['FactorDailyIBBias', {'n': 8, 'x': 'OTH_NON_CUR_ASSETS', 'y': 'OPER_PROFIT'}, 'F_D_GPX0090.h5'],
        ['FactorDailyIBBias', {'n': 8, 'x': 'INVENTORIES', 'y': 'OPER_REV'}, 'F_D_GPX0092.h5'],
        ['FactorDailyIBBias', {'n': 8, 'x': 'NOTES_RCV', 'y': 'LESS_TAXES_SURCHARGES_OPS'}, 'F_D_GPX0095.h5'],
        ['FactorDailyIBBias', {'n': 8, 'x': 'INT_RCV', 'y': 'OPER_REV'}, 'F_D_GPX0098.h5'],
        ['FactorDailyIBBias', {'n': 8, 'x': 'NOTES_PAYABLE', 'y': 'TOT_COMPREH_INC_PARENT_COMP'}, 'F_D_GPX0102.h5'],
        ['FactorDailyIBBias', {'n': 8, 'x': 'TOT_CUR_ASSETS', 'y': 'INC_TAX'}, 'F_D_GPX0103.h5'],
        ['FactorDailyIBBias', {'n': 8, 'x': 'ACCT_PAYABLE', 'y': 'NET_PROFIT_AFTER_DED_NR_LP'}, 'F_D_GPX0105.h5'],
        ['FactorDailyIBBias', {'n': 8, 'x': 'INVENTORIES', 'y': 'LESS_TAXES_SURCHARGES_OPS'}, 'F_D_GPX0106.h5'],
        ['FactorDailyIBBias', {'n': 8, 'x': 'NON_CUR_LIAB_DUE_WITHIN_1Y', 'y': 'LESS_GERL_ADMIN_EXP'}, 'F_D_GPX0108.h5'],
        ['FactorDailyIBBias', {'n': 8, 'x': 'OTH_PAYABLE', 'y': 'NET_PROFIT_AFTER_DED_NR_LP'}, 'F_D_GPX0110.h5'],
        ['FactorDailyIBBias', {'n': 8, 'x': 'MONETARY_CAP', 'y': 'NET_PROFIT_INCL_MIN_INT_INC'}, 'F_D_GPX0112.h5'],
        ['FactorDailyIBBias', {'n': 8, 'x': 'R_AND_D_COSTS', 'y': 'TOT_PROFIT'}, 'F_D_GPX0114.h5'],
        ['FactorDailyIBBias', {'n': 8, 'x': 'EMPL_BEN_PAYABLE', 'y': 'NET_PROFIT_EXCL_MIN_INT_INC'}, 'F_D_GPX0119.h5'],
        ['FactorDailyIBBias', {'n': 8, 'x': 'TOT_NON_CUR_ASSETS', 'y': 'INC_TAX'}, 'F_D_GPX0124.h5'],
        ['FactorDailyIBBias', {'n': 8, 'x': 'CONST_IN_PROG', 'y': 'NET_PROFIT_AFTER_DED_NR_LP'}, 'F_D_GPX0125.h5'],
        ['FactorDailyIBBias', {'n': 8, 'x': 'R_AND_D_COSTS', 'y': 'INC_TAX'}, 'F_D_GPX0126.h5'],
        ['FactorDailyIBBias', {'n': 8, 'x': 'ACCT_PAYABLE', 'y': 'LESS_OPER_COST'}, 'F_D_GPX0127.h5'],
        ['FactorDailyIBBias', {'n': 8, 'x': 'TOT_CUR_LIAB', 'y': 'S_FA_EPS_BASIC'}, 'F_D_GPX0128.h5'],
        ['FactorDailyIBBias', {'n': 8, 'x': 'PREPAY', 'y': 'NET_PROFIT_AFTER_DED_NR_LP'}, 'F_D_GPX0129.h5'],
        ['FactorDailyIBBias', {'n': 8, 'x': 'ST_BORROW', 'y': 'TOT_OPER_REV'}, 'F_D_GPX0131.h5'],
        ['FactorDailyIBBias', {'n': 8, 'x': 'NON_CUR_LIAB_DUE_WITHIN_1Y', 'y': 'OPER_PROFIT'}, 'F_D_GPX0132.h5'],
        ['FactorDailyIBBias', {'n': 8, 'x': 'INT_RCV', 'y': 'S_FA_EPS_BASIC'}, 'F_D_GPX0134.h5'],
        ['FactorDailyIBBias', {'n': 8, 'x': 'R_AND_D_COSTS', 'y': 'TOT_COMPREH_INC_PARENT_COMP'}, 'F_D_GPX0138.h5'],
        ['FactorDailyIBBias', {'n': 8, 'x': 'DEFERRED_TAX_LIAB', 'y': 'LESS_GERL_ADMIN_EXP'}, 'F_D_GPX0141.h5'],
        ['FactorDailyIBBias', {'n': 8, 'x': 'NON_CUR_LIAB_DUE_WITHIN_1Y', 'y': 'TOT_COMPREH_INC'}, 'F_D_GPX0144.h5'],
        ['FactorDailyIBBias', {'n': 8, 'x': 'TAXES_SURCHARGES_PAYABLE', 'y': 'LESS_OPER_COST'}, 'F_D_GPX0145.h5'],
        ['FactorDailyIBBias', {'n': 8, 'x': 'MONETARY_CAP', 'y': 'TOT_COMPREH_INC'}, 'F_D_GPX0146.h5'],
        ['FactorDailyIBBias', {'n': 8, 'x': 'OTH_CUR_ASSETS', 'y': 'INC_TAX'}, 'F_D_GPX0148.h5'],
        ['FactorDailyIBBias', {'n': 8, 'x': 'TOT_CUR_LIAB', 'y': 'LESS_GERL_ADMIN_EXP'}, 'F_D_GPX0149.h5'],
        ['FactorDailyIBBias', {'n': 8, 'x': 'FIN_ASSETS_AVAIL_FOR_SALE', 'y': 'TOT_COMPREH_INC_PARENT_COMP'}, 'F_D_GPX0151.h5'],
        ['FactorDailyIBBias', {'n': 8, 'x': 'INT_PAYABLE', 'y': 'TOT_PROFIT'}, 'F_D_GPX0152.h5'],
        ['FactorDailyIBBias', {'n': 8, 'x': 'OTH_CUR_ASSETS', 'y': 'NET_PROFIT_EXCL_MIN_INT_INC'}, 'F_D_GPX0157.h5'],
        ['FactorDailyIBBias', {'n': 8, 'x': 'OTH_NON_CUR_ASSETS', 'y': 'TOT_COMPREH_INC_PARENT_COMP'}, 'F_D_GPX0159.h5'],
        ['FactorDailyIBBias', {'n': 8, 'x': 'ACCT_RCV', 'y': 'TOT_COMPREH_INC_PARENT_COMP'}, 'F_D_GPX0167.h5'],
        ['FactorDailyIBBias', {'n': 8, 'x': 'CONST_IN_PROG', 'y': 'TOT_OPER_REV'}, 'F_D_GPX0170.h5'],
        ['FactorDailyIBBias', {'n': 8, 'x': 'LT_BORROW', 'y': 'NET_PROFIT_AFTER_DED_NR_LP'}, 'F_D_GPX0172.h5'],
        ['FactorDailyICBias', {'n': 8, 'x': 'NET_INCR_CASH_CASH_EQU', 'y': 'S_FA_EPS_DILUTED'}, 'F_D_GPX0174.h5'],
        ['FactorDailyICBias', {'n': 8, 'x': 'CASH_RECP_SG_AND_RS', 'y': 'S_FA_EPS_BASIC'}, 'F_D_GPX0175.h5'],
        ['FactorDailyICBias', {'n': 8, 'x': 'NET_CASH_FLOWS_OPER_ACT', 'y': 'S_FA_EPS_DILUTED'}, 'F_D_GPX0176.h5'],
        ['FactorDailyICBias', {'n': 8, 'x': 'NET_CASH_FLOWS_FNC_ACT', 'y': 'INC_TAX'}, 'F_D_GPX0177.h5'],
        ['FactorDailyICBias', {'n': 8, 'x': 'NET_CASH_FLOWS_INV_ACT', 'y': 'S_FA_EPS_BASIC'}, 'F_D_GPX0178.h5'],
        ['FactorDailyICBias', {'n': 8, 'x': 'NET_INCR_CASH_CASH_EQU', 'y': 'NET_PROFIT_AFTER_DED_NR_LP'}, 'F_D_GPX0179.h5'],
        ['FactorDailyICBias', {'n': 8, 'x': 'OTHER_CASH_PAY_RAL_OPER_ACT', 'y': 'S_FA_EPS_DILUTED'}, 'F_D_GPX0180.h5'],
        ['FactorDailyICBias', {'n': 8, 'x': 'CASH_PAY_ACQ_CONST_FIOLTA', 'y': 'INC_TAX'}, 'F_D_GPX0181.h5'],
        ['FactorDailyICBias', {'n': 8, 'x': 'NET_CASH_FLOWS_FNC_ACT', 'y': 'TOT_COMPREH_INC'}, 'F_D_GPX0182.h5'],
        ['FactorDailyICBias', {'n': 8, 'x': 'NET_CASH_FLOWS_OPER_ACT', 'y': 'TOT_COMPREH_INC'}, 'F_D_GPX0183.h5'],
        ['FactorDailyICBias', {'n': 8, 'x': 'CASH_PAY_DIST_DPCP_INT_EXP', 'y': 'S_FA_EPS_DILUTED'}, 'F_D_GPX0184.h5'],
        ['FactorDailyICBias', {'n': 8, 'x': 'STOT_CASH_OUTFLOWS_INV_ACT', 'y': 'LESS_TAXES_SURCHARGES_OPS'}, 'F_D_GPX0185.h5'],
        ['FactorDailyICBias', {'n': 8, 'x': 'NET_CASH_FLOWS_FNC_ACT', 'y': 'TOT_OPER_COST'}, 'F_D_GPX0186.h5'],
        ['FactorDailyICBias', {'n': 8, 'x': 'CASH_PAY_ACQ_CONST_FIOLTA', 'y': 'LESS_TAXES_SURCHARGES_OPS'}, 'F_D_GPX0187.h5'],
        ['FactorDailyICBias', {'n': 8, 'x': 'STOT_CASH_OUTFLOWS_FNC_ACT', 'y': 'TOT_COMPREH_INC'}, 'F_D_GPX0188.h5'],
        ['FactorDailyICBias', {'n': 8, 'x': 'CASH_PAY_ACQ_CONST_FIOLTA', 'y': 'LESS_OPER_COST'}, 'F_D_GPX0189.h5'],
        ['FactorDailyICBias', {'n': 8, 'x': 'NET_CASH_FLOWS_INV_ACT', 'y': 'INC_TAX'}, 'F_D_GPX0190.h5'],
        ['FactorDailyICBias', {'n': 8, 'x': 'NET_INCR_CASH_CASH_EQU', 'y': 'LESS_TAXES_SURCHARGES_OPS'}, 'F_D_GPX0191.h5'],
        ['FactorDailyICBias', {'n': 8, 'x': 'STOT_CASH_INFLOWS_OPER_ACT', 'y': 'OPER_PROFIT'}, 'F_D_GPX0192.h5'],
        ['FactorDailyICBias', {'n': 8, 'x': 'STOT_CASH_OUTFLOWS_FNC_ACT', 'y': 'INC_TAX'}, 'F_D_GPX0193.h5'],
        ['FactorDailyICBias', {'n': 8, 'x': 'OTHER_CASH_RECP_RAL_OPER_ACT', 'y': 'LESS_GERL_ADMIN_EXP'}, 'F_D_GPX0194.h5'],
        ['FactorDailyICBias', {'n': 8, 'x': 'NET_CASH_FLOWS_OPER_ACT', 'y': 'INC_TAX'}, 'F_D_GPX0195.h5'],
        ['FactorDailyICBias', {'n': 8, 'x': 'STOT_CASH_OUTFLOWS_FNC_ACT', 'y': 'TOT_OPER_REV'}, 'F_D_GPX0196.h5'],
        ['FactorDailyICBias', {'n': 8, 'x': 'OTHER_CASH_RECP_RAL_OPER_ACT', 'y': 'OPER_PROFIT'}, 'F_D_GPX0197.h5'],
        ['FactorDailyICBias', {'n': 8, 'x': 'CASH_PAY_BEH_EMPL', 'y': 'TOT_COMPREH_INC'}, 'F_D_GPX0198.h5'],
        ['FactorDailyICBias', {'n': 8, 'x': 'STOT_CASH_OUTFLOWS_INV_ACT', 'y': 'NET_PROFIT_EXCL_MIN_INT_INC'}, 'F_D_GPX0199.h5'],
        ['FactorDailyICBias', {'n': 8, 'x': 'NET_CASH_FLOWS_OPER_ACT', 'y': 'LESS_GERL_ADMIN_EXP'}, 'F_D_GPX0200.h5'],
        ['FactorDailyICBias', {'n': 8, 'x': 'CASH_PAY_DIST_DPCP_INT_EXP', 'y': 'TOT_COMPREH_INC_PARENT_COMP'}, 'F_D_GPX0201.h5'],
        ['FactorDailyICBias', {'n': 8, 'x': 'CASH_PAY_GOODS_PURCH_SERV_REC', 'y': 'TOT_OPER_COST'}, 'F_D_GPX0202.h5'],
        ['FactorDailyICBeta', {'n': 8, 'x': 'PAY_ALL_TYP_TAX', 'y': 'LESS_GERL_ADMIN_EXP'}, 'F_D_GPX0203.h5'],
        ['FactorDailyICBeta', {'n': 8, 'x': 'STOT_CASH_OUTFLOWS_OPER_ACT', 'y': 'INC_TAX'}, 'F_D_GPX0204.h5'],
        ['FactorDailyICBeta', {'n': 8, 'x': 'CASH_PAY_BEH_EMPL', 'y': 'LESS_GERL_ADMIN_EXP'}, 'F_D_GPX0205.h5'],
        ['FactorDailyICBeta', {'n': 8, 'x': 'CASH_PAY_GOODS_PURCH_SERV_REC', 'y': 'LESS_TAXES_SURCHARGES_OPS'}, 'F_D_GPX0206.h5'],
        ['FactorDailyICBeta', {'n': 8, 'x': 'NET_CASH_FLOWS_OPER_ACT', 'y': 'TOT_OPER_COST'}, 'F_D_GPX0207.h5'],
        ['FactorDailyICBeta', {'n': 8, 'x': 'STOT_CASH_INFLOWS_OPER_ACT', 'y': 'TOT_OPER_COST'}, 'F_D_GPX0208.h5'],
        ['FactorDailyICBeta', {'n': 8, 'x': 'NET_CASH_FLOWS_OPER_ACT', 'y': 'NET_PROFIT_INCL_MIN_INT_INC'}, 'F_D_GPX0209.h5'],
        ['FactorDailyBCBias', {'n': 8, 'x': 'MONETARY_CAP', 'y': 'CASH_PAY_ACQ_CONST_FIOLTA'}, 'F_D_GPX0210.h5'],
        ['FactorDailyBCBias', {'n': 8, 'x': 'ACCT_RCV', 'y': 'CASH_PAY_ACQ_CONST_FIOLTA'}, 'F_D_GPX0211.h5'],
        ['FactorDailyBCBias', {'n': 8, 'x': 'NOTES_RCV', 'y': 'CASH_CASH_EQU_END_PERIOD'}, 'F_D_GPX0212.h5'],
        ['FactorDailyBCBias', {'n': 8, 'x': 'NOTES_RCV', 'y': 'CASH_PAY_ACQ_CONST_FIOLTA'}, 'F_D_GPX0213.h5'],
        ['FactorDailyBCBias', {'n': 8, 'x': 'MONETARY_CAP', 'y': 'OTHER_CASH_RECP_RAL_OPER_ACT'}, 'F_D_GPX0214.h5'],
        ['FactorDailyBCBias', {'n': 8, 'x': 'PREPAY', 'y': 'CASH_CASH_EQU_END_PERIOD'}, 'F_D_GPX0215.h5'],
        ['FactorDailyBCBias', {'n': 8, 'x': 'MONETARY_CAP', 'y': 'NET_CASH_FLOWS_INV_ACT'}, 'F_D_GPX0216.h5'],
        ['FactorDailyBCBias', {'n': 8, 'x': 'PREPAY', 'y': 'STOT_CASH_OUTFLOWS_FNC_ACT'}, 'F_D_GPX0217.h5'],
        ['FactorDailyBCBias', {'n': 8, 'x': 'OTH_RCV', 'y': 'CASH_PAY_ACQ_CONST_FIOLTA'}, 'F_D_GPX0218.h5'],
        ['FactorDailyBCBias', {'n': 8, 'x': 'PREPAY', 'y': 'CASH_PAY_ACQ_CONST_FIOLTA'}, 'F_D_GPX0219.h5'],
        ['FactorDailyBCBias', {'n': 8, 'x': 'ACCT_RCV', 'y': 'CASH_PAY_DIST_DPCP_INT_EXP'}, 'F_D_GPX0220.h5'],
        ['FactorDailyBCBias', {'n': 8, 'x': 'MONETARY_CAP', 'y': 'CASH_RECP_SG_AND_RS'}, 'F_D_GPX0221.h5'],
        ['FactorDailyBCBias', {'n': 8, 'x': 'NOTES_RCV', 'y': 'CASH_PAY_DIST_DPCP_INT_EXP'}, 'F_D_GPX0222.h5'],
        ['FactorDailyBCBias', {'n': 8, 'x': 'NOTES_RCV', 'y': 'CASH_PAY_BEH_EMPL'}, 'F_D_GPX0223.h5'],
        ['FactorDailyBCBias', {'n': 8, 'x': 'NOTES_RCV', 'y': 'PAY_ALL_TYP_TAX'}, 'F_D_GPX0224.h5'],
        ['FactorDailyBCBias', {'n': 8, 'x': 'CONST_IN_PROG', 'y': 'CASH_PAY_ACQ_CONST_FIOLTA'}, 'F_D_GPX0225.h5'],
        ['FactorDailyBCBias', {'n': 8, 'x': 'PREPAY', 'y': 'CASH_PAY_DIST_DPCP_INT_EXP'}, 'F_D_GPX0226.h5'],
        ['FactorDailyBCBias', {'n': 8, 'x': 'CONST_IN_PROG', 'y': 'CASH_CASH_EQU_END_PERIOD'}, 'F_D_GPX0227.h5'],
        ['FactorDailyBCBias', {'n': 8, 'x': 'TOT_NON_CUR_ASSETS', 'y': 'CASH_CASH_EQU_END_PERIOD'}, 'F_D_GPX0228.h5'],
        ['FactorDailyBCBias', {'n': 8, 'x': 'MONETARY_CAP', 'y': 'STOT_CASH_OUTFLOWS_FNC_ACT'}, 'F_D_GPX0229.h5'],
        ['FactorDailyBCBias', {'n': 8, 'x': 'DEFERRED_TAX_ASSETS', 'y': 'CASH_CASH_EQU_END_PERIOD'}, 'F_D_GPX0230.h5'],
        ['FactorDailyBCBias', {'n': 8, 'x': 'OTH_RCV', 'y': 'CASH_PAY_DIST_DPCP_INT_EXP'}, 'F_D_GPX0231.h5'],
        ['FactorDailyBCBias', {'n': 8, 'x': 'PREPAY', 'y': 'OTHER_CASH_PAY_RAL_OPER_ACT'}, 'F_D_GPX0232.h5'],
        ['FactorDailyBCBias', {'n': 8, 'x': 'ACCT_RCV', 'y': 'NET_CASH_FLOWS_OPER_ACT'}, 'F_D_GPX0233.h5'],
        ['FactorDailyBCBias', {'n': 8, 'x': 'LONG_TERM_DEFERRED_EXP', 'y': 'STOT_CASH_INFLOWS_OPER_ACT'},'F_D_GPX0234.h5'],
        ['FactorDailyBCBias', {'n': 8, 'x': 'INVENTORIES', 'y': 'CASH_PAY_DIST_DPCP_INT_EXP'}, 'F_D_GPX0235.h5'],
        ['FactorDailyBCBias', {'n': 8, 'x': 'DEFERRED_TAX_ASSETS', 'y': 'CASH_PAY_DIST_DPCP_INT_EXP'}, 'F_D_GPX0236.h5']

    ]

if platform.system() == "Windows":
    stock_list = Dtk.get_complete_stock_list()
else:
    stock_list = Dtk.get_complete_stock_list()

start_date = 20131001
end_date = 20191231

if platform.system() == "Windows":
    alpha_factor_root_path = "D:\\NewFactorData"
elif os.system("nvidia-smi") == 0:
    alpha_factor_root_path = "/data/NewFactorData"
else:
    user_id = os.environ['USER_ID']
    alpha_factor_root_path = "/app/data/" + user_id + "/Apollo"

# ------------需要设定的部分到此为止-----------------------------------------

factor_path_dir = os.path.join(alpha_factor_root_path, "AlphaFactors")
nonfactor_path_dir = os.path.join(alpha_factor_root_path, "AlphaNonFactors")

if not os.path.exists(alpha_factor_root_path):
    os.mkdir(alpha_factor_root_path)
if not os.path.exists(factor_path_dir):
    os.mkdir(factor_path_dir)
if not os.path.exists(nonfactor_path_dir):
    os.mkdir(nonfactor_path_dir)


def check_future_data(ans_date: pd.DataFrame):
    last_series = ans_date.iloc[-1]
    if np.isnan(last_series).all():
        print('WARNING!!! ALL DATA IN LAST DAY IS NAN. IT MAY INCLUDE FUTURE DATA')
    return


def main():
    # 将列表中待更新的因子逐个更新
    for i_factor in factors_need_updated_list:
        t1 = dt.datetime.now()
        file_name = i_factor[2]
        if i_factor[0][0:3] == "Fac":
            factor_module = import_module("Factor." + i_factor[0])
            output_file_path = os.path.join(factor_path_dir, file_name)
            valid_start_date = start_date
        else:
            factor_module = import_module("NonFactor." + i_factor[0])
            output_file_path = os.path.join(nonfactor_path_dir, file_name)
            if not os.path.exists(output_file_path):
                # 如因子值文件不存在，则考虑到ema或ma的问题，将valid_start_date再往前推30个交易日
                valid_start_date = Dtk.get_n_days_off(start_date, -30)[0]
            else:
                valid_start_date = start_date
        class_name = getattr(factor_module, i_factor[0])
        # 初始化因子类
        factor_obj = class_name(alpha_factor_root_path, stock_list, valid_start_date, end_date, i_factor[1])
        # 计算因子
        ans_df = factor_obj.factor_calc()
        check_future_data(ans_df)
        # 如没有因子文件则创设之
        if not os.path.exists(output_file_path):
            pd.set_option('io.hdf.default_format', 'table')
            store = pd.HDFStore(output_file_path)
            store.put("stock_list", pd.DataFrame(stock_list, columns=['code']))
            store.put("factor", ans_df, format="table")
            store.flush()
            store.close()
            print("Factor file",  file_name, "was created.")
        # 如已有因子文件，则更新之；如遇日期重叠的部分，以新计算的为准
        else:
            store = pd.HDFStore(output_file_path)
            original_data_df = store.select("/factor")
            if original_data_df.index[-1] < ans_df.index[0]:
                ans_df2 = pd.concat([original_data_df, ans_df])
            else:
                ans_df2 = pd.concat([original_data_df.loc[:ans_df.index[0] - 1], ans_df])
            new_stock_list = list(ans_df2.columns)
            if new_stock_list.__len__() > list(original_data_df.columns).__len__():
                store.put("stock_list", pd.DataFrame(new_stock_list, columns=['code']))
            store.put("factor", ans_df2, format="table")
            store.flush()
            store.close()
            print("Factor_file", file_name, "was updated to", end_date, ".")
        t2 = dt.datetime.now()
        print(i_factor[0], t2 - t1)

if __name__ == '__main__':
    main()
