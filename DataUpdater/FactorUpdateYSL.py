#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019/2/21 14:28
# @Author  : 011673
import DataAPI.DataToolkit as Dtk
from importlib import import_module
import os
import pandas as pd
import platform
import datetime as dt
import numpy as np

# -----------------------------------------------------------------------------
# 需要更新的因子名写在下列列表中，内容依次是模块名、参数列表、因子文件名
# 若是因子，以Factor开头；若是非因子（即因子原始值），以NonFactor开头
# 这部分可以写成类似AlgoConfig的形式
factors_need_updated_list = \
    [
        ['NonFactorDailyUpVolatilityRatio', {'n': 20, 'forward_date': 22}, 'NF_D_UpVolatilityRatio_20.h5'],
        ['FactorDailyUpVolatilityRatio', {'n': 20}, 'F_D_UpVolatilityRatio_20.h5'],
        ['NonFactorDailyDownVolatilityRatio', {'n': 20, 'forward_date': 22}, 'NF_D_DownVolatilityRatio_20.h5'],
        ['FactorDailyDownVolatilityRatio', {'n': 20, 'forward_date': 22}, 'F_D_DownVolatilityRatio_20.h5'],
        ['FactorDailyMomentum_nRet', {'n': 20, 'forward_date': 22}, 'F_D_MomentumRet_20.h5'],
        #
        # ['FactorDailyMomentum_PingPongRebound',{'n':20},'F_D_PingPongRebound_20.h5'],
        # ['NonFactorDaily_Exp_Wgt_Return_nm',{'n':12,'forward_date':14},'NF_D_Exp_Wgt_Return_ema_12.h5'],
        # ['FactorDaily_Exp_Wgt_Return_nm',{'n':12,},'F_D_Exp_Wgt_Return_ema_12.h5'],
        # ['NonFactorDailyRSkew',{'n':20,'forward_date':22},'NF_D_RSkew_20.h5'],
        # ['FactorDailyRSkew',{'n':20},'F_D_RSkew_20.h5'],
        #
        # ['NonFactorDailyShoutCutILLIQ',{'n':10,'forward_date':12},"NF_D_ShoutCutILLIQ_10.h5"],
        # ['FactorDailyShoutCutILLIQ',{'n':10},"F_D_ShoutCutILLIQ_10.h5"],
        # ['FactorDailyBeforehandRet',{'n':20},"F_D_BeforehandRet_20.h5"],
        # ['FactorDailyBeforehandRetCut',{'n':20},'F_D_BeforehandRetCut20.h5'],

        ['FactorDailyBeforehandRetCut', {'n': 30}, 'F_D_BeforehandRetCut30.h5'],
        ['FactorDailyBeforehandRetResidual', {'n': 20}, 'F_D_BeforehandRetResidual20.h5'],
        ['FactorDailyBeforehandRetResidual', {'n': 30}, 'F_D_BeforehandRetResidual30.h5'],

        # ['FactorDailySeperateBeforehandRet',{'n':20},'F_D_SeperateBeforehandRet_20.h5'],
        # ['FactorDailySeperateBeforehandRet', {'n': 30}, 'F_D_SeperateBeforehandRet_30.h5'],
        # ['FactorDailySeperateBeforehandRetResidual',{'n':20},'F_D_SeperateBeforehandRetResidual_20.h5'],
        #
        # ['FactorDailySeperateBeforehandRetResidual',{'n':30},'F_D_SeperateBeforehandRetResidual_30.h5'],
        # ['NonFactorDailySeperateBeforehandRetNormolize',{'n':20,'forward_date':142},'NF_D_SeperateBeforehandRet_Normolized20.h5'],
        # ['FactorDailySeperateBeforehandRetNormolize',{'n':20},'F_D_SeperateBeforehandRet_Normolized20.h5'],
        # ['NonFactorDailySeperateBeforehandRetNormolize',{'n':30,'forward_date':162},'NF_D_SeperateBeforehandRet_Normolized30.h5'],
        # ['FactorDailySeperateBeforehandRetNormolize',{'n':30},'F_D_SeperateBeforehandRet_Normolized30.h5'],

        ['FactorDailyMomentumDeindustry', {}, 'F_D_MomentumDeindustry.h5'],
        ['NonFactorDailyOppsiteIndexMomentum', {'forward_date': 22}, 'NF_D_OppsiteIndexMomentum.h5'],
        ['FactorDailyOppsiteIndexMomentum', {'n': 5}, 'F_D_OppsiteIndexMomentum.h5'],
        ['FactorDailyCorrIndexVolumeRet', {'n': 20}, 'F_D_CorrIndexVolumeRet.h5'],

        # ['FactorDailyExceedRetStd',{'n':20},'F_D_ExceedRetStd.h5'],
        # ['FactorDailyExceedSwingCorAmt',{'n':25},'F_D_ExceedSwingCorAmt.h5'],

        ['NonFactorDailyMinTrendStrength', {}, 'NF_D_MinTrendStrength.h5'],
        ['FactorDailyMinTrendStrength', {}, 'F_D_MinTrendStrength.h5'],
        ['FactorDailyMinTrendStrength_ema', {}, 'F_D_MinTrendStrength_ema.h5'],
        ['NonFactorDailyMinILLIQ', {}, 'NF_D_MinILLIQ.h5'],
        ['FactorDailyMinILLIQ', {}, 'F_D_MinILLIQ.h5'],
        ['NonFactorDailyMinShortcutILLIQ', {}, 'NF_D_MinShortcutILLIQ.h5'],
        ['FactorDailyMinShortcutILLIQ', {}, 'F_D_MinShortcutILLIQ.h5'],
        #
        # ['NonFactorDailyMinSeperateMomentum',{'ratio':[1,1,1,1,1]},'NF_D_MinSeperateMomentum_ema_eq.h5'],
        # ['FactorDailyMinSeperateMomentum',{'eq':'_eq'},'F_D_MinSeperateMomentum_ema_eq.h5'],
        # ['NonFactorDailyMinSeperateMomentum', {'ratio': [0.5, 0.5, 0.75, 1.5, 1]}, 'NF_D_MinSeperateMomentum_ema.h5'],
        # ['FactorDailyMinSeperateMomentum',{'eq':''},'F_D_MinSeperateMomentum_ema.h5'],
        # ['NonFactorDailyMinVolumeRatio',{},'NF_D_MinVolumeRatio_ema.h5'],
        # ['FactorDailyMinVolumeRatio',{},'F_D_MinVolumeRatio_ema.h5'],
        #
        # ['NonFactorDailyMinSeperateCorr',{'n':10},'NF_D_MinSeperateCorr.h5'],
        # ['FactorDailyMinSeperateCorr',{'n':10},'F_D_MinSeperateCorr.h5'],
        # ['NonFactorDailyMinActiveVolumeRatio',{},'NF_D_MinActiveVolumeRatio_ema_10.h5'],
        # ['FactorDailyMinActiveVolumeRatio',{},'F_D_MinActiveVolumeRatio_ema_10.h5'],
        # ['NonFactorDailyMinExceedIndexRet_ema',{'n':10},'NF_D_MinExceedIndexRet_ema15.h5'],
        # ['FactorDailyMinExceedIndexRet_ema',{},'F_D_MinExceedIndexRet_ema15.h5'],

        ['NonFactorDailyMinIndexVolCorr', {'n': 10}, 'NF_D_MinIndexVolCorr_ema15.h5'],
        ['FactorDailyMinIndexVolCorr', {}, 'F_D_MinIndexVolCorr_ema15.h5'],
        ['NonFactorDailyMinExceedIndexVolumeRet_ema', {'n': 10}, 'NF_D_MinExceedIndexVolumeRet_ema15.h5'],
        ['FactorDailyMinExceedIndexVolumeRet_ema', {}, 'F_D_MinExceedIndexVolumeRet_ema15.h5'],
        ['NonFactorDailyMinIndexRetMatching', {'n': 10}, 'NF_D_MinIndexRetMatching.h5'],
        ['FactorDailyMinIndexRetMatching', {}, 'F_D_MinIndexRetMatching.h5'],

        # ['NonFactorDailyTempRetForAPM',{'forward_date':22},'NF_D_TempRetForAPM.h5'],
        # ['NonFactorDailyMinAPMTemp',{'n':20},'NF_D_MinTempAPM.h5'],
        # ['FactorDailyMinAPM',{},'F_D_MinAPM_000300.h5'],
        # ['NonFactorDailyMinSkew', {}, 'NF_D_MinRskew.h5'],
        # ['FactorDailyMinSkew', {'ema_com': 10}, 'F_D_MinRskew_ema.h5'],
        # ["NonFactorDailyMin1030to1130VolRatio", {}, "NF_D_VolumeDailyMin1030-1130VolumeRatio.h5"],
        # ["FactorDailyMin1030to1130VolRatio", {"ema_com": 10}, "F_D_Min1030-1130VolumeRatio_ema.h5"],

        ['FactorDailyMomentumAdj', {}, 'NF_D_MomentumAdj'],
        ['NonFactorDailyMinClosePeriodAmtRatio', {}, 'NF_D_MinClosePeriodAmtRatio.h5'],
        ['FactorDailyMinClosePeriodAmtRatio', {'n': 10}, 'F_D_MinClosePeriodAmtRatio.h5'],
        ['NonFactorDailyMinMidPeriodAmtRatio', {}, 'NF_D_MinMidPeriodAmtRatio.h5'],
        ['FactorDailyMinMidPeriodAmtRatio', {'n': 10}, 'F_D_MinMidPeriodAmtRatio.h5'],
        ['NonFactorDailyMinAbnAmtRet',{},'NF_D_MinAbnAmtRet.h5'],
        ['FactorDailyMinAbnAmtRet',{'n':15},'F_D_MinAbnAmtRet.h5'],
        ['FactorDailyAbnAmtRet', {'n': 60}, 'F_D_AbnAmtRet.h5'],
        ['FactorDailyAbnRet', {'n': 60}, 'F_D_AbnRet.h5'],
        ['FactorDailyQuantileDistanceWithIndex', {'n': 40}, 'F_D_QuantDistance.h5']

    ]

if platform.system() == "Windows":
    stock_list = Dtk.get_complete_stock_list()
else:
    stock_list = Dtk.get_complete_stock_list()

start_date = 20131001
end_date = 20190222

if platform.system() == "Windows":
    alpha_factor_root_path = "D:\\NewFactorData"
elif os.system("nvidia-smi") == 0:
    alpha_factor_root_path = "/data/NewFactorData"
else:
    user_id = os.environ['USER_ID']
    alpha_factor_root_path = "/app/data/" + user_id + "/NewFactorData"

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
    if np.isnan(last_series.astype(float)).all():
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
                if 'forward_date' not in i_factor[1].keys():
                    date = 30
                else:
                    date = i_factor[1]['forward_date']
                # 如因子值文件不存在，则考虑到ema或ma的问题，将valid_start_date再往前推30个交易日
                valid_start_date = Dtk.get_n_days_off(start_date, -date)[0]
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
            print("Factor file", file_name, "was created.")
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
