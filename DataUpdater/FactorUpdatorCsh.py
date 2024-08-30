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
     ["NonFactorDailyFamaFrench", {}, "NF_D_FamaFrench.h5"],
     ["FactorDailyAmtPerVolatility", {"n": 5}, "F_D_AmtPerVolatility_5.h5"],
     ["FactorDailyAutoCorrAmt", {"n": 20}, "F_D_AutoCorrAmt_20.h5"],
     ["FactorDailyCorrAmpAmt", {"n": 20}, "F_D_CorrAmpAmt_20.h5"],
     ["FactorDailyCorrCloseTurn", {"n": 10}, "F_D_CorrCloseTurn_10.h5"],
     ["FactorDailyDisposition", {"n": 20}, "F_D_Disposition_20.h5"],
     ["FactorDailyHAlpha", {"index_code": '000300.SH', "n": 20}, "F_D_HAlpha_000300_20.h5"],
     ["FactorDailyHBeta", {"index_code": '000300.SH', "n": 20}, "F_D_HBeta_000300_20.h5"],
     ["FactorDailyIndayReturn", {"n": 15}, "F_D_IndayReturn_15.h5"],
     ["FactorDailyIVR", {"index_code": '000300.SH', "n": 20}, "F_D_IVR_000300_20.h5"],
     ["FactorDailyMaRatio", {"m": 5, "n": 20}, "F_D_MaRatio_5_20.h5"],
     ["FactorDailyMaxMinRatio", {"n": 10}, "F_D_MaxMinRatio_10.h5"],
     ["FactorDailyOvernightNegReturn", {"n": 3}, "F_D_OvernightNegReturn_3.h5"],
     ["FactorDailyStd", {"n": 20}, "F_D_Std_20.h5"],
     ["FactorDailyStdHigh", {"n": 20}, "F_D_StdHigh_20.h5"],
     ["FactorDailyStdHmL", {"n": 20}, "F_D_StdHmL_20.h5"],
     ["FactorDailyStdHpL", {"n": 20}, "F_D_StdHpL_20.h5"],
     ["FactorDailyStdId1", {"index_code": '000300.SH', "n": 20}, "F_D_StdId1_000300_20.h5"],
     ["FactorDailyStdId2", {"index_code": '000300.SH', "n": 20}, "F_D_StdId2_000300_20.h5"],
     ["FactorDailyStdId2Down", {"index_code": '000300.SH', "n": 20}, "F_D_StdId2Down_000300_20.h5"],
     ["FactorDailyStdId2Up", {"index_code": '000300.SH', "n": 20}, "F_D_StdId2Up_000300_20.h5"],
     ["FactorDailyStdId2UpD", {"index_code": '000300.SH', "n": 20}, "F_D_StdId2UpD_000300_20.h5"],
     ["FactorDailyTurn", {"n": 5}, "F_D_Turn_5.h5"],
     ["FactorDailyTurnCV", {"n": 10}, "F_D_TurnCV_10.h5"],
     ["FactorDailyTurnLog", {"n": 5}, "F_D_TurnLog_5.h5"],
     ["FactorDailyTurnNon", {"n": 5}, "F_D_TurnNon_5.h5"],
     ["FactorDailyTurnPure", {"n": 10}, "F_D_TurnPure_10.h5"],
     ["FactorDailyTurnStd", {"n": 10}, "F_D_TurnStd_10.h5"],
     ["FactorDailyVolRatioUpDown", {"n": 20}, "F_D_VolRatioUpDown_20.h5"],
     ["NonFactorDailyMinCloseVolumePercent", {}, "NF_D_MinCloseVolumePercent.h5"],
     ["FactorDailyMinCloseVolumePercent", {"rolling": 10}, "F_D_MinCloseVolumePercent_10.h5"],
     ["NonFactorDailyMinCorrPriceVolume", {}, "NF_D_MinCorrPriceVolume.h5"],
     ["FactorDailyMinCorrPriceVolume", {"rolling": 10}, "F_D_MinCorrPriceVolume_10.h5"],
     ["NonFactorDailyMinCorrRetVolume", {}, "NF_D_MinCorrRetVolume.h5"],
     ["FactorDailyMinCorrRetVolume", {"ema_com": 10}, "F_D_MinCorrRetVolume_10.h5"],
     ["NonFactorDailyMinSignAmplitude", {}, "NF_D_MinSignAmplitude.h5"],
     ["FactorDailyMinSignAmplitude", {"ema_com": 20}, "F_D_MinSignAmplitude_20.h5"],
     ["NonFactorDailyMinSignAmpVolume", {}, "NF_D_MinSignAmpVolume.h5"],
     ["FactorDailyMinSignAmpVolume", {"ema_com": 20}, "F_D_MinSignAmpVolume_20.h5"],
     ["NonFactorDailyMinTimeHighLow", {}, "NF_D_MinTimeHighLow.h5"],
     ["FactorDailyMinTimeHighLow", {"ema_com": 20}, "F_D_MinTimeHighLow_20.h5"],
     ["NonFactorDailyMinVolumeWeightedReturn", {}, "NF_D_MinVolumeWeightedReturn.h5"],
     ["FactorDailyMinVolumeWeightedReturn", {"ema_com": 20}, "F_D_MinVolumeWeightedReturn_20.h5"],
     ["NonFactorDailyMinTurnPart", {"period": 0}, "NF_D_MinTurnPart_0.h5"],
     ["NonFactorDailyMinTurnPart", {"period": 1}, "NF_D_MinTurnPart_1.h5"],
     ["NonFactorDailyMinTurnPart", {"period": 2}, "NF_D_MinTurnPart_2.h5"],
     ["NonFactorDailyMinTurnPart", {"period": 3}, "NF_D_MinTurnPart_3.h5"],
     ["NonFactorDailyMinTurnPart", {"period": 4}, "NF_D_MinTurnPart_4.h5"],
     ["FactorDailyMinTurnPart", {"period": 0, "rolling": 10}, "F_D_MinTurnPart_0_10.h5"],
     ["FactorDailyMinTurnPart", {"period": 1, "rolling": 10}, "F_D_MinTurnPart_1_10.h5"],
     ["FactorDailyMinTurnPart", {"period": 2, "rolling": 10}, "F_D_MinTurnPart_2_10.h5"],
     ["FactorDailyMinTurnPart", {"period": 3, "rolling": 10}, "F_D_MinTurnPart_3_10.h5"],
     ["FactorDailyMinTurnPart", {"period": 4, "rolling": 10}, "F_D_MinTurnPart_4_10.h5"],
     ["FactorDailyTurnPartPure", {"period": 0, "rolling": 10}, "F_D_TurnPartPure_0_10.h5"],
     ["FactorDailyTurnPartPure", {"period": 1, "rolling": 10}, "F_D_TurnPartPure_1_10.h5"],
     ["FactorDailyTurnPartPure", {"period": 2, "rolling": 10}, "F_D_TurnPartPure_2_10.h5"],
     ["FactorDailyTurnPartPure", {"period": 4, "rolling": 10}, "F_D_TurnPartPure_4_10.h5"],
     ["NonFactorDailyMinPVI", {"n": 10}, "NF_D_MinPVI.h5"],
     ["FactorDailyMinPVI", {"n": 10}, "F_D_MinPVI_10.h5"],

    ]

if platform.system() == "Windows":
    stock_list = Dtk.get_complete_stock_list()[0:50]
else:
    stock_list = Dtk.get_complete_stock_list()

start_date = 20161201
end_date = 20180630

if platform.system() == "Windows":
    alpha_factor_root_path = "D:\ApolloTestData"
elif os.system("nvidia-smi") == 0:
    alpha_factor_root_path = "/data/ApolloTestData"
else:
    user_id = os.environ['USER_ID']
    alpha_factor_root_path = "/app/data/" + user_id + "/ApolloTestData"

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
