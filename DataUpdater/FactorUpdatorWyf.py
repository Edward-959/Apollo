import DataAPI.DataToolkit as Dtk
from importlib import import_module
import os
import pandas as pd
import platform
import datetime as dt


# -----------------------------------------------------------------------------
# 需要更新的因子名写在下列列表中，内容依次是模块名、参数列表、因子文件名
# 若是因子，以Factor开头；若是非因子（即因子原始值），以NonFactor开头
# 这部分可以写成类似AlgoConfig的形式
factors_need_updated_list = \
    [
     ["NonFactorDailyCHNFamaFrench", {}, "NF_D_CHNFamaFrench.h5"],
     ["NonFactorDailyMinRet1Mean", {}, "NF_D_Ret1Mean.h5"],
     ["FactorDailyMinRet1Mean", {"ma": 8}, "F_D_MinRet1Mean_8.h5"],
     ["NonFactorDailyMinRet5Mean", {}, "NF_D_Ret5Mean.h5"],
     ["FactorDailyMinRet5Mean", {"ma": 8}, "F_D_MinRet5Mean_8.h5"],
     ["NonFactorDailyMinVolumeChg1Mean", {}, "NF_D_VolumeChg1Mean.h5"],
     ["FactorDailyMinVolumeChg1Mean", {"ma": 3}, "F_D_MinVolumeChg1Mean_3.h5"],
     ["NonFactorDailyMinVolumeCloseCorr", {}, "NF_D_VolumeCloseCorr.h5"],
     ["FactorDailyMinVolumeCloseCorr", {"ma": 6}, "F_D_MinVolumeCloseCorr_6.h5"],
     ["FactorDailyCREwm", {"span": 5}, "F_D_CREwm5.h5"],
     ["FactorDailyHighLowVol", {"ma": 5}, "F_D_HighLowVol5.h5"],
     ["FactorDailyMomentum_Adj", {"span": 5}, "F_D_Momentum_Adj5.h5"],
     ["FactorDailyMomentum_Adj_Rank", {"span": 5}, "F_D_Momentum_Adj_Rank_5.h5"],
     ["FactorDailyUclerEwm", {"span": 5}, "F_D_UclerEwm5.h5"],
     ["NonFactorDailyUpDownVolSub", {"n": 10}, "NF_D_UpDownVolSub_10.h5"],
     ["FactorDailyUpDownVolSub", {"emaspan": 10}, "F_D_UpDownVolSub_10.h5"],
     ["FactorDailyWMS", {"ma": 5}, "F_D_WMS5.h5"],
     ["FactorDailyWMS_Rank", {"ma": 5}, "F_D_WMS_Rank_5.h5"],
     ["FactorDailyUpDownVolSub_Rank", {"lag1": 10, "span": 5}, "F_D_UpDownVolSub_Rank_10.h5"],
     ["FactorDailyOpenPct_Rank", {"span": 12}, "F_D_OpenPct_Rank_12.h5"],
     ["FactorDailyIndayReturn_Rank", {"ma": 10}, "F_D_IndayReturn_Rank_10.h5"],
     ["FactorDailyTurn_Rank", {"ma": 5}, "F_D_Turn_Rank_5.h5"],
     ["FactorDailyCorrCloseTurn_Rank", {"ma": 10}, "F_D_CorrCloseTurn_Rank_10.h5"],
     ["FactorDailyMinSDRVOL", {"n": 10}, "F_D_MinSDRVOL10.h5"],
     ["FactorDailyMinSDVVOL", {"n": 10}, "F_D_MinSDVVOL10.h5"],
     ["NonFactorDailyMinUpAveVol", {"n": 5}, "NF_D_UpAveVol.h5"],
     ["FactorDailyMinUpAveVol", {"span": 5}, "F_D_MinUpAveVol_5.h5"],
     ["NonFactorDailyMinUpPreCloseVol", {}, "NF_D_UpPreCloseVol.h5"],
     ["FactorDailyMinUpPreCloseVol", {"span": 5}, "F_D_MinUpPreCloseVol5.h5"],
     ["NonFactorDailyMinRetCorr", {}, "NF_D_MinRetCorr.h5"],
     ["FactorDailyMinRetCorr", {"ema_span": 8}, "F_D_MinRetCorr_8.h5"],
     ["NonFactorDailyMinVolCorr", {}, "NF_D_MinVolCorr.h5"],
     ["FactorDailyMinVolCorr", {"ewmspan": 5}, "F_D_MinVolCorr_5.h5"],
     ["NonFactorDailyMinCloseToAvgPrice", {}, "NF_D_CloseToAvgPrice.h5"],
     ["FactorDailyMinCloseToAvgPrice", {"span": 5}, "F_D_MinCloseToAvgPrice_5.h5"],
     ["FactorDailyVwapRatio", {"n": 10}, "F_D_VwapRatio_10.h5"],
     ["FactorDailyFFAlpha", {"index_code": '000905.SH', "n": 25}, "F_D_FFAlpha_000905_25.h5"],
     ["FactorDailyFFBetaMarket", {"index_code": '000905.SH', "n": 25}, "F_D_FFBetaMarket_000905_25.h5"],
     ["FactorDailyFFAlphaStd", {"index_code": '000905.SH', "n": 25}, "F_D_FFAlphaStd_000905_25.h5"],
     ["FactorDailyFFBetaMarketStd", {"index_code": '000905.SH', "n": 25}, "F_D_FFBetaMarketStd_000905_25.h5"],
     ["FactorDailyFFRSquareStd", {"index_code": '000300.SH', "n": 20}, "F_D_FFRSquareStd_000300_20.h5"],
     ["NonFactorDailyCloseMeanToMedian", {"n": 10}, "NF_D_CloseMeanToMedian.h5"],
     ["FactorDailyCloseMeanToMedian", {"span":5}, "F_D_CloseMeanToMedian_5.h5"],
     ["FactorDailyRetRankStd", {"n":10}, "F_D_RetRankStd_10.h5"],
     ["FactorDailyResMaRegressionStd", {"Lag_list":[3,5,10,20,50,100,200], "n":10}, "F_D_ResMaRegressionStd.h5"],
     ["FactorDailyVolRegIndexRsquare", {"n":20, "index_code":'000905.SH'}, "F_D_VolRegIndexRsquare_20.h5"],
     ["NonFactorDailyStockStdBetaIndex", {"n":25, "index_code":'000905.SH'}, "NF_D_StockStdBetaIndex_25.h5"],
     ["FactorDailyStockStdBetaIndex", {"n":25}, "F_D_StockStdBetaIndex_25.h5"],
     ["NonFactorDailyVolRegIndexResStd", {"n":20, "index_code":'000905.SH'}, "NF_D_VolRegIndexResStd.h5"],
     ["FactorDailyVolRegIndexResStd", {"n":20}, "F_D_VolRegIndexResStd.h5"],
     ["NonFactorDailyVolRegMktMeanResStd", {"n":20}, "NF_D_VolRegMktMeanResStd.h5"],
     ["FactorDailyVolRegMktMeanResStd", {"n":20}, "F_D_VolRegMktMeanResStd_20.h5" ],
     ["FactorDailyLiqND", {"n":5}, "F_D_LiqND_5.h5"],
     ["FactorDailyROEOPP", {}, "F_D_ROEOPP.h5"],
     ["FactorDailyROAOPP", {}, "F_D_ROAOPP.h5"],
     ["FactorDailyGTJA120", {"n":3}, "F_D_GTJA120.h5"],
     ["FactorDailySDAlpha001", {"n":3}, "F_D_SDAlpha001.h5"],
     ["FactorDailyGTJA54", {}, "F_D_GTJA54.h5"],
     ["FactorDailyWQ016", {"n":5}, "F_D_WQ016.h5"]
    ]

if platform.system() == "Windows":
    stock_list = Dtk.get_complete_stock_list()
else:
    stock_list = Dtk.get_complete_stock_list()

start_date = 20131008
end_date = 20190222

if platform.system() == "Windows":
    alpha_factor_root_path = "D:\\NewFactorData"
elif os.system("nvidia-smi") == 0:
    alpha_factor_root_path = "/data/NewFactorData"
else:
    user_id = os.environ['USER_ID']
    alpha_factor_root_path = "/app/data/" + user_id + "/Apollo/AlphaFactors"

# ------------需要设定的部分到此为止-----------------------------------------

factor_path_dir = os.path.join(alpha_factor_root_path, "AlphaFactors")
nonfactor_path_dir = os.path.join(alpha_factor_root_path, "AlphaNonFactors")

if not os.path.exists(alpha_factor_root_path):
    os.mkdir(alpha_factor_root_path)
if not os.path.exists(factor_path_dir):
    os.mkdir(factor_path_dir)
if not os.path.exists(nonfactor_path_dir):
    os.mkdir(nonfactor_path_dir)


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
