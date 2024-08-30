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
     ["FactorDailyBP", {}, "F_D_BP.h5"],
     ["FactorDailyCatoassets", {}, "F_D_Catoassets.h5"],
     # 这个因子于2019/3/12因xquant的数据与api不同而删掉 ["FactorDailyDivyield", {}, "F_D_Divyield.h5"],
     ["FactorDailyEP", {}, "F_D_EP.h5"],
     ["FactorDailyGrossprofitmargin_q", {}, "F_D_Grossprofitmargin_q.h5"],
     ["FactorDailyProfitGq", {}, "F_D_ProfitGq.h5"],
     ["FactorDailyQfaROE", {}, "F_D_QfaROE.h5"],
     ["FactorDailySalesGq", {}, "F_D_SalesGq.h5"],
     ["FactorDailySP", {}, "F_D_SP.h5"],
     ["FactorDailyNetprofitmargin_q", {}, "F_D_Netprofitmargin_q.h5"],
     ["FactorDailyEPTTM2", {}, "F_D_EPTTM2.h5"],
     ["FactorDailyOCFP", {}, "F_D_OCFP.h5"],
     ["FactorDailyEarningsTTM", {}, "F_D_EarningsTTM.h5"]
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
            factor_module = import_module("Factor.FactorFundamental." + i_factor[0])
            output_file_path = os.path.join(factor_path_dir, file_name)
            valid_start_date = start_date
        else:
            factor_module = import_module("NonFactor.FactorFundamental." + i_factor[0])
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