import DataAPI.DataToolkit as Dtk
from importlib import import_module
import os
import pandas as pd
import platform
import numpy as np


# -----------------------------------------------------------------------------
# 需要更新的因子名写在下列列表中，内容依次是模块名、参数列表、因子文件名
# 若是因子，以Factor开头；若是非因子（即因子原始值），以NonFactor开头
# 这部分可以写成类似AlgoConfig的形式
factors_need_updated_list = [
                             ["FactorBarraSize", {}, "F_B_Size.h5"],
                             ["FactorBarraBeta", {}, "F_B_Beta.h5"],
                             ["FactorBarraMomentum", {}, "F_B_Momentum.h5"],
                             ["FactorBarraResidualVolatility", {}, "F_B_ResidualVolatility.h5"],
                             ["FactorBarraNonLinearSize", {}, "F_B_NonLinearSize.h5"],
                             ["FactorBarraLiquidity", {}, "F_B_Liquidity.h5"],
                             ["FactorBarraEarningsYield", {}, "F_B_EarningsYield.h5"],
                             ["FactorBarraGrowth", {}, "F_B_Growth.h5"],
                             ["FactorBarraLeverage", {}, "F_B_Leverage.h5"],
                             ["FactorBarraValue", {}, "F_B_Value.h5"],
                             ]

# 涉及到横截面上的因子回归，股票更新列表务必为全市场
stock_list = Dtk.get_complete_stock_list()
# stock_list.append("603713.SH")

start_date = 20090701
end_date = 20180630

if platform.system() == "Windows":
    alpha_factor_root_path = "D:\ApolloTestData"
elif os.system("nvidia-smi") == 0:
    alpha_factor_root_path = "/vipzrz/Apollo"
else:
    alpha_factor_root_path = "/app/data/666889/Apollo"

# ------------需要设定的部分到此为止-----------------------------------------

barra_path_dir = os.path.join(alpha_factor_root_path, "BarraFactors")

if not os.path.exists(barra_path_dir):
    os.mkdir(barra_path_dir)


def check_future_data(ans_date: pd.DataFrame):
    last_series = ans_date.iloc[-1]
    if np.isnan(last_series).all():
        print('WARNING!!! ALL DATA IN LAST DAY IS NAN. IT MAY INCLUDE FUTURE DATA')
    return


# 将列表中待更新的因子逐个更新
for i_factor in factors_need_updated_list:
    file_name = i_factor[2]
    factor_module = import_module("Factor.FactorBarra." + i_factor[0])
    output_file_path = os.path.join(barra_path_dir, file_name)
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
