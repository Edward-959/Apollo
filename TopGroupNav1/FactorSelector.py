import pandas as pd
import os


corr_threshold = 0.9

dir_path = r"D:\xzx"
ret_alpha_file = "TopGroupDayRet_alpha_universe.csv"

ret_df = pd.read_csv(os.path.join(dir_path, ret_alpha_file), index_col=0)
ret_corr_df = ret_df.corr()
cumsum_ret_df = ret_df.cumsum()
final_ret = cumsum_ret_df.iloc[-1]
final_ret_sorted = final_ret.sort_values(ascending=False)  # 累计收益降序排列

selected_factors = []
for i, i_factor in enumerate(final_ret_sorted.index):
    if i == 0:  # 收益率最高的直接入选
        selected_factors.append(i_factor)
        continue
    else:
        corr_not_greater_than_threshold = True
        for j_selected_factor in selected_factors:  # 和已入库的因子逐个比相关性
            if ret_corr_df.loc[i_factor, j_selected_factor] > corr_threshold:
                corr_not_greater_than_threshold = False
                print(i_factor, j_selected_factor, ret_corr_df.loc[i_factor, j_selected_factor])
                break
        if corr_not_greater_than_threshold:
            selected_factors.append(i_factor)

new_corr_df = ret_corr_df.reindex(index=selected_factors, columns=selected_factors)
output_file_name = "alpha_corr_" + str(ret_alpha_file[15:])  # 输出文件的名字
new_corr_df.to_csv(os.path.join(dir_path, output_file_name))
