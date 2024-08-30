"""
@author:013542
每日更新GPW类因子
"""
import pickle
import DataAPI.DataToolkit as Dtk
import datetime as dt
import Utils.HelperFunctions as UH
from DataAPI.FactorTestloader import *
import time

with open('/app/data/013542/Apollo/GPW/feature_info/feature.pkl', 'rb') as k:
    feature_info = pickle.load(k)
with open('/app/data/013542/Apollo/GPW/feature_info/idx.pkl', 'rb') as g:
    idx_info = pickle.load(g)
stock_list = Dtk.get_complete_stock_list()

def load_factor_stack(factor_list, start_date, end_date):
    path = "/app/data/666889/Apollo/AlphaFactors/AlphaFactors"
    complete_stock_list = Dtk.get_complete_stock_list()
    start_date_datetime = Dtk.convert_date_or_time_int_to_datetime(start_date)
    end_date_datetime = Dtk.convert_date_or_time_int_to_datetime(end_date)
    day_factor_df_alltime = pd.DataFrame()
    for day_factor_name in factor_list:
        day_factor_name = day_factor_name.split("'")[1]
        temp_factor = load_factor(day_factor_name, complete_stock_list, start_date_datetime, end_date_datetime, path)
        volume_df = Dtk.get_panel_daily_pv_df(complete_stock_list, start_date, end_date, "volume")
        volume_df = Dtk.convert_df_index_type(volume_df, 'date_int', 'timestamp')
        temp_factor = temp_factor * volume_df / volume_df  # 将停牌股票的因子值置为nan
        temp_factor = UH.outlier_filter(temp_factor)  # 因子去除极值
        temp_factor = UH.z_score_standardizer(temp_factor)  # 因子标准化
        temp_factor = Dtk.convert_df_index_type(temp_factor, 'timestamp', 'date_int')
        day_factor_df_alltime[day_factor_name] = temp_factor.loc[start_date: end_date, :].stack()
    day_factor_df_alltime.fillna(0, inplace=True)
    return day_factor_df_alltime

def updatefactors(keys, start_date, end_date):
    feature_input = feature_info[keys]
    with open('/app/data/013542/Apollo/GPW/model/' + keys + '.pkl', 'rb') as f:
        gp = pickle.load(f)
    X = load_factor_stack(feature_input, start_date, end_date)
    y_transform = gp.transform(X)
    transform_factor_df_stack = pd.Series(y_transform[:, int(idx_info[keys])], index=X.index)
    transform_factor_df = transform_factor_df_stack.unstack()
    transform_factor_df = Dtk.convert_df_index_type(transform_factor_df, 'date_int', 'timestamp')

    out_put = '/app/data/666889/Apollo/AlphaFactors/AlphaFactors/'
    file_name = 'F_D_' + keys + '.h5'
    output_file_path = os.path.join(out_put, file_name)
    if not os.path.exists(output_file_path):
        pd.set_option('io.hdf.default_format', 'table')
        store = pd.HDFStore(output_file_path)
        store.put("stock_list", pd.DataFrame(stock_list, columns=['code']))
        store.put("factor", transform_factor_df, format="table")
        store.flush()
        store.close()
        print("Factor file", file_name, "was created.")
    # 如已有因子文件，则更新之；如遇日期重叠的部分，以新计算的为准
    else:
        store = pd.HDFStore(output_file_path)
        original_data_df = store.select("/factor")
        if original_data_df.index[-1] < transform_factor_df.index[0]:
            ans_df2 = pd.concat([original_data_df, transform_factor_df])
        else:
            ans_df2 = pd.concat([original_data_df.loc[:transform_factor_df.index[0] - 1], transform_factor_df])
        new_stock_list = list(ans_df2.columns)
        if new_stock_list.__len__() > list(original_data_df.columns).__len__():
            store.put("stock_list", pd.DataFrame(new_stock_list, columns=['code']))
        store.put("factor", ans_df2, format="table")
        store.flush()
        store.close()
        print("Factor_file", file_name, "was updated to", end_date, ".")

def main():
    t1 = dt.datetime.now()
    today_int = int(time.strftime("%Y%m%d"))
    day_before_yesterday = Dtk.get_n_days_off(today_int, -3)[0]
    yesterday = Dtk.get_n_days_off(today_int, -3)[1]
    start_date = day_before_yesterday
    end_date = yesterday
    # 需要更新的因子
    all_factor_list = os.listdir('/app/data/666889/Apollo/AlphaFactors/AlphaFactors/')
    GPW_list = []
    for iname in all_factor_list:
        if iname[0:7] == 'F_D_GPW':
            iname = iname.split('.h5')[0].split('F_D_')[1]
            GPW_list.append(iname)
    for i, key in enumerate(GPW_list):
        print('update' + str(i))
        updatefactors(key, start_date, end_date)
    t2 = dt.datetime.now()
    print('it costs', t2 - t1, 'to update')

if __name__ == "__main__":
    main()



