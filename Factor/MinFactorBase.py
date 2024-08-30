"""
updated 2018/11/9 by 006566  新增功能，如一只股票计算出错，将停止
updated 2018/11/30 by 006566 新增功能，如连续600秒没有写入、表示进程卡住了，打印未计算完的股票列表
updated 2018/12/14 by 011673 新增功能，运行前检测CPU占用数量
update 2018/12/21 by 011673 修正功能，解决了检查cpu占用数量部分等待输入导致阻塞的问题
"""

import pandas as pd
from typing import List, Tuple
import multiprocessing
from abc import abstractmethod
import logging
import numpy as np
import platform
import DataAPI.DataToolkit as Dtk
from DataAPI.FactorLoader import get_info_of_factor
from datetime import datetime, timedelta
import os
import psutil
import threading, time


class TaskUnit:
    def __init__(self):
        self.task_id = -1
        self.codes: List[str] = []
        self.start_time = None
        self.end_time = None
        self.finish_calc_idx = -1  # 表示self.codes中已经计算完成的股票的序号


class MinFactorBase:
    def __init__(self, codes: List[str] = ..., start_date_int: int = ..., end_date_int: int = ...,
                 save_path: str = ..., name: str = ...):
        self.__codes = codes
        self.__start_date = start_date_int
        self.__end_date = end_date_int
        self.__save_path = save_path
        self.__name = name
        manager = multiprocessing.Manager()
        self.__queue: multiprocessing.Queue = manager.Queue()
        self.__is_calc_smooth = True

    def __create_tasks(self, cpu_num) -> List[Tuple[TaskUnit, ]]:
        tasks = []
        code_num_per_task = self.__codes.__len__() // cpu_num
        remains = self.__codes.__len__() % cpu_num
        start_index = 0
        for i in range(cpu_num):
            end_index = start_index + code_num_per_task
            if i < remains:
                end_index += 1
            task = TaskUnit()
            task.codes = self.__codes[start_index: end_index]
            task.start_time = self.__start_date
            task.end_time = self.__end_date
            task.task_id = i
            start_index = end_index
            tasks.append((task,))
        return tasks

    def task_routine(self, task_unit: TaskUnit = ...):
        print("start to run task {} {} ..., in total {} stocks".format(task_unit.task_id, str(task_unit.codes[0:3]),
                                                                       task_unit.codes.__len__()))
        for code in task_unit.codes:
            try:
                result = self.single_stock_factor_generator(code, task_unit.start_time, task_unit.end_time)
                self.__queue.put((code, result, "smooth", task_unit.task_id))
            except:
                print(code, "crashed")
                self.__queue.put((code, 0, "error", task_unit.task_id))
        print("task {} finished".format(task_unit.task_id))

    @abstractmethod
    def single_stock_factor_generator(self, code: str = ..., start: int = ..., end: int = ...):
        ############################################
        # 以下是因子计算逻辑的部分，需要用户自定义 #
        # 计算因子时，最后应得到factor_data这个对象，类型应当是DataFrame，涵盖的时间段是start至end（前闭后闭）；
        # factor_data的因子值一列，应当以股票代码为列名；
        # 最后factor_data的索引，建议与从get_single_stock_minute_data获得的原始行情的索引（index）一致，
        # 如通过reset_index撤销了原始行情的索引，那么不要删除'dt'或'minute'这两列，也不要设别的索引。
        ############################################
        start_date_minus_1 = Dtk.get_n_days_off(start, -2)[0]
        stock_minute_data = Dtk.get_single_stock_minute_data(code, start_date_minus_1, end, fill_nan=True,
                                                             append_pre_close=False, adj_type='FORWARD',
                                                             drop_nan=False, full_length_padding=True)
        if stock_minute_data.columns.__len__() > 0:  # 如可正常取到行情DataFrame
            stock_minute_data: pd.DataFrame = stock_minute_data.drop(['open', 'high', 'low', 'volume', 'amt'], axis=1)
            # 【因子计算过程】上溯60根BAR，比较，计算收益率
            stock_minute_data[code] = stock_minute_data['close'] / stock_minute_data['close'].shift(60) - 1
            # 将start_date至end_date期间的因子值提取出来
            factor_data = stock_minute_data.loc[start: end][code]
        ########################################
        # 因子计算逻辑到此为止，以下勿随意变更 #
        ########################################
        else:  # 如取到的行情的DataFrame为空，则自造一个全为nan的DataFrame
            date_list = Dtk.get_trading_day(start, end)
            complete_minute_list = Dtk.get_complete_minute_list()  # 每天242根完整的分钟Bar线的分钟值
            i_stock_minute_data_full_length = date_list.__len__() * 242
            index_tuple = [np.repeat(date_list, len(complete_minute_list)), complete_minute_list * len(date_list)]
            mi_index = pd.MultiIndex.from_tuples(list(zip(*index_tuple)), names=['dt', 'minute'])
            factor_data = pd.DataFrame(index=mi_index)  # 新建一个空的DataFrame, 且先设好了索引
            temp_array = np.empty(shape=[i_stock_minute_data_full_length, ])
            temp_array[:] = np.nan
            factor_data[code] = temp_array
        if factor_data.index.names[0] == 'dt':  # 将index变为普通的列，以便得到日期'dt'和分钟'minute'用于后续计算
            factor_data = factor_data.reset_index()
        # 拼接计算14位数的datetime, 格式例如20180802145500
        factor_data['datetime'] = factor_data['dt'] * 1000000 + factor_data['minute'] * 100
        # 将14位数的datetime转为dt.datetime
        date_time_dt = Dtk.convert_date_or_time_int_to_datetime(factor_data['datetime'].tolist())
        # 将dt.datetime转为timestamp
        timestamp_list = [i_date_time.timestamp() for i_date_time in date_time_dt]
        factor_data['timestamp'] = timestamp_list
        # 将timestamp设为索引
        factor_data = factor_data.set_index(['timestamp'])
        # DataFrame仅保留因子值一列，作为多进程任务的返回值
        factor_data = factor_data[[code]].copy()
        logging.info("finished calc {}".format(code))
        return factor_data

    def __write_buffer(self, tasks):
        save_name = "{}/{}_{}_{}.h5".format(self.__save_path, self.__name, self.__start_date, self.__end_date)
        pd.set_option('io.hdf.default_format', 'table')
        store = pd.HDFStore(save_name)
        count = self.__codes.__len__()
        store.put("stock_list", pd.DataFrame(self.__codes, columns=["code"]))
        i = 0
        while i < count:
            try:
                buffer = self.__queue.get(block=True, timeout=600)
            except:
                stocks_not_finished = []
                for task in tasks:
                    if task[0].finish_calc_idx != task[0].codes.__len__() - 1:
                        stocks_not_finished.extend(task[0].codes[task[0].finish_calc_idx + 1:])
                print("Stocks not finished:")
                print(stocks_not_finished)
                self.__is_calc_smooth = False
                break
            if buffer[2] == "error":
                print(buffer[0], "crashed and please check this stock. Calculation is stopping")
                self.__is_calc_smooth = False
                break
            code: str = buffer[0]
            data: pd.Series = buffer[1]
            store.put("factor/S{}".format(code.replace('.', '_')), data)
            tasks[buffer[3]][0].finish_calc_idx += 1
            i += 1
            logging.info("code:{} {}/{}".format(code, i, count))
            if i % 10 == 0:  # 每10只股票flush一次
                store.flush()
        store.flush()  # 把最后没存完的再flush一下
        logging.info("factor calculation is finished!!!!")
        store.close()

    def restore_factor(self) -> None:
        # 第一遍存储文件时，每只股票是在h5文件中单独存一个table，这样利于边算边存、但读取时因表太多、速度太慢；
        # 这里我们转存一次，将多表合并为一张表
        ori_file_name = "{}/{}_{}_{}.h5".format(self.__save_path, self.__name, self.__start_date, self.__end_date)
        if not os.path.isfile(ori_file_name):
            print("Factor does't exist")
            return
        file_name_fast = "{}/{}.h5".format(self.__save_path, self.__name)
        # if os.path.isfile(file_name_fast):
        #     # 文件已经存在
        #     return
        store_ori = pd.HDFStore(ori_file_name)
        store_new = pd.HDFStore(file_name_fast)
        stock_list = store_ori.select("stock_list")
        store_new.put(key="stock_list", value=stock_list)
        keys = list(map(lambda icode: "/factor/S{}".format(icode.replace('.', '_')), stock_list["code"]))
        index: pd.DataFrame = store_ori.select_column(keys[0], "index")
        left_rows = index.__len__()
        for chk in store_ori.select_as_multiple(keys, chunksize=1000):
            store_new.append("factor", chk, format="table")
            store_new.flush()
            left_rows = left_rows - chk.__len__()
            print("remains {} rows".format(left_rows))
        store_new.close()
        store_ori.close()
        # 转存完之后，将原来的文件删除
        os.remove(ori_file_name)
        # os.rename(file_name_fast, ori_file_name)

    def __increment_process(self) -> bool:
        file_name_fast = "{}/{}.h5".format(self.__save_path, self.__name)
        if not os.path.exists(file_name_fast):
            return True
        stored_stock_list, stored_index = get_info_of_factor(self.__name)
        if self.__codes != stored_stock_list:
            logging.critical("factor already exist, but their stock list are difference!!!\n"
                             "please remove {} or use another name to save".format(file_name_fast))
            return False

        if stored_index.__len__() == 0:
            return True
        stored_start_dt = datetime.fromtimestamp(stored_index.min())
        stored_end_dt = datetime.fromtimestamp(stored_index.max()) + timedelta(days=1)
        stored_end_dt = stored_end_dt.year * 10000 + stored_end_dt.month * 100 + stored_end_dt.day
        stored_start_dt = stored_start_dt.year * 10000 + stored_start_dt.month * 100 + stored_start_dt.day
        logging.info("the factor has data from ({} to {})".format(stored_start_dt, stored_end_dt))
        if self.__start_date < stored_start_dt:
            logging.critical("factor already exist, but the start date is later than current config\n"
                             "if you still want to calculate, please remove the old file!!")
            return False
        if self.__end_date < stored_end_dt:
            logging.critical("factor already exist, Nothing to be done!!")
            return False

        self.__start_date = stored_end_dt
        logging.info("calculating factor from [{} to {}]".format(self.__start_date, self.__end_date))
        return True

    @staticmethod
    def get_cpu_number():
        """
        检查cpu可用数量，如果cpu没有或者只有一个询问是否坚持用一个cpu执行程序
        如果还有多个cpu可用，输入需要执行的cpu数量（不会超过可用cpu数量）
        :return: 可用cpu数量
        """

        def get_input(temp: list):
            a = input()
            temp.append(a)
            return temp

        cpu_percent = psutil.cpu_percent(interval=1, percpu=True)
        available_cpu = 0
        for i in cpu_percent:
            if i < 50:
                available_cpu += 1
        if available_cpu <= 1:
            if available_cpu == 1:
                print(
                    'Only 1 core is available. If you insist to run, input y/Y')
            else:
                print('All cores are busy. If you insist to run, input y/Y')
            temp = []
            t = threading.Thread(target=get_input, args=(temp,))
            t.setDaemon(True)
            t.start()
            timer = 1
            while timer < 60:
                time.sleep(1)
                timer += 1
                if not t.is_alive():
                    break
            if t.is_alive():
                temp = 'n'
            else:
                temp = temp[0]
            if temp in ['y', 'Y']:
                cores = 1
            else:
                cores = 0
        else:
            print(
                '{} cores are available. Input the cores number you want to use.'
                'Input any non_number to terminate)'.format(available_cpu))
            temp = []
            t = threading.Thread(target=get_input, args=(temp,))
            t.setDaemon(True)
            t.start()
            timer = 1
            while timer < 60:
                time.sleep(1)
                timer += 1
                if not t.is_alive():
                    break
            if t.is_alive():
                temp = str(available_cpu - 1)
            else:
                temp = temp[0]
            if temp.isdigit():
                cores = min(int(temp), available_cpu)
            else:
                cores = 0
        return cores

    def launch(self):
        # CPU占用率大于50%之后需要手动确认是否执行程序
        logging.info("start")
        if not self.__increment_process():
            return
        cores = self.get_cpu_number()
        if cores == 0:
            print('process return')
            return
        if cores > 7:
            print('max cpu number is 7')
            cores = 7
        tasks = self.__create_tasks(cores)
        pool = multiprocessing.Pool(processes=cores)
        pool.starmap_async(self.task_routine, tasks)
        logging.info("start calculating factor with {} processes".format(cores))
        self.__write_buffer(tasks)
        if self.__is_calc_smooth:
            self.restore_factor()
        else:
            ori_file_name = "{}/{}_{}_{}.h5".format(self.__save_path, self.__name, self.__start_date, self.__end_date)
            if os.path.exists(ori_file_name):
                os.remove(ori_file_name)
        pool.close()


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    stock_code_list = Dtk.get_complete_stock_list()
    if platform.system() == 'Windows':
        save_dir = "S:\\Apollo\\Factors\\"  # 保存于S盘的地址
    else:
        save_dir = "/app/data/006566/Apollo/Factors"  # 保存于XQuant的地址
    ###############################################
    # 以下3行及factor_generator的类名需要自行改写 #
    ###############################################
    istart_date_int = 20150701
    iend_date_int = 20180630
    factor_name = 'Factor_Test'
    file_name = factor_name
    factor_generator = MinFactorBase(codes=stock_code_list, start_date_int=istart_date_int,
                                     end_date_int=iend_date_int, name=file_name, save_path=save_dir)
    factor_generator.launch()
    logging.info("program is stopped")
