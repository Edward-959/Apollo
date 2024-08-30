# -*- coding: utf-8 -*-
"""
created on 2019/02/13
@author: 006566
"""
from abc import abstractmethod
import pandas as pd
import DataAPI.DataToolkit as Dtk
import numpy as np
import multiprocessing
from typing import List, Tuple


class TaskUnit:
    def __init__(self):
        self.task_id = -1
        self.codes: List[str] = []
        self.start_time = None
        self.end_time = None
        self.finish_calc_idx = -1  # 表示self.codes中已经计算完成的股票的序号


class DailyMinFactorBase:
    def __init__(self, alpha_factor_root_path, stock_list, start_date, end_date):
        self.alpha_factor_root_path = alpha_factor_root_path
        self.stock_list = stock_list
        self.start_date = start_date
        self.end_date = end_date
        manager = multiprocessing.Manager()
        self.__queue = manager.Queue()
        self.trading_day_list = Dtk.get_trading_day(self.start_date, self.end_date)
        self.result = pd.DataFrame(index=self.trading_day_list)

    @abstractmethod
    def single_stock_factor_generator(self, code):
        # 根据单只股票的数据来计算因子
        pass

    def factor_calc(self, multi_process=False):
        trading_day_list = Dtk.get_trading_day(self.start_date, self.end_date)
        if not multi_process:
            ans_df = pd.DataFrame(index=trading_day_list)
            # 将单只股票的因子聚合，拼起来得到完整的因子列表
            for j, stock_code in enumerate(self.stock_list):
                if j % 50 == 0:
                    print("{}/{} stocks calculated.".format(j, self.stock_list.__len__()))
                single_stock_factor_value = self.single_stock_factor_generator(stock_code)
                single_stock_factor_value = single_stock_factor_value.reindex(trading_day_list)
                ans_df = pd.concat([ans_df, single_stock_factor_value], axis=1)
        else:
            # 多进程计算——适合一次性计算一两个因子；若要计算3个以上因子，因多进程无法共享内存、所以每次计算因子都要从
            # 硬盘重新获取数据、多进程速度不如单进程快
            cores = multiprocessing.cpu_count()
            cores -= 1
            cores = min(cores, 7)
            tasks = self.__create_tasks(cores)
            pool = multiprocessing.Pool(processes=cores)
            pool.starmap_async(self.task_routine, tasks)
            self.save_buffer(tasks)
            ans_df = self.result
        ans_df = Dtk.convert_df_index_type(ans_df, 'date_int', 'timestamp')
        return ans_df

    def __create_tasks(self, cpu_num) -> List[Tuple[TaskUnit, ]]:
        tasks = []
        code_num_per_task = self.stock_list.__len__() // cpu_num
        remains = self.stock_list.__len__() % cpu_num
        start_index = 0
        for i in range(cpu_num):
            end_index = start_index + code_num_per_task
            if i < remains:
                end_index += 1
            task = TaskUnit()
            task.codes = self.stock_list[start_index: end_index]
            task.start_time = self.start_date
            task.end_time = self.end_date
            task.task_id = i
            start_index = end_index
            tasks.append((task,))
        return tasks

    def task_routine(self, task_unit: TaskUnit = ...):
        print("start to run task {} {} ..., in total {} stocks".format(task_unit.task_id, str(task_unit.codes[0:3]),
                                                                       task_unit.codes.__len__()))
        for code in task_unit.codes:
            try:
                result = self.single_stock_factor_generator(code)
                self.__queue.put((code, result, "smooth", task_unit.task_id))
            except:
                print(code, "crashed")
                self.__queue.put((code, 0, "error", task_unit.task_id))
        print("task {} finished".format(task_unit.task_id))

    def save_buffer(self, tasks):
        i = 0
        count = self.stock_list.__len__()
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
                break
            if buffer[2] == "error":
                print(buffer[0], "crashed and please check this stock. Calculation is stopping")
                break
            data = buffer[1]
            self.result = pd.concat([self.result, data], axis=1)
            i += 1

    def generate_empty_df(self, code):
        date_list = Dtk.get_trading_day(self.start_date, self.end_date)
        factor_data = pd.DataFrame(index=date_list)  # 新建一个空的DataFrame, 且先设好了索引
        temp_array = np.empty(shape=[date_list.__len__(), ])
        temp_array[:] = np.nan
        factor_data[code] = temp_array
        return factor_data
