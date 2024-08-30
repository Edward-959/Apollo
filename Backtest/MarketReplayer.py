from .MDLocalReader import read_single_month_file, read_single_day_file, read_single_day_pickle_file,\
    read_single_day_file_transaction
from pandas import DataFrame, HDFStore
from .common.MarketData import Quote, Transaction, KBar
from .utils.timetool import get_month_list
from datetime import datetime, timedelta
from time import sleep
import typing
import queue
import threading
from .utils import Const
from DataAPI.DataToolkit import get_single_stock_minute_data
import uuid
import logging

__all__ = ['MDReplay',  'read_single_month_file']

Const.OPEN_TIME = 93000000
Const.MORNING_CLOSE = 113000000
Const.AFTERNOON_OPEN = 130000000
Const.SH_CLOSE = 145957000
Const.SZ_CLOSE = 145657000


class MDReplay:
    def __init__(self, platform: str='local'):
        self._buff_size = 30
        self.__callbacks = []
        self.event = threading.Event()
        self._buff = queue.Queue(3)
        # self._buff_transaction = queue.Queue(3 * ((self.buff_size - 1) // 30 + 1))
        self.__callbacks_transaction = []
        self.__callbacks_kbar = []
        self.__callbacks_finish = []
        self.finished = False
        self.lock = threading.Lock()
        self.__data_root = ''
        self.__platform = platform
        self.__start_timestamp = None
        self.__end_timestamp = None
        self.__start_datetime: datetime = None
        self.__end_datetime: datetime = None
        self.__start_time = None
        self.__end_time = None
        self.__stock_list = []
        self._mode = 1
        self.__data_type = 1
        self.is_br_data = False
        self.__play_transaction = False
        self.__play_next_block: typing.Callable = None
        self.__callbacks_new_day = []
        self.__data_read_api = [
            self.__read_data_type0,
            self.__read_data_type1,
            self.__read_data_type2,
            self.__read_data_type3,
            self.__read_data_type4
        ]
        self.__pre_day = 0
        self.__need_date_filter = False

    def set_data_source_root(self, path) ->None:
        self.data_root = path

    @property
    def data_root(self) ->str:
        return self.__data_root

    @data_root.setter
    def data_root(self, path: str =...) ->None:
        self.__data_root = path

    @property
    def play_transaction(self) ->bool:
        return self.__play_transaction

    @play_transaction.setter
    def play_transaction(self, flag: bool=...) ->None:
        self.__play_transaction = flag

    @property
    def buff_size(self) ->int:
        return self._buff_size

    @buff_size.setter
    def buff_size(self, days) ->None:
        """
        initialize buffer size that caches market days, don't call this function after starting replay
        :param days: buffer size holds how many days Market data
        :return:
        """
        self._buff_size = days
        self._buff.maxsize = 3
        # self._buff_transaction.maxsize = 3 * ((self.buff_size - 1) // 30 + 1)

    def set_config(self, stock_list: []=..., start_time: datetime=...,
                   end_time: datetime=..., mode: int=1, data_type: int = 1, need_date_filter: bool = False) ->None:
        self.__stock_list = stock_list
        self.__start_datetime = start_time
        self.__end_datetime = end_time
        self.__start_time = start_time.hour * 10000000 + start_time.minute * 100000 + start_time.second * 1000
        self.__end_time = end_time.hour * 10000000 + end_time.minute * 100000 + end_time.second * 1000
        self._mode = mode
        self.__data_type = data_type
        self.__need_date_filter = need_date_filter
        if data_type in [3, 4]:
            self.__play_next_block = self.__play_next_minute_block
        else:
            self.__play_next_block = self.__play_next_tick_block

    def run(self):
        """
        a sync version api for replaying market data, which will read the whole data into memory
        :return:
        """
        if self.__data_type == 3:
            block = None
            start = int(self.__start_datetime.strftime("%Y%m%d"))
            end = int(self.__end_datetime.strftime("%Y%m%d"))
            for code in self.__stock_list:
                cell = get_single_stock_minute_data(code, start, end, fill_nan=False,
                                                    append_pre_close=False, adj_type='NONE', drop_nan=True,
                                                    full_length_padding=False)
                if cell.__len__() == 0:
                    continue
                cell["symbol"] = code
                cell = cell.reset_index()
                if block is not None:
                    block = block.append(cell)
                else:
                    block = cell
            block["dt"] = block["dt"].astype('int')
            block["minute"] = block["minute"].astype('int')
            block = block.sort_values(by=["dt", "minute"])
            self._buff.put({'bar': block, 'transaction': None})
            self.__play_next_minute_block()
        elif self.__data_type == 4:
            start = int(self.__start_datetime.strftime("%Y%m%d"))
            end = int(self.__end_datetime.strftime("%Y%m%d"))
            file_name = "{}/{}.h5".format(self.__data_root.rstrip("/"), self.__stock_list)
            t1 = datetime.now()
            store = HDFStore(file_name, mode='r')
            "Reading minute data"
            if self.__need_date_filter:
                block = store.select("data", where="dt >= start & dt <= end")
            else:
                start_year, _ = divmod(start, 10000)
                end_year, _ = divmod(end, 10000)
                block = DataFrame()
                if start_year < end_year:
                    while start_year <= end_year:
                        temp_block = store.select("/data/y{}".format(start_year))
                        block = block.append(temp_block)
                        start_year += 1
                else:
                    block = store.select("/data/y{}".format(start_year))
            self._buff.put({'bar': block, 'transaction': None})
            store.close()
            print("Read Data costs time: ", datetime.now() - t1)
            self.__play_next_minute_block()
        else:
            print("error: MDReplay not support data type : {} in run(), try with async_run()".format(self.__data_type))
        pass

    def async_run(self) ->None:
        """
        async run function is used when program is running on a standalone pc which is leak of enough memory.
        it reads data from disk into memory cached while playing back market data, the used data will be dropped to
        release memory.
        if cache is not empty, then keep playing back market data, else cache is empty and finished flag isn't set
        then waiting for data prefetch from file. if cache is empty and finished flag is set, then that means
        all the market data are completely played, just need to return
        """
        th = threading.Thread(target=self.__start_reading_process)
        th.start()
        is_stop = False
        while not (is_stop & self._buff.empty()):
            if not self._buff.empty():
                self.__play_next_block()
            #####################
            self.lock.acquire()
            is_stop = self.finished
            self.lock.release()
            ####################
            sleep(1)

        self.__finished()
        th.join()

    def __get_cache(self) ->dict:
        if not self._buff.empty():
            if self._buff.qsize() == 1:  # wake up reading thread if the buff is almost empty
                self.event.set()
            return self._buff.get()

    def __finished(self):
        for func in self.__callbacks_finish:
            func()

    def __quote_updated(self, quote) ->None:
        for func in self.__callbacks:
            func(quote)

    def __transaction_updated(self, transaction) ->None:
        for func in self.__callbacks_transaction:
            func(transaction)

    def __kbar_updated(self, bar: KBar) ->None:
        for func in self.__callbacks_kbar:
            func(bar)

    def __inform_new_day(self, day):
        for func in self.__callbacks_new_day:
            func(day)

    def __play_next_tick_block(self) ->None:
        cache = self.__get_cache()
        block = cache['quote']
        idx = 0
        len_transaction = 0
        block_transaction = 0
        if self.play_transaction:
            block_transaction = cache['transaction']
            len_transaction = len(block_transaction) if block_transaction is not None else 0
            idx = 0
        for index, row in block.iterrows():
            bids = [row['BidP1'], row['BidP2'], row['BidP3'], row['BidP4'], row['BidP5'],
                    row['BidP6'], row['BidP7'], row['BidP8'], row['BidP9'], row['BidP10']]
            asks = [row['AskP1'], row['AskP2'], row['AskP3'], row['AskP4'], row['AskP5'],
                    row['AskP6'], row['AskP7'], row['AskP8'], row['AskP9'], row['AskP10']]
            bid_vol = [row['BidV1'], row['BidV2'], row['BidV3'], row['BidV4'], row['BidV5'],
                       row['BidV6'], row['BidV7'], row['BidV8'], row['BidV9'], row['BidV10']]
            ask_vol = [row['AskV1'], row['AskV2'], row['AskV3'], row['AskV4'], row['AskV5'],
                       row['AskV6'], row['AskV7'], row['AskV8'], row['AskV9'], row['AskV10']]
            last_price = row['Price']
            volume = row['Volume']
            amount = row['Turover']
            total_volume = row['AccVolume']
            total_amt = row['AccTurover']
            pre_close = row['PreClose']
            time_stamp = row['TimeStamp']
            time = row['Time']
            if self.play_transaction:
                while idx < len_transaction:
                    r = block_transaction.iloc[idx]
                    if r['TimeStamp'] >= time_stamp:
                        break
                    trans = Transaction(r['Code'], r['Time'], r['Price'], r['Volume'], r['BsFlag'], r['TimeStamp'])
                    idx += 1
                    self.__transaction_updated(trans)

            quote = Quote(row['Code'], time_stamp, time, bids, asks, bid_vol, ask_vol, last_price,
                          volume, amount, total_volume, total_amt, pre_close)
            self.__quote_updated(quote)

    def __play_next_minute_block(self)->None:
        cache = self.__get_cache()
        block = cache["bar"]
        # for index, row in block.iterrows():
        #     bar = KBar(symbol=row['symbol'], open=row["open"], close=row["close"], volume=row['volume'],
        #                amount=row['amt'], low=row["low"], high=row["high"], time=row["minute"],
        #                pre_close=0, date=row['dt'])
        #     if self.__pre_day == int(row['dt']):
        #         pass
        #     else:
        #         self.__inform_new_day(row['dt'])
        #         self.__pre_day = int(row['dt'])
        for row in block.values:
            bar = KBar(symbol=row[8], open=row[2], close=row[5], volume=row[6],
                       amount=row[7], low=row[4], high=row[3], time=row[1],
                       pre_close=0, date=row[0])
            if self.__pre_day == int(row[0]):
                pass
            else:
                self.__inform_new_day(row[0])
                self.__pre_day = int(row[0])

            self.__kbar_updated(bar)

    def subscribe_callback_finished(self, func):
        """
        subscribe the callback function
        :param func: callback function will be called when all the market data replaying is finished
        :return:
        """
        self.__callbacks_finish.append(func)

    def subscribe_callback(self, func: typing.Callable[[Quote], None]) ->None:
        """
        subscribe the callback function
        :param func: callback function will be called when a new tick played
        :return:
        """
        self.__callbacks.append(func)

    def subscribe_callback_bar(self, func: typing.Callable[[KBar], None])->None:
        """
        subscribe the callback function for replaying bar market data
        :param func:
        :return:
        """
        self.__callbacks_kbar.append(func)

    def subscribe_callback_transaction(self, func: typing.Callable[[Transaction], None]) ->None:
        """
        subscribe the callback function
        :param func: callback function will be called when a new tick played
        :return:
        """
        self.__callbacks_transaction.append(func)

    def subscribe_callback_new_day(self, func: typing.Callable[[int], None])->None:
        self.__callbacks_new_day.append(func)

    def __read_data_type0(self):
        """
        read tick data from Local disk. eg: S:\
        :return: None
        """
        year_month_list = get_month_list(self.__start_datetime, self.__end_datetime)
        for month_id in year_month_list:
            block = None
            # data_frame.append(read_single_month_file(srcDir, stockCode, month_id))
            for code in self.__stock_list:
                if block is None:
                    block = read_single_month_file(self.__data_root, code, month_id)
                else:
                    block = block.append(read_single_month_file(self.__data_root, code, month_id), ignore_index=True)
            block = self.__data_filter(self.__stock_list[0], block)
            block = block.sort_values(by='TimeStamp', axis=0)
            block_transaction = None
            if self.play_transaction:
                raise Exception("No implemented")
            if self._buff.full():
                self.event.wait()
            self._buff.put({'quote': block, 'transaction': block_transaction})

    def __read_data_type1(self):
        tick_date = self.__start_datetime.date()
        while tick_date <= self.__end_datetime.date():
            block = None
            for code in self.__stock_list:
                if tick_date.weekday() < 5:
                    if block is None:
                        block = read_single_day_file(self.__data_root, code,
                                                     tick_date.strftime("%Y%m%d"), self.is_br_data)
                    else:
                        block = block.append(read_single_day_file(self.__data_root, code,
                                                                  tick_date.strftime("%Y%m%d"),
                                                                  self.is_br_data), ignore_index=True)
            if block is not None:
                block = self.__data_filter(self.__stock_list[0], block)
                block = block.sort_values(by='TimeStamp', axis=0)
                block_transaction = None
                if self.play_transaction:
                    for code in self.__stock_list:
                        if block_transaction is None:
                            block_transaction = read_single_day_file_transaction(self.__data_root, code,
                                                                                 tick_date.strftime("%Y%m%d"))
                        else:
                            block_transaction = block_transaction.append(
                                read_single_day_file_transaction(self.__data_root, code,
                                                                 tick_date.strftime("%Y%m%d")), ignore_index=True)
                    block_transaction = self.__data_filter(self.__stock_list[0], block_transaction)
                    block_transaction = block_transaction.sort_values(by='TimeStamp', axis=0)
                if self._buff.full():
                    self.event.wait()
                self._buff.put({'quote': block, 'transaction': block_transaction})
            tick_date += timedelta(days=1)

    def __read_data_type2(self):
        tick_date = self.__start_datetime.date()
        while tick_date <= self.__end_datetime.date():
            block = None
            for code in self.__stock_list:
                if tick_date.weekday() < 5:
                    if block is None:
                        block = read_single_day_pickle_file(self.__data_root, code, tick_date.strftime("%Y%m%d"))
                    else:
                        block = block.append(
                            read_single_day_file(self.__data_root, code, tick_date.strftime("%Y%m%d"),
                                                 self.is_br_data), ignore_index=True)
            if block is not None:
                block = self.__data_filter(self.__stock_list[0], block)
                block = block.sort_values(by='TimeStamp', axis=0)
                block_transaction = None
                if self.play_transaction:
                    for code in self.__stock_list:
                        if block_transaction is None:
                            block_transaction = read_single_day_file_transaction(self.__data_root, code,
                                                                                 tick_date.strftime("%Y%m%d"))
                        else:
                            block_transaction = block_transaction.append(
                                read_single_day_file_transaction(self.__data_root, code,
                                                                 tick_date.strftime("%Y%m%d")), ignore_index=True)
                    block_transaction = self.__data_filter(self.__stock_list[0], block_transaction)
                    block_transaction = block_transaction.sort_values(by='TimeStamp', axis=0)
                if self._buff.full():
                    self.event.wait()
                self._buff.put({'quote': block, 'transaction': block_transaction})
            tick_date += timedelta(days=1)

    def __read_data_type3(self):
        """
        read 1min bar data from local disk
        :return:
        """
        start = self.__start_datetime
        while start < self.__end_datetime:
            start_date = start.year * 10000 + start.month * 100 + start.day
            end = start + timedelta(days=self._buff_size)
            end = min(end, self.__end_datetime)
            end_date = end.year * 10000 + end.month * 100 + end.day
            block: DataFrame = None
            for code in self.__stock_list:
                cell = get_single_stock_minute_data(code, start_date, end_date, fill_nan=False,
                                                    append_pre_close=False, adj_type='NONE', drop_nan=True,
                                                    full_length_padding=False)
                if cell.__len__() == 0:
                    continue
                cell["symbol"] = code
                cell = cell.reset_index()
                if block is not None:
                    block = block.append(cell)
                else:
                    block = cell
            block["dt"] = block["dt"].astype('int')
            block["minute"] = block["minute"].astype('int')
            block = block.sort_values(by=["dt", "minute"])
            start = end + timedelta(days=1)
            if self._buff.full():
                self.event.wait()
            self._buff.put({'bar': block, 'transaction': None})

    def __read_data_type4(self):
        start = int(self.__start_datetime.strftime("%Y%m%d"))
        end = int(self.__end_datetime.strftime("%Y%m%d"))
        file_name = "{}/{}.h5".format(self.__data_root.rstrip("/"), self.__stock_list)
        store = HDFStore(file_name, mode='r')
        if self.__need_date_filter:
            for block in store.select("data", where="dt >= start & dt <= end", chunksize=50000):
                if self._buff.full():
                    self.event.wait()
                self._buff.put({'bar': block, 'transaction': None})
            store.close()
        else:
            start_year, _ = divmod(start, 10000)
            end_year, _ = divmod(end, 10000)
            if start_year < end_year:
                while start_year <= end_year:
                    for block in store.select("/data/y{}".format(start_year), chunksize=50000):
                        if self._buff.full():
                            self.event.wait()
                        self._buff.put({'bar': block, 'transaction': None})
                    start_year += 1
                store.close()
            else:
                for block in store.select("/data/y{}".format(start_year), chunksize=50000):
                    if self._buff.full():
                        self.event.wait()
                    self._buff.put({'bar': block, 'transaction': None})
                store.close()

    def __start_reading_process(self) ->None:
        if self.__data_type > self.__data_read_api.__len__():
            raise Exception("Unsupported Data type")
        api = self.__data_read_api[self.__data_type]
        api()
        ############################
        # all the data are read from disk, set the finished flag
        self.lock.acquire()
        self.finished = True
        self.lock.release()
        ########################

    def __data_filter(self, code: str=..., stock_data: DataFrame=...) ->DataFrame:
        start_timestamp = self.__start_datetime.timestamp()
        end_timestamp = self.__end_datetime.timestamp()
        valid_date = (stock_data['TimeStamp'] >= start_timestamp) & (stock_data['TimeStamp'] <= end_timestamp)
        stock_data = stock_data[valid_date]  # 仅保留startDateTime和endDateTime之间的数据
        if code[-1] == 'H':
            end_time = Const.SH_CLOSE
        else:
            end_time = Const.SZ_CLOSE
        if self._mode == 1:
            end_time = min(end_time, self.__end_time)
        ifilter = ((self.__start_time <= stock_data['Time']) & (stock_data['Time'] < Const.MORNING_CLOSE)) | \
                  ((Const.AFTERNOON_OPEN <= stock_data['Time']) & (stock_data['Time'] < end_time))
        stock_data = stock_data[ifilter]  # 仅保留在连续竞价期间的数据
        return stock_data

