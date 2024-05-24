import backtrader as bt
from queue import Queue
from exchange.okxManager import OkxManager
from datetime import timedelta, datetime


# 实盘数据源
class LiveTradingDataFeed(bt.feed.DataBase):
    def __init__(self, symbol, interval, parse_interval, max_length):
        super().__init__()
        self.symbol = symbol.replace('USDT', '')
        self.interval = interval
        self.parse_interval = parse_interval

        self.data_queue = Queue()
        self.last_date = None

        self.historical_data = None
        self.historical_done = False
        self._historical_iter = None

        self.max_length = max_length

        self.data_stream_observable = None
        self.init_flag = True
        self.new_flag = True
        self.okx_manager = OkxManager(symbol=self.symbol, interval=self.interval, live_mode=False)

    def islive(self):
        return True

    def start(self):
        super().start()
        self.historical_data = self.okx_manager.candles_init(self.max_length)
        self._historical_iter = iter(self.historical_data)
        self.data_stream_observable = self.okx_manager.get_okx_candles_channel_stream()
        self.data_stream_observable.subscribe(self.update_lines)

    def update_lines(self, data):
        self.data_queue.put(data)

    def _load(self):
        data_ready = False
        while not data_ready:
            if not self.historical_done:
                try:
                    row = next(self._historical_iter)
                    self.lines.datetime[0] = bt.date2num(row['time'])
                    self.lines.open[0] = row['open']
                    self.lines.high[0] = row['high']
                    self.lines.low[0] = row['low']
                    self.lines.close[0] = row['close']
                    self.lines.volume[0] = row['volume']
                    data_ready = True
                    return data_ready
                except StopIteration:
                    self.historical_done = True

            if not self.data_queue.empty():
                if self.init_flag:
                    new_date = self.data_queue.get()
                    self.last_date = new_date
                    self.init_flag = False
                else:
                    new_date = self.data_queue.get()
                    if len(self.lines.datetime) > 1:
                        system_time = datetime.now().replace(microsecond=0)
                        time_diff_sys = abs((system_time - self.last_date['time']).total_seconds())
                        if time_diff_sys >= 70:
                            self.new_flag = False
                        else:
                            self.new_flag = True

                        if len(self.lines.datetime) > 2:
                            pre_time = bt.num2date(self.lines.datetime[-1])
                            doupre_time = bt.num2date(self.lines.datetime[-2])
                            time_diff_pre = pre_time - doupre_time
                            interval_timedelta = self.parse_interval
                            if time_diff_pre != interval_timedelta:
                                print('相邻k线之间时间间隔不匹配 正在重新加载数据源')
                                self.historical_data = self.okx_manager.candles_init(self.max_length)
                                self._historical_iter = iter(self.historical_data)
                                self.historical_done = False
                                self.init_flag = True

                    if self.last_date['time'] != new_date['time'] and self.historical_done and self.new_flag:
                        self.lines.datetime[0] = bt.date2num(self.last_date['time'])
                        self.lines.open[0] = self.last_date['open']
                        self.lines.high[0] = self.last_date['high']
                        self.lines.low[0] = self.last_date['low']
                        self.lines.close[0] = self.last_date['close']
                        self.lines.volume[0] = self.last_date['volume']

                        data_ready = True
                        self.init_flag = data_ready
                        return data_ready
                    self.last_date = new_date

