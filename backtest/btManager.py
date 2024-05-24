import logging
import threading

import backtrader as bt
import pandas as pd

from database.dbManager import DatabaseManager
from strategy.strategyManager import StrategyManager
from btLiveTradeRewrite import LiveTradingDataFeed
from base.configManager import ConfigManager

from datetime import timedelta

pd.set_option('display.max_rows', None)  # 显示最多行数
pd.set_option('display.max_columns', None)  # 显示最多列数
pd.set_option('expand_frame_repr', False)  # 当列太多时显示不清楚

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class BackTestManager:

    def __init__(self, symbol='BTCUSDT', interval='1m'):
        self.symbol = symbol  # 交易对 BTCUSDT
        self.symbol_base = symbol.replace('USDT', '')  # 交易对基础币种 BTC

        self.interval = interval
        self.parse_interval = self.parse_interval()

        self.backtestInfo = {}
        self.candleInfo = []
        self.performanceInfo = []
        self.earliestDate = None
        self.latestDate = None

    # =================== 解析周期 ===================
    def parse_interval(self):
        unit = self.interval[-1]
        quantity = int(self.interval[:-1])

        if unit == 'm':
            return timedelta(minutes=quantity)
        elif unit == 'h' or unit == 'H':
            return timedelta(hours=quantity)
        elif unit == 'd':
            return timedelta(days=quantity)

    # =================== 获取数据源 ===================
    def getDataSource(self):
        logging.info(f'getDataSource 获取数据源 品种: {self.symbol} 周期: {self.interval}')

        try:
            # 加载数据
            dbmanager = DatabaseManager(mysql_db='backTrade_Data', mysql_table=f'{self.symbol}_{self.interval}',
                                        table_type='cdles', use_type='read')

            dataSource = dbmanager.fetch_all_data()
        except Exception as e:
            logging.error(f"getDataSource 获取数据源失败: {e}")
            raise ValueError(f"getDataSource 获取数据源失败 {e}")

        # 转换字节字符串为普通字符串
        for column in dataSource.columns:
            if dataSource[column].dtype == object:  # 只转换对象类型的列，即可能包含字符串的列
                dataSource[column] = dataSource[column].apply(
                    lambda x: x.decode('utf-8') if isinstance(x, bytes) else x)

        dataSource['OpenTime'] = pd.to_datetime(dataSource['OpenTime'])
        dataSource.set_index('OpenTime', inplace=True)

        numColumns = ['Open', 'High', 'Low', 'Close', 'Volume']
        for col in numColumns:
            dataSource[col] = dataSource[col].astype(float)

        self.earliestDate = dataSource.index[0]
        self.latestDate = dataSource.index[-1]

        return dataSource

    # =================== 执行回测 ===================

    """
    参数：
        symbol: str 示例：'BTCUSDT'
            交易对
        startDate: str 示例：'2021-01-01 00:00:00'
            回测开始时间
        endDate: str 示例：'2021-01-02 00:00:00'
            回测结束时间
        interval: str 示例：'1h'
            数据源时间周期
        useStrategy: str 示例：'BBandStrategy'
            使用的策略
        initCash: float 示例：10000
            初始资金

    返回：
        dataSource: pd.DataFrame
            数据源

    """

    def executeByDataBase(self, useStrategy, startDate, endDate, initCash=10000):
        logging.info(
            f'execute 执行回测 品种: {self.symbol} 回测时间段: ( {startDate},{endDate} ) 周期: {self.interval} 策略: {useStrategy} 初始资金: {initCash}')

        # 获取数据源
        dataSource = self.getDataSource()

        if startDate < self.earliestDate:
            logging.error(f"execute 回测开始时间 {startDate} 早于数据源最早时间 {self.earliestDate}")
            raise ValueError(f"execute 回测开始时间 {startDate} 早于数据源最早时间 {self.earliestDate}")

        if endDate > self.latestDate:
            logging.error(f"execute 回测结束时间 {endDate} 晚于数据源最晚时间 {self.latestDate}")
            raise ValueError(f"execute 回测结束时间 {endDate} 晚于数据源最晚时间 {self.latestDate}")

        dataSource = dataSource.loc[startDate:endDate]

        # 创建策略管理器
        strategyManager = StrategyManager()

        for strategy in strategyManager.strategies:
            if strategy.__name__ == useStrategy:
                useStrategy = strategy

        # 如果 useStrategy 不是 <class 'strategy.bbandStrategy.BBandStrategy'> 类型 则报错
        if not issubclass(useStrategy, bt.Strategy):
            logging.error(f"execute 策略 {useStrategy} 不存在")
            raise ValueError(f"execute 策略 {useStrategy} 不存在")

        cerebro = bt.Cerebro()

        # 加载数据
        data = bt.feeds.PandasData(dataname=dataSource)

        cerebro.adddata(data)

        cerebro.addstrategy(useStrategy, is_live=False)

        # 添加分析器
        cerebro.addanalyzer(bt.analyzers.DrawDown, _name='DrawDown')

        # 设置初始资金
        cerebro.broker.setcash(initCash)

        cerebro.addsizer(bt.sizers.PercentSizer, percents=98)  # 设置交易量

        # 运行回测
        resultInfo = cerebro.run()
        resultInfo = resultInfo[0]

        sell_signal = pd.DataFrame(resultInfo.sell_signal)
        buy_signal = pd.DataFrame(resultInfo.buy_signal)
        indicators = pd.DataFrame(resultInfo.indicator_data)
        trade_history = pd.DataFrame(resultInfo.trade_history)
        max_drawdown = resultInfo.analyzers.DrawDown.get_analysis()

        candle_line = dataSource.reset_index()
        candle_line = candle_line.rename(columns={'OpenTime': 'Date'})
        candle_line.set_index('Date', inplace=True)

        df = pd.merge(candle_line, indicators, on='Date', how='outer')

        if not buy_signal.empty:
            df = pd.merge(df, buy_signal, on='Date', how='outer', suffixes=('', '_buy'))
        if not sell_signal.empty:
            df = pd.merge(df, sell_signal, on='Date', how='outer', suffixes=('', '_sell'))

        candle_records = df.to_dict(orient='records')
        performance_records = trade_history.to_dict(orient='records')

        for candle_record in candle_records:
            new_record = {}
            for key, value in candle_record.items():
                # 如果值是Timestamp类型，转换为字符串格式
                if isinstance(value, pd.Timestamp):
                    new_record[key] = value.strftime("%Y-%m-%d %H:%M:%S")
                # 如果值是NaN，转换为None
                elif pd.isna(value):
                    new_record[key] = None
                else:
                    new_record[key] = value
            self.candleInfo.append(new_record)

        for performance_record in performance_records:
            new_record = {}
            for key, value in performance_record.items():
                # 如果值是Timestamp类型，转换为字符串格式
                if isinstance(value, pd.Timestamp):
                    new_record[key] = value.strftime("%Y-%m-%d %H:%M:%S")
                else:
                    new_record[key] = value
            self.performanceInfo.append(new_record)

        self.backtestInfo = {
            'candleInfo': self.candleInfo,
            'performanceInfo': self.performanceInfo,
            'maxDrawdown': max_drawdown
        }

        return self.backtestInfo

    # =================== 执行实盘 ===================
    def excuteByLiveData(self, useStrategy):
        logging.info(
            f'excuteByLiveData 执行实盘 品种: {self.symbol} 周期: {self.interval} 策略: {useStrategy}')

        # 创建策略管理器
        strategyManager = StrategyManager()

        for strategy in strategyManager.strategies:
            if strategy.__name__ == useStrategy:
                useStrategy = strategy

        if not issubclass(useStrategy, bt.Strategy):
            logging.error(f"execute 策略 {useStrategy} 不存在")
            raise ValueError(f"execute 策略 {useStrategy} 不存在")

        cerebro = bt.Cerebro()

        config = ConfigManager()
        max_length = int(config.get('STRATEGY_MAX_LENGTH', useStrategy.__name__))

        # 加载数据
        live_data = LiveTradingDataFeed(symbol=self.symbol, interval=self.interval, parse_interval=self.parse_interval,
                                        max_length=max_length)

        cerebro.adddata(live_data)

        cerebro.addstrategy(useStrategy, symbol=self.symbol, is_live=True, interval=self.interval,
                            parse_interval=self.parse_interval)

        # 添加分析器
        cerebro.addanalyzer(bt.analyzers.DrawDown, _name='DrawDown')

        # 运行回测
        cerebro.run()

        stop_event = threading.Event()
        stop_event.wait()
