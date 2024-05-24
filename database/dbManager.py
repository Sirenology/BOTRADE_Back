import logging
import time
from base.configManager import ConfigManager
from exchange.binanceManager import BinanceManager
import pymysql
from datetime import datetime, timedelta
import pandas as pd

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


# 数据库模块
class DatabaseManager:
    def __init__(self, mysql_db, mysql_table, table_type, use_type):
        # 读取配置信息
        config = ConfigManager()
        mysql_port = config.get('MYSQL', 'PORT')
        mysql_host = config.get('MYSQL', 'HOST')
        mysql_user = config.get('MYSQL', 'USER')
        mysql_pwd = config.get('MYSQL', 'PASSWORD')

        # MySQL
        self.MYSQL_HOST = mysql_host
        self.MYSQL_DB = mysql_db
        self.MYSQL_USER = mysql_user
        self.MYSQL_PWD = mysql_pwd
        self.MYSQL_TABLE = mysql_table
        self.connect = pymysql.connect(
            host=self.MYSQL_HOST,
            db=self.MYSQL_DB,
            port=int(mysql_port),
            user=self.MYSQL_USER,
            passwd=self.MYSQL_PWD,
            charset='utf8mb4',
            use_unicode=False
        )
        self.cursor = self.connect.cursor()

        if not self.MYSQL_TABLE == 'OpenOrds':
            self.symbol = self.MYSQL_TABLE.split('USDT_')[0]
            self.timeframe = self.MYSQL_TABLE.split('USDT_')[1]

        if table_type not in ['cdles', 'ords']:
            raise ValueError("table_type参数错误")
        self.table_type = table_type
        if use_type not in ['read', 'write']:
            raise ValueError("use_type参数错误")
        self.use_type = use_type
        self.checkTable()

    # 检查表是否存在，如果不存在则创建
    def checkTable(self):
        check_table_exist_query = f"SHOW TABLES LIKE '{self.MYSQL_TABLE}';"
        check_table_empty_query = f"SELECT COUNT(*) FROM {self.MYSQL_TABLE};"
        self.cursor.execute(check_table_exist_query)
        isExist = self.cursor.fetchone()

        if self.table_type == 'cdles':
            if not isExist:
                if self.use_type == 'write':
                    logging.info(f"创建数据表: {self.MYSQL_TABLE}")
                    create_table_query = f"""
                            CREATE TABLE `{self.MYSQL_TABLE}` (
                                `OpenTime` DATETIME NOT NULL,
                                `Open` VARCHAR(100) NOT NULL,
                                `High` VARCHAR(100) NOT NULL,
                                `Low` VARCHAR(100) NOT NULL,
                                `Close` VARCHAR(100) NOT NULL,
                                `Volume` VARCHAR(100) NOT NULL
                            );
                            """
                    self.cursor.execute(create_table_query)
                    self.connect.commit()
                else:
                    logging.error(f"checkTable 数据表 {self.MYSQL_TABLE} 不存在 尝试进行获取")
                    logging.info(f"创建数据表: {self.MYSQL_TABLE}")
                    create_table_query = f"""
                                                CREATE TABLE `{self.MYSQL_TABLE}` (
                                                    `OpenTime` DATETIME NOT NULL,
                                                    `Open` VARCHAR(100) NOT NULL,
                                                    `High` VARCHAR(100) NOT NULL,
                                                    `Low` VARCHAR(100) NOT NULL,
                                                    `Close` VARCHAR(100) NOT NULL,
                                                    `Volume` VARCHAR(100) NOT NULL
                                                );
                                                """
                    self.cursor.execute(create_table_query)
                    self.connect.commit()
                    self.upload_mark_kline()
                    raise ValueError(f"checkTable 数据表 {self.MYSQL_TABLE} 数据已经填充完成 请重新运行尝试")
            else:
                self.cursor.execute(check_table_empty_query)
                isEmpty = self.cursor.fetchone()
                if isEmpty[0] == 0:
                    self.upload_mark_kline()
                    raise ValueError(f"checkTable 数据表 {self.MYSQL_TABLE} 数据已经填充完成 请重新运行尝试")

        elif self.table_type == 'ords':
            if not isExist:
                create_table_query = f"""
                        CREATE TABLE `{self.MYSQL_TABLE}` (
                            `CreateTime` DATETIME NOT NULL,
                            `ClOrdId` VARCHAR(100) NOT NULL,
                            `InstId` VARCHAR(100) NOT NULL,
                            `Side` VARCHAR(100) NOT NULL,
                            `PosSide` VARCHAR(100) NOT NULL,
                            `Size` VARCHAR(100) NOT NULL,
                            `AvgPrice` VARCHAR(100) NOT NULL,
                            `TdMode` VARCHAR(100) NOT NULL,
                            `OrdType` VARCHAR(100) NOT NULL,
                            `Leverage` VARCHAR(100) NOT NULL
                        );
                        """
                self.cursor.execute(create_table_query)
                self.connect.commit()

    # 删除数据表信息
    def delete_table_info(self):
        self.cursor.execute("DELETE FROM {}".format(self.MYSQL_TABLE))

    # 删除最新数据
    def delete_latest_info_by_opentime(self, opentime):
        query = f"DELETE FROM {self.MYSQL_TABLE} WHERE OpenTime = %s"
        self.cursor.execute(query, (opentime,))

    def fetch_all_data(self):
        query = f"SELECT * FROM {self.MYSQL_TABLE}"
        self.cursor.execute(query)

        data = self.cursor.fetchall()

        # 获取列名
        columns = [col_desc[0] for col_desc in self.cursor.description]

        pd_data = pd.DataFrame(data, columns=columns)

        return pd_data

    # 插入数据
    def insert_table(self, data_source):
        sql_insert = "insert into {}(OpenTime,Open,High,Low,Close,Volume) VALUES (%s, %s, %s, %s, %s, %s)".format(
            self.MYSQL_TABLE)
        try:
            if self.table_type == 'cdles':
                if data_source:
                    if isinstance(data_source, list):
                        for data in data_source:
                            self.cursor.execute(
                                sql_insert,
                                (datetime.utcfromtimestamp(data[0] / 1000) + timedelta(hours=8), data[1], data[2],
                                 data[3], data[4], data[5] * 100)),
                    else:
                        self.cursor.execute(
                            sql_insert,
                            (data_source['time'], data_source['open'], data_source['high'],
                             data_source['low'], data_source['close'], data_source['volume'] * 100))
                    self.connect.commit()
                else:
                    pass
            elif self.table_type == 'ords':
                sql_insert = "insert into {}(CreateTime,ClOrdId,InstId,Side,PosSide,Size,AvgPrice,TdMode,OrdType,Leverage) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)".format(
                    self.MYSQL_TABLE)
                if data_source:
                    self.cursor.execute(
                        sql_insert,
                        (datetime.utcfromtimestamp(
                            int(data_source['trade_order']['data'][0]['cTime']) / 1000) + timedelta(hours=8),
                         data_source['trade_order']['data'][0]['clOrdId'],
                         data_source['trade_order']['data'][0]['instId'],
                         data_source['trade_order']['data'][0]['side'],
                         data_source['trade_order']['data'][0]['posSide'],
                         data_source['trade_order']['data'][0]['sz'],
                         data_source['trade_order']['data'][0]['avgPx'],
                         data_source['trade_order']['data'][0]['tdMode'],
                         data_source['trade_order']['data'][0]['ordType'],
                         data_source['leverage'])
                    )
                    self.connect.commit()
                else:
                    pass

        except Exception as e:
            logging.error(f"数据插入失败: {e}")

    # 更新数据
    def update_table(self, data_source):
        sql_update = "UPDATE {} SET Open=%s, High=%s, Low=%s, Close=%s, Volume=%s WHERE OpenTime=%s".format(
            self.MYSQL_TABLE)
        try:
            self.cursor.execute(sql_update, (
                data_source['open'], data_source['high'],
                data_source['low'], data_source['close'], data_source['volume'], data_source['time']))
            self.connect.commit()
        except Exception as e:
            logging.error(f"数据更新失败: {e}")

    # 检查数据表完整性
    def check_and_data_array_to_table(self, data_array, okx_exchange, symbol, timeframe):
        time.sleep(15)
        expected_interval = timedelta(minutes=1)
        while True:
            print(f"开始检查 {self.MYSQL_TABLE} 表完整性")
            # 检查数据表完整性 补充缺失数据
            self.cursor.execute(f"SELECT MIN(OpenTime), MAX(OpenTime) FROM `{self.MYSQL_TABLE}`")
            min_time, max_time = self.cursor.fetchone()

            if min_time and max_time:
                missing_opentime_interval = []  # 用来记录缺失数据的时间段
                start_missing_period = None  # 缺失时间段的开始时间点
                curr_time = min_time

                while curr_time <= max_time:
                    # 检查当前时间点是否存在数据
                    self.cursor.execute(
                        f"SELECT EXISTS(SELECT 1 FROM `{self.MYSQL_TABLE}` WHERE OpenTime = '{curr_time}' LIMIT 1)"
                    )
                    exist = self.cursor.fetchone()[0]
                    if not exist:
                        if not start_missing_period:
                            start_missing_period = curr_time - expected_interval
                    else:
                        if start_missing_period:
                            # 结束时间点是当前时间点减去一个时间间隔
                            end_missing_period = curr_time - expected_interval
                            missing_opentime_interval.append(
                                {'since': start_missing_period, 'until': end_missing_period})
                            start_missing_period = None  # 重置开始时间点，为下一个缺失段准备
                    curr_time += expected_interval

                # 补充缺失数据
                if missing_opentime_interval:
                    print(
                        f"{self.MYSQL_TABLE} 表发现缺失数据: {len(missing_opentime_interval)}段，正在重新获取...")
                    check_min_time = min_time
                    check_flag = True
                    for opentime_interval in missing_opentime_interval:
                        try:
                            # fetch_ohlcv()方法前后不包含since和until时间点的数据，所以需要对时间戳进行微调
                            opentime_since = int(opentime_interval['since'].timestamp()) * 1000
                            opentime_until = int(opentime_interval['until'].timestamp()) * 1000 + 60000
                            missing_data = okx_exchange.fetch_ohlcv(symbol=f"{symbol}-USDT-SWAP",
                                                                    timeframe=timeframe,
                                                                    since=opentime_since,
                                                                    params={'until': opentime_until}
                                                                    )

                            self.delete_latest_info_by_opentime(opentime_interval['since'])
                            self.insert_table(missing_data)

                        except Exception as e:
                            logging.error(f"{self.MYSQL_TABLE} 表添加缺失数据失败: {e}")
                    while check_min_time <= max_time:
                        # 检查补充数据后是否还有缺失数据
                        self.cursor.execute(
                            f"SELECT EXISTS(SELECT 1 FROM `{self.MYSQL_TABLE}` WHERE OpenTime = '{check_min_time}' LIMIT 1)"
                        )
                        check_exist = self.cursor.fetchone()[0]
                        if not check_exist:
                            check_flag = False
                            missing_opentime_interval.clear()
                            print(f"{self.MYSQL_TABLE} 表数据尚未添加完成(API单次达到获取上线)等待下次添加")
                            break
                        check_min_time += expected_interval
                    if check_flag:
                        missing_opentime_interval.clear()
                        print(f"{self.MYSQL_TABLE} 表缺失数据已补全")
                else:
                    print(f"{self.MYSQL_TABLE} 表数据完整，无需补全")
            else:
                print(f"{self.MYSQL_TABLE} 表数据为空，无需补全")
            time.sleep(5)

            # 同步data_array至数据库
            self.cursor.execute(f"SELECT MAX(OpenTime) FROM `{self.MYSQL_TABLE}`")
            result = self.cursor.fetchone()[0]
            max_open_time = result if result else datetime.min
            for data in data_array:
                if data['time'] > max_open_time:
                    self.insert_table(data)
                elif data['time'] == max_open_time:
                    self.update_table(data)
                else:
                    continue
            logging.info(f"完成 {self.MYSQL_TABLE} 表同步data_array数据")
            time.sleep(15)

    # 获取并保存U本位合约标记价格K线数据
    # timeframe: K线周期 例如: '1m', '5m', '15m', '1h', '4h', '1d'
    def upload_mark_kline(self):
        bian_exchange = BinanceManager(symbol=self.symbol, timeframe=self.timeframe, live_mode=True).bian_exchange

        print(f"开始获取Binance {self.symbol}USDT {self.timeframe} K线数据")
        # 将日期字符串转换为 datetime 对象,获取对应的时间戳（以毫秒为单位）
        start_date = int(datetime.strptime("2017-08-17 00:00:00", "%Y-%m-%d %H:%M:%S").timestamp()) * 1000
        end_date = int(datetime.strptime("2024-02-17 00:00:00", "%Y-%m-%d %H:%M:%S").timestamp()) * 1000
        candle_list = []
        flag = 0
        max_retries = 5  # 最大重试次数
        init_retries = 0  # 初始化重试次数
        success_flag = False
        while init_retries <= max_retries and not success_flag:
            try:
                while start_date <= end_date:
                    ohlcv_data = bian_exchange.fetch_ohlcv(f'{self.symbol}USDT', self.timeframe,
                                                           since=start_date)
                    if ohlcv_data:
                        next_date = ohlcv_data[-1][0]
                        start_date = next_date
                        if flag == 0:
                            candle_list += ohlcv_data
                            flag = 1
                        else:
                            candle_list += ohlcv_data[1:]
                    else:
                        break

                success_flag = True  # 数据获取成功，设置成功标志

                # 重置重试次数
                init_retries = 0
            except Exception as e:
                init_retries += 1
                if init_retries < max_retries:
                    print(
                        f"获取Binance {self.symbol}USDT {self.timeframe} K线数据失败: {e} 正在重试... 第 {init_retries} 次 / 最大次数: {max_retries} 次")
                    time.sleep(2)
                else:
                    print(f"重试次数已达最大次数: {max_retries} 次")
                    break

        candle_list = [{
            'time': datetime.utcfromtimestamp(data[0] / 1000) + timedelta(hours=8),
            'open': float(data[1]),
            'high': float(data[2]),
            'low': float(data[3]),
            'close': float(data[4]),
            'volume': float(data[5])
        } for data in candle_list]

        for candle in candle_list:
            self.insert_table(candle)
        print('数据插入成功！')

        # self.check_bian_backtest(bian_exchange)

    # 检查回测数据完整性
    def check_bian_backtest(self, bian_exchange):
        print(f"开始检查 {self.MYSQL_TABLE} 表完整性")

        frame_parse = self.timeframe[:-1]
        if self.timeframe.endswith('m'):
            expected_interval = timedelta(minutes=int(frame_parse))
        elif self.timeframe.endswith('h' or 'H'):
            expected_interval = timedelta(hours=int(frame_parse))
        else:
            raise ValueError("timeframe格式错误")

        bian_exchange.cursor.executeByDataBase(f"SELECT MIN(OpenTime), MAX(OpenTime) FROM `{self.MYSQL_TABLE}`")
        min_time, max_time = bian_exchange.cursor.fetchone()

        if min_time and max_time:
            missing_opentimes = []
            curr_time = min_time

            while curr_time <= max_time:
                self.cursor.execute(
                    f"SELECT COUNT(*) FROM `{self.MYSQL_TABLE}` WHERE OpenTime = '{curr_time}'"
                )
                count = self.cursor.fetchone()[0]
                if count and count == 1:
                    pass
                else:
                    # 删除重复数据
                    self.cursor.execute(
                        f"DELETE FROM `{self.MYSQL_TABLE}` WHERE OpenTime = '{curr_time}'"
                    )
                    missing_opentimes.append(curr_time)
                curr_time += expected_interval
            if missing_opentimes:
                print(f"{self.MYSQL_TABLE} 表发现缺失数据: {len(missing_opentimes)}个时间点，正在重新获取...")
                for missing_opentime in missing_opentimes:
                    try:
                        missing_data = bian_exchange.fetch_ohlcv(symbol=f"{self.symbol}USDT",
                                                                 timeframe=self.timeframe,
                                                                 since=int(missing_opentime.timestamp()) * 1000,
                                                                 limit=1
                                                                 )
                        self.insert_table(missing_data)
                    except Exception as e:
                        print(f"{self.MYSQL_TABLE} 表添加缺失数据失败: {e}")
                print(f"{self.MYSQL_TABLE} 表缺失数据已补全")
            else:
                print(f"{self.MYSQL_TABLE} 表数据完整")
        else:
            print(f"{self.MYSQL_TABLE} 表无数据")

    # 批量获取不同品种的数据
    def batch_upload_mark_kline(self):
        coins = ["BLZ", "WIF", "BOME", "ONG", "ONDO", "OMNI"]
        timeframes = ["15m", "30m", "1h", "4h"]
        for coin in coins:
            for timeframe in timeframes:
                self.symbol = coin
                self.timeframe = timeframe
                self.MYSQL_TABLE = f"{coin}USDT_{timeframe}"
                check_table_exist_query = f"SHOW TABLES LIKE '{self.MYSQL_TABLE}';"
                check_table_empty_query = f"SELECT COUNT(*) FROM {self.MYSQL_TABLE};"
                self.cursor.execute(check_table_exist_query)
                isExist = self.cursor.fetchone()
                if not isExist:
                    logging.error(f"batch_upload_mark_kline 数据表 {self.MYSQL_TABLE} 不存在 进行获取")
                    logging.info(f"创建数据表: {self.MYSQL_TABLE}")
                    create_table_query = f"""
                                            CREATE TABLE `{self.MYSQL_TABLE}` (
                                                `OpenTime` DATETIME NOT NULL,
                                                `Open` VARCHAR(100) NOT NULL,
                                                `High` VARCHAR(100) NOT NULL,
                                                `Low` VARCHAR(100) NOT NULL,
                                                `Close` VARCHAR(100) NOT NULL,
                                                `Volume` VARCHAR(100) NOT NULL
                                            );
                                            """
                    self.cursor.execute(create_table_query)
                    self.connect.commit()
                    self.upload_mark_kline()
                else:
                    self.cursor.execute(check_table_empty_query)
                    isEmpty = self.cursor.fetchone()
                    if isEmpty[0] == 0:
                        logging.error(f"batch_upload_mark_kline 数据表 {self.MYSQL_TABLE} 获取数据为空 尝试进行获取")
                        self.upload_mark_kline()
                    print(f"{self.MYSQL_TABLE} 表数据完整")
        print("所有数据表数据已经填充完成")
        raise ValueError("所有数据表数据已经填充完成 请重新运行尝试")
