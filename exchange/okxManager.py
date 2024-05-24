import threading
import ccxt
import logging
import websocket
import json
import queue
import backoff

from rx.subject import Subject
from datetime import datetime, timedelta
from base.configManager import ConfigManager

logger = logging.getLogger(__name__)


# OKX交易所数据获取
class OkxManager:

    def __init__(self, symbol=None, interval=None, live_mode=False):

        if type(live_mode) is not bool:
            raise ValueError("live_mode参数必须为布尔值")

        self.live_mode = live_mode  # 是否为实盘模式
        self.okx_exchange = self.okx_exchange_init()

        # 初始化参数
        self.symbol = symbol
        self.instId = f"{self.symbol}-USDT-SWAP"
        self.interval = interval
        self.data_array = []
        self.data_queue = queue.Queue()
        self.subject = Subject()
        self.stop_event = threading.Event()  # 用于停止线程
        self.missing_datetime_data = []
        self.candles_channel_url = "wss://ws.okx.com:8443/ws/v5/business"
        self.ws = None  # WebSocket连接

    # 初始化OKX交易所
    @backoff.on_exception(backoff.expo, Exception, max_time=60)
    def okx_exchange_init(self):
        config = ConfigManager()
        proxies = config.get('PROXIES', 'PROXIES')
        try:
            if not self.live_mode:
                okx_api_key = config.get('OKX_EXCHANGE_SANDBOX', 'API_KEY')
                okx_secret = config.get('OKX_EXCHANGE_SANDBOX', 'SECRET')
                okx_pwd = config.get('OKX_EXCHANGE_SANDBOX', 'PASSWORD')

                # 初始化交易所信息配置(沙盒 U本位合约)
                okx_exchange = ccxt.okx(
                    {
                        'proxies': {'http': proxies, 'https': proxies},  # 代理配置
                        'apiKey': okx_api_key,
                        'secret': okx_secret,
                        'password': okx_pwd,
                        'fetchMarkets': 'swap',
                        'defaultType': 'swap',
                        'timeout': 3000,
                        'rateLimit': 10,  # 限制请求次数
                        'enableRateLimit': True
                    }
                )
                okx_exchange.set_sandbox_mode(True)
            else:
                okx_api_key = config.get('OKX_EXCHANGE_LIVE', 'API_KEY')
                okx_secret = config.get('OKX_EXCHANGE_LIVE', 'SECRET')
                okx_pwd = config.get('OKX_EXCHANGE_LIVE', 'PASSWORD')

                # 初始化交易所信息配置(实盘 U本位合约)
                okx_exchange = ccxt.okx(
                    {
                        'proxies': {'http': proxies, 'https': proxies},  # 代理配置
                        'apiKey': okx_api_key,
                        'secret': okx_secret,
                        'password': okx_pwd,
                        'fetchMarkets': 'swap',
                        'defaultType': 'swap',
                        'timeout': 3000,
                        'rateLimit': 10,  # 限制请求次数
                        'enableRateLimit': True
                    }
                )
        except Exception as e:
            logger.error(f"初始化交易所信息配置失败: {e}")
            raise

        return okx_exchange

    # 获取OKX交易所账户余额信息
    @backoff.on_exception(backoff.expo, Exception, max_tries=60)
    def get_account_balance_info(self):
        try:
            balance_info = self.okx_exchange.private_get_account_balance()
            curr_balance = float(balance_info['data'][0]['details'][0]['availBal'])
            print(
                # f"时间: {datetime.utcfromtimestamp(int(balance_info['data'][0]['uTime'])) + timedelta(hours=8)} "
                f"可用余额: {curr_balance} ")
            return curr_balance
        except Exception as e:
            logger.error(f"获取账户余额信息失败: {e}")
            raise

    # 连接OKX交易所 K线频道
    @backoff.on_exception(backoff.expo, Exception, max_tries=60)
    def _connet_candles_channel(self):
        print("连接OKX K线频道")
        while not self.stop_event.is_set():
            try:
                self.ws = websocket.create_connection(self.candles_channel_url)
                if self.interval.endswith('h'):
                    interval = self.interval.replace('h', 'H')
                else:
                    interval = self.interval

                # 订阅K线数据
                subsribe_msg = json.dumps({
                    "op": "subscribe",
                    "args": [{
                        "channel": f"candle{interval}",
                        "instId": f"{self.symbol}-USDT-SWAP",
                    }]
                })
                self.ws.send(subsribe_msg)
                return
            except Exception as e:
                logger.error(f"连接OKX K线频道失败:{e}")
                raise

    # 监听websocket消息
    @backoff.on_exception(backoff.expo, Exception, max_tries=60)
    def _listen_ws(self):
        self._connet_candles_channel()
        while not self.stop_event.is_set():
            try:
                candles_json = self.ws.recv()
                candles_info = json.loads(candles_json)
                # 检查是否为订阅确认消息
                if 'event' in candles_info and candles_info['event'] == 'subscribe':
                    print("订阅确认: 品种为", candles_info['arg']['instId'], "频道为",
                          candles_info['arg']['channel'])

                if 'data' in candles_info:
                    candles_info = {
                        'time': datetime.utcfromtimestamp(int(candles_info['data'][0][0]) / 1000) + timedelta(
                            hours=8),
                        'open': float(candles_info['data'][0][1]),
                        'high': float(candles_info['data'][0][2]),
                        'low': float(candles_info['data'][0][3]),
                        'close': float(candles_info['data'][0][4]),
                        'volume': float(candles_info['data'][0][5])
                    }

                    curr_datetime = candles_info['time']
                    pre_datetime = self.data_array[-1]['time'] if self.data_array else datetime.min
                    if curr_datetime > pre_datetime:
                        self.data_array.append(candles_info)
                    else:
                        for index, key in enumerate(['open', 'high', 'low', 'close', 'volume'], start=1):
                            self.data_array[-1][key] = candles_info[key]

                    self.data_queue.put(candles_info)
            except Exception as e:
                logger.error(f"监听websocket消息失败:{e}")
                raise

    # 监听队列
    def _listen_queue(self):
        while not self.stop_event.is_set():
            # 从队列中取出数据，如果队列为空则阻塞等待
            data = self.data_queue.get(block=True)
            # 将数据推送到数据流中
            self.subject.on_next(data)

    # 获取OKX K线频道数据流
    def get_okx_candles_channel_stream(self):
        threading.Thread(target=self._listen_ws, daemon=True).start()
        threading.Thread(target=self._listen_queue, daemon=True).start()

        return self.subject

    # 停止监听和WebSocket连接
    def stop_channel(self):
        self.stop_event.set()
        if self.ws:
            self.ws.close()

    # 实盘数据预加载
    @backoff.on_exception(backoff.expo, Exception, max_time=30)
    def candles_init(self, max_length):
        try:
            init_candles = self.okx_exchange.fetch_ohlcv(symbol=f"{self.symbol}-USDT-SWAP",
                                                         timeframe=self.interval,
                                                         limit=max_length)
            init_candles = [{
                'time': datetime.utcfromtimestamp(init_candle[0] / 1000) + timedelta(hours=8),
                'open': init_candle[1],
                'high': init_candle[2],
                'low': init_candle[3],
                'close': init_candle[4],
                'volume': init_candle[5] * 100
            } for init_candle in init_candles]

            # 删去时间最晚的一根K线
            init_candles.pop()
            return init_candles
        except Exception as e:
            logger.error(f"初始化数据加载失败: {e}")
            raise

