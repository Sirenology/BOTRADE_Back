import ccxt
from base.configManager import ConfigManager


class BinanceManager:
    def __init__(self, symbol=None, timeframe=None, live_mode=False):
        # 读取配置信息
        config = ConfigManager()
        proxies = config.get('PROXIES', 'PROXIES')

        if live_mode is not True or False:
            raise ValueError("live_mode参数必须为布尔值")
        if live_mode:
            # okx_api_key = config.get('BINANCE_EXCHANGE_SANDBOX', 'API_KEY')
            # okx_secret = config.get('BINANCE_EXCHANGE_SANDBOX', 'SECRET')
            # okx_pwd = config.get('BINANCE_EXCHANGE_SANDBOX', 'PASSWORD')
            # 初始化交易所信息配置(沙盒 U本位合约)
            # self.okx_exchange = ccxt.okx(
            #     {
            #         'proxies': {'http': proxies, 'https': proxies},  # 代理配置
            #         'apiKey': okx_api_key,
            #         'secret': okx_secret,
            #         'password': okx_pwd,
            #         'fetchMarkets': 'swap',
            #         'defaultType': 'swap',
            #         'timeout': 3000,
            #         # 'rateLimit': 10,  # 限制请求次数
            #         'enableRateLimit': True
            #     }
            # )
            # self.okx_exchange.set_sandbox_mode(True)
            # else:
            bian_api_key = config.get('BINANCE_EXCHANGE_LIVE', 'API_KEY')
            bian_secret = config.get('BINANCE_EXCHANGE_LIVE', 'SECRET')

            # 初始化交易所信息配置(实盘 U本位合约)
            self.bian_exchange = ccxt.binance(
                {
                    'proxies': {'http': proxies, 'https': proxies},  # 代理配置
                    'apiKey': bian_api_key,
                    'secret': bian_secret,
                    'options': {
                        'fetchMarkets': ['linear'],
                        'defaultType': 'future',
                    },
                    'timeout': 3000,
                    'enableRateLimit': True
                }
            )

        self.symbol = symbol
        self.timeframe = timeframe


