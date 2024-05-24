from position.positionManager import PositionManager
from database.dbManager import DatabaseManager
from exchange.okxManager import OkxManager
from datetime import datetime
import backtrader as bt


class Indicators(bt.Indicator):
    # 定义指标参数
    params = (
        ('length', 24),  # bbw指标长度
        ('curve_fast', 12),  # 快线周期
        ('curve_slow', 26),  # 慢线周期
        ('curve_singal_length', 9),  # 曲线信号周期
        ('nbdev', 1.0),
    )

    # 定义指标线
    lines = ('bbw', 'bbwsma', 'curve')

    # 初始化指标
    def __init__(self):
        std = bt.talib.STDDEV(self.data.close, timeperiod=self.params.length, nbdev=self.params.nbdev)

        ema = bt.talib.EMA(std, timeperiod=self.params.length)

        self.l.bbw = std / ema

        self.l.bbwsma = bt.talib.SMA(self.l.bbw, timeperiod=self.params.length)

        # 计算曲线
        curve_fast = bt.talib.ROC(self.data.close, timeperiod=self.params.curve_fast)
        curve_slow = bt.talib.ROC(self.data.close, timeperiod=self.params.curve_slow)

        self.l.curve = bt.talib.WMA(
            curve_fast + curve_slow,
            timeperiod=self.params.curve_singal_length)


# =================== 策略部分 ===================
class BBWCURVEStrategy(bt.Strategy):
    def __init__(self, is_live, symbol=None, interval=None, parse_interval=None):
        self.is_live = is_live
        # 判断交易模式
        if self.is_live:
            self.symbol = symbol.replace('USDT', '')  # 交易对 例如: BTC
            self.interval = interval
            print('当前为 实盘模式 交易对:', self.symbol, '周期:', self.interval)
            self.parse_interval = parse_interval

            self.instId = f"{self.symbol}-USDT-SWAP"  # 合约ID
            self.is_indicators_init = False  # 指标是否初始化
            self.is_data_ready = False  # 实时数据流预热
            self.indicators = None  # 指标
            self.trade_positions = []  # 交易仓位
            self.position_flag = 0  # 仓位标志 0: 无仓位 1: 多仓位 -1: 空仓位

            # tips:
            # 当有多个指标进行迭代平滑时 无需其他指标平滑的指标能够一次性使用之前的数据
            # 而需要其他指标平滑的指标需要等待其他指标加载到对应周期数

            # 结论: 有多个指标进行平滑时 min_length即为首个指标的周期数减1
            self.min_length = 23

            # 初始化获取账户仓位
            self.okx_manager = OkxManager(symbol=self.symbol, live_mode=False)
            self.init_trade_position()
        else:
            print('当前为 回测模式')
            self.indicators = Indicators()

            # 记录买入卖出信号
            self.sell_signal = []
            self.buy_signal = []

            # 初始化字典来存储指标数据
            self.indicator_data = {
                'Date': [],
                'bbw': [],
                'bbwsma': [],
                'curve': []
            }

            # 记录交易历史
            self.trade_history = []
            self.tradeId = 1

    # =================== 实盘部分函数 ===================

    # 获取历史未平仓仓位
    def init_trade_position(self):
        db_manager = DatabaseManager(mysql_db='QuantCoinHub', mysql_table='OpenOrds', table_type='ords',
                                     use_type='read')
        positions = db_manager.fetch_all_data()
        positions_sorted = positions.sort_values(by='ClOrdId')

        if not positions_sorted.empty:
            for index, row in positions_sorted.iterrows():
                side = row['Side'].decode('utf-8')
                posSide = row['PosSide'].decode('utf-8')
                size = row['Size'].decode('utf-8')

                position = PositionManager(symbol=self.symbol,
                                           side=side,
                                           pos_side=posSide,
                                           size=size,
                                           live_mode=False)
                self.trade_positions.append(position)

                if side == 'sell':
                    self.position_flag = -1
                elif side == 'buy':
                    self.position_flag = 1
        else:
            print("当前无正在进行的仓位")

    # =================== 回测部分函数 ===================

    # 记录回测日志
    def log(self, txt):
        print(f"{self.data.datetime.datetime()} {txt}")

    # 记录指标数据
    def record_indicators(self):
        self.indicator_data['Date'].append(self.data.datetime.datetime())
        self.indicator_data['bbw'].append(self.indicators.bbw[0])
        self.indicator_data['bbwsma'].append(self.indicators.bbwsma[0])
        self.indicator_data['curve'].append(self.indicators.curve[0])

    # 监听交易操作
    def notify_order(self, order):
        if order.status in [order.Submitted, order.Accepted]:
            return

        if order.status in [order.Completed]:
            if order.isbuy():
                self.log(f"执行买入 Buy, 买入价格：{order.executed.price}, 买入数量：{order.executed.size}")

                # 记录买入时间, 买入价格
                self.buy_signal.append({'Date': self.data.datetime.datetime(), 'Buy': order.executed.price})

            elif order.issell():
                self.log(f"执行卖出 Sell, 卖出价格：{order.executed.price}, 卖出数量：{order.executed.size}")

                # 记录卖出时间, 卖出价格
                self.sell_signal.append({'Date': self.data.datetime.datetime(), 'Sell': order.executed.price})

        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            self.log("交易失败")

    # 记录交易日志
    def notify_trade(self, trade):
        if not trade.isclosed:
            return

        self.log(
            f"交易id: {self.tradeId} 开始时间: {bt.num2date(trade.dtopen)} 结束时间: {bt.num2date(trade.dtclose)} 交易数量: {trade.size} 交易净利润: {trade.pnlcomm} 总金额: {self.broker.getvalue()}")

        # 记录交易历史
        self.trade_history.append(
            {'tradeid': self.tradeId, 'dtopen': bt.num2date(trade.dtopen), 'dtclose': bt.num2date(trade.dtclose),
             'pnlcomm': trade.pnlcomm, 'value': self.broker.getvalue(), 'buy': self.buy_signal[-1],
             'sell': self.sell_signal[-1]})

        self.tradeId += 1

    # =================== 策略部分函数 ===================

    # 策略主逻辑
    def next(self):
        if self.is_live:
            if len(self.data) < self.min_length:
                return

            if not self.is_indicators_init:
                print(len(self.data))
                self.indicators = Indicators()
                self.is_indicators_init = True
            else:
                print(
                    f'{len(self.data)} time: {self.data.datetime.datetime()} close: {self.data.close[0]} bbw: {self.indicators.bbw[0]} bbwsma: {self.indicators.bbwsma[0]} curve: {self.indicators.curve[0]}')
                current_time = bt.num2date(self.data.datetime[0])
                interval_timedelta = self.parse_interval
                system_time = datetime.now().replace(microsecond=0)
                time_diff_sys = abs((system_time - current_time - interval_timedelta).total_seconds())

                if len(self.data.datetime) > 2:
                    pre_time = bt.num2date(self.data.datetime[-1])
                    time_diff_pre = current_time - pre_time
                    if time_diff_pre != interval_timedelta:
                        return
                if time_diff_sys >= 2:
                    return

                self.next_logic_live()
        else:
            self.next_logic_back()
            self.record_indicators()

    # 策略逻辑 - 实盘
    def next_logic_live(self):
        print('执行策略逻辑', self.data.datetime.datetime(), self.data.close[0], self.indicators.bbw[0],
              self.indicators.bbwsma[0])
        if not self.position_flag:
            if self.indicators.bbw[0] > self.indicators.bbwsma[0] and self.indicators.curve[0] > 0:
                print(
                    f'bbw {self.indicators.bbw[0]} 上穿 bbwsma {self.indicators.bbwsma[0]} 且 curve {self.indicators.curve[0]} 大于 0 执行买入开仓')
                position = PositionManager(symbol=self.symbol, side='buy', pos_side='long', live_mode=False)
                position.open_position(curr_price=self.data.close[0])
                self.trade_positions.append(position)
                self.position_flag = 1
            elif self.indicators.bbw[0] < self.indicators.bbwsma[0] and self.indicators.curve[0] < 0:
                print(
                    f'bbw {self.indicators.bbw[0]} 下穿 bbwsma {self.indicators.bbwsma[0]} 且 curve {self.indicators.curve[0]} 小于 0 执行卖出开仓')
                position = PositionManager(symbol=self.symbol, side='sell', pos_side='short', live_mode=False)
                position.open_position(curr_price=self.data.close[0])
                self.trade_positions.append(position)
                self.position_flag = -1
        else:
            if self.position_flag > 0 and self.indicators.bbw[0] < self.indicators.bbwsma[0] and \
                    self.indicators.curve[
                        0] < 0:
                print(
                    f'bbw {self.indicators.bbw[0]} 下穿 bbwsma {self.indicators.bbwsma[0]} 且 curve {self.indicators.curve[0]} 小于 0 执行卖出平仓')
                for position in self.trade_positions:
                    position.close_position()
                    self.trade_positions.remove(position)
                self.position_flag = 0
            elif self.position_flag < 0 and self.indicators.bbw[0] > self.indicators.bbwsma[0] and \
                    self.indicators.curve[0] > 0:
                print(
                    f'bbw {self.indicators.bbw[0]} 上穿 bbwsma {self.indicators.bbwsma[0]} 且 curve {self.indicators.curve[0]} 大于 0 执行买入平仓')
                for position in self.trade_positions:
                    position.close_position()
                    self.trade_positions.remove(position)
                self.position_flag = 0

    # 策略逻辑 - 回测
    def next_logic_back(self):
        if not self.position:
            if self.indicators.bbw[0] > self.indicators.bbwsma[0] and self.indicators.curve[0] > 0:
                self.log(
                    f'bbw {self.indicators.bbw[0]} 上穿 bbwsma {self.indicators.bbwsma[0]} 且 curve {self.indicators.curve[0]} 大于 0 执行买入开仓')
                self.buy()
            elif self.indicators.bbw[0] < self.indicators.bbwsma[0] and self.indicators.curve[0] < 0:
                self.log(
                    f'bbw {self.indicators.bbw[0]} 下穿 bbwsma {self.indicators.bbwsma[0]} 且 curve {self.indicators.curve[0]} 小于 0 执行卖出开仓')
                self.sell()
        else:
            if self.position.size > 0 and self.indicators.bbw[0] < self.indicators.bbwsma[0] and self.indicators.curve[
                0] < 0:
                self.log(
                    f'bbw {self.indicators.bbw[0]} 下穿 bbwsma {self.indicators.bbwsma[0]} 且 curve {self.indicators.curve[0]} 小于 0 执行卖出平仓')
                self.sell()
            elif self.position.size < 0 and self.indicators.bbw[0] > self.indicators.bbwsma[0] and self.indicators.curve[0] > 0:
                self.log(
                    f'bbw {self.indicators.bbw[0]} 上穿 bbwsma {self.indicators.bbwsma[0]} 且 curve {self.indicators.curve[0]} 大于 0 执行买入平仓')
                self.buy()
