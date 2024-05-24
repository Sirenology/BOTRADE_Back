import backtrader as bt


# =================== 指标部分 ===================
class Indicators(bt.Indicator):
    # 定义指标参数
    params = (
        ('length', 10),  # CCI指标长度

        ('multiplier', 3),  # ATR指标倍数
        ('atr_length', 10)  # ATR指标长度

    )

    # 定义指标线
    lines = ('cci', 'super_trend_up', 'super_trend_dn', 'up', 'dn', 'trend')

    # 初始化指标
    def __init__(self):
        self.l.cci = bt.talib.CCI(self.data.high, self.data.low, self.data.close, timeperiod=self.params.length)
        hlc3 = (self.data.high + self.data.close + self.data.low) / 3
        atr = bt.talib.ATR(self.data.high, self.data.low, self.data.close, timeperiod=self.params.atr_length)
        self.l.up = hlc3 - (atr * self.params.multiplier)
        self.l.dn = hlc3 + (atr * self.params.multiplier)

    def next(self):
        INVALID_VALUE = -99999
        if len(self.data) > 20:  # 检查是否不是第一个数据点
            if self.l.trend[-1] == -1 and self.data.close[0] > self.l.super_trend_dn[-1]:
                self.l.trend[0] = 1
            elif self.l.trend[-1] == 1 and self.data.close[0] < self.l.super_trend_up[-1]:
                self.l.trend[0] = -1
            else:
                self.l.trend[0] = self.l.trend[-1]

            if self.l.trend[0] == 1:
                if self.data.close[-1] < self.l.super_trend_up[-1]:
                    self.l.super_trend_up[0] = self.l.up[0]
                else:
                    self.l.super_trend_up[0] = max(self.l.super_trend_up[-1], self.l.up[0])
                self.l.super_trend_dn[0] = INVALID_VALUE
            elif self.l.trend[0] == -1:
                if self.data.close[-1] > self.l.super_trend_dn[-1]:
                    self.l.super_trend_dn[0] = self.l.dn[0]
                else:
                    self.l.super_trend_dn[0] = min(self.l.super_trend_dn[-1], self.l.dn[0])
                self.l.super_trend_up[0] = INVALID_VALUE
        else:
            self.l.trend[0] = 1
            self.l.super_trend_up[0] = self.l.up[0]
            self.l.super_trend_dn[0] = INVALID_VALUE


# =================== 策略部分 ===================
class TrendCCIStrategy(bt.Strategy):

    def __init__(self):
        # 记录买入卖出信号
        self.sell_signal = []
        self.buy_signal = []

        # 初始化字典来存储指标数据
        self.indicator_data = {
            'Date': [],
            'cci': [],
            'super_trend_up': [],
            'super_trend_dn': []
        }

        # 记录交易历史
        self.trade_history = []
        self.tradeId = 1

        self.indicators = Indicators()

        self.close = self.data.close

    # 记录回测日志
    def log(self, txt):
        print(f"{self.data.datetime.datetime()} {txt}")

    # 记录指标数据
    def record_indicators(self):
        self.indicator_data['Date'].append(self.data.datetime.datetime())
        self.indicator_data['cci'].append(self.indicators.cci[0])
        self.indicator_data['super_trend_up'].append(self.indicators.super_trend_up[0])
        self.indicator_data['super_trend_dn'].append(self.indicators.super_trend_dn[0])

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

    # 策略主逻辑
    def next(self):
        if not self.position:
            if (self.indicators.trend[0] == 1 and self.data.close[0] > self.indicators.super_trend_up[0] and
                    self.indicators.cci[0] > 100):
                self.buy()
            elif (self.indicators.trend[0] == -1 and self.data.close[0] < self.indicators.super_trend_dn[0] and
                    self.indicators.cci[0] < -100):
                self.sell()
        else:
            # 获取当前持仓方向
            if self.position.size > 0:
                # 当上涨大于等于 2% 时止盈
                if self.data.close[0] >= self.position.price * 1.03:
                    self.sell()
                if self.data.close[0] <= self.position.price * 0.98:
                    self.sell()
            else:
                # 当下跌大于等于 2% 时止盈
                if self.data.close[0] <= self.position.price * 0.97:
                    self.buy()
                if self.data.close[0] >= self.position.price * 1.02:
                    self.buy()

        # 记录指标数据
        self.record_indicators()
