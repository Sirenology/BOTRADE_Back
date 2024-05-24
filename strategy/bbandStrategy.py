import backtrader as bt


# =================== 指标部分 ===================
class Indicators(bt.Indicator):
    # 定义指标参数
    params = (
        ('length', 19),  # 窄幅布林带长度 宽幅布林带长度 布林带宽度指标长度

        ('narrow_std', 2.3),  # 窄幅布林带标准差 布林带宽度标准差
        ('wide_std', 3.55),  # 宽幅布林带标准差
        ('BBW_pd', 0.01)  # 布林带宽度指标阈值
    )

    # 定义指标线
    lines = ('na_up_bband', 'na_mid_bband', 'na_low_bband', 'wi_up_bband', 'wi_low_bband')

    # 初始化指标
    def __init__(self):
        bbands_na = bt.talib.BBANDS(self.data.close, timeperiod=self.params.length, nbdevup=self.params.narrow_std,
                                    nbdevdn=self.params.narrow_std, matype=0)
        self.l.na_up_bband = bbands_na.lines[0]
        self.l.na_mid_bband = bbands_na.lines[1]
        self.l.na_low_bband = bbands_na.lines[2]

        bbands_wi = bt.talib.BBANDS(self.data.close, timeperiod=self.params.length, nbdevup=self.params.wide_std,
                                    nbdevdn=self.params.wide_std, matype=0)
        self.l.wi_up_bband = bbands_wi.lines[0]
        self.l.wi_low_bband = bbands_wi.lines[2]

        self.l.bbw = (self.l.na_up_bband - self.l.na_low_bband) / self.l.na_mid_bband


# =================== 策略部分 ===================
class BBandStrategy(bt.Strategy):

    def __init__(self):
        # 记录买入卖出信号
        self.sell_signal = []
        self.buy_signal = []

        # 初始化字典来存储指标数据
        self.indicator_data = {
            'Date': [],
            'na_up_bband': [],
            'na_mid_bband': [],
            'na_low_bband': [],
            'wi_up_bband': [],
            'wi_low_bband': []
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
        self.indicator_data['na_up_bband'].append(self.indicators.na_up_bband[0])
        self.indicator_data['na_mid_bband'].append(self.indicators.na_mid_bband[0])
        self.indicator_data['na_low_bband'].append(self.indicators.na_low_bband[0])
        self.indicator_data['wi_up_bband'].append(self.indicators.wi_up_bband[0])
        self.indicator_data['wi_low_bband'].append(self.indicators.wi_low_bband[0])

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
            if self.data.close[0] > self.indicators.na_up_bband[0]:
                self.buy()
            elif self.data.close[0] < self.indicators.na_low_bband[0]:
                self.sell()
        else:
            # 获取当前持仓方向
            if self.position.size > 0:
                if self.data.close[0] > self.indicators.wi_up_bband[0]:
                    self.sell()
                elif self.data.close[0] < self.indicators.na_mid_bband[0]:
                    self.sell()
            else:
                if self.data.close[0] < self.indicators.wi_low_bband[0]:
                    self.buy()
                elif self.data.close[0] > self.indicators.na_mid_bband[0]:
                    self.buy()

        # 记录指标数据
        self.record_indicators()
