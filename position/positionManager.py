import backoff

from exchange.okxManager import OkxManager
from database.dbManager import DatabaseManager
import logging

"""
仓位管理类


初始化参数(OKX交易所)：
symbol: 交易对 (OKX BTC)

init_capital: 本金

size: 仓位大小(委托数量 当交割、永续、期权买入和卖出时，指合约张数) 

U本位 单张合约USDT值(min_value_usdt) = 该币种当前开仓价格(close) * 单张合约币值(min_value_coin)
size(合约张数) = (本金(init_capital) * 2%) / 单张合约USDT值 * 杠杆倍数 (leverage)
order_pct(下单金额占本金百分比) 默认为2%

side: 订单方向 (buy 买/sell 卖)
pos_side: 仓位方向(开平仓模式下必填) (long 多/short 空)

ord_type: 订单类型 (market 市价/limit 限价)
tdMode: 交易模式 (cross 全仓/Isolated 逐仓)

tp_sl_update_func: 移动止盈止损方法
tp_sl_update_args: 移动止盈止损方法参数

closed: 仓位是否已平仓

仓位管理:

仓位大小 = (本金 * 2%) * 杠杆  任何一笔交易最大亏损小于等于本金的2%
总亏损 = 本金 * 20% 所有交易总亏损小于等于本金的20%

止盈止损:
按 价位 设置止盈止损
"""

logger = logging.getLogger(__name__)


class PositionManager:
    _id_counter = 1  # 仓位编号

    def __init__(self, symbol=None, side=None, pos_side=None, live_mode=False,
                 size=None,
                 sl_fixed=None,
                 tp_fixed=None,
                 tp_sl_update_func=None,
                 tp_sl_update_args=None):

        self.order_id = PositionManager._id_counter
        PositionManager._id_counter += 1  # 创建新实例时增加计数器
        print(f"当前仓位编号 {self.order_id}")

        if not symbol:
            raise ValueError("symbol不能为空")
        self.symbol = symbol

        if not side:
            raise ValueError("side不能为空")
        elif side not in ['buy', 'sell']:
            raise ValueError("side必须为'buy'或'sell'")
        self.side = side

        if not pos_side:
            raise ValueError("pos_side不能为空")
        if pos_side not in ['long', 'short']:
            raise ValueError("pos_side必须为'long'或'short'")
        self.pos_side = pos_side

        # # 固盈固损
        # if sl_fixed and tp_fixed:
        #     if sl_fixed >= tp_fixed:
        #         raise ValueError("stop_loss必须小于take_profit")
        #     else:
        #         self.stop_loss = sl_fixed
        #         self.take_profit = tp_fixed
        # # 非固盈非固损
        # elif sl_fixed is None and tp_fixed is None:
        #     self.tp_sl_update()
        # # 固盈非固损
        # elif tp_fixed:
        #     self.tp_sl_update()
        # else:
        #     raise ValueError("止盈止损模式不存在")

        self.okx_manager = OkxManager(symbol=self.symbol, live_mode=live_mode)
        self.size = size
        self.curr_balance = self.okx_manager.get_account_balance_info()
        self.is_closed = False
        self.td_mode = 'isolated'
        self.leverage = None
        self.min_value_coin_list = {
            'BTC': 0.001,
            'ETH': 0.01,
            'XRP': 100,
            'SOL': 0.01,
            'LTC': 0.001,
            'EOS': 10,
            'BCH': 0.01,
            'ETC': 10,
        }
        self.sl_fixed = sl_fixed,
        self.tp_fixed = tp_fixed,
        self.tp_sl_update_func = tp_sl_update_func
        self.tp_sl_update_args = tp_sl_update_args

    # 移动止盈止损
    def tp_sl_update(self):
        if self.tp_sl_update_func:
            if self.tp_sl_update_args:
                raise ValueError("移动盈损 需要传递tp_sl_update_args参数")
            self.tp_sl_update_func(self, self.tp_sl_update_args)
        else:
            raise ValueError("移动盈损 需要传递tp_sl_update_func方法")

    # 执行开仓
    @backoff.on_exception(backoff.expo, Exception, max_time=60)
    def open_position(self, curr_price=None,
                      order_pct=0.02, ord_type='market', td_mode='isolated', leverage=3):

        init_capital = self.curr_balance

        # 设置杠杠
        self.set_leverage(leverage, td_mode)

        if not ord_type == 'market':
            raise ValueError("ord_type必须为'market'")

        if td_mode not in ['cross', 'isolated']:
            raise ValueError("td_mode必须为'cross'或'isolated'")

        validate_positive(curr_price, "curr_price")
        validate_positive(init_capital, "init_capital")
        validate_positive(order_pct, "order_pct")

        min_value_coin = self.min_value_coin_list[self.symbol]
        min_value_usdt = min_value_coin * curr_price / leverage
        allocated_usdt = init_capital * order_pct

        if min_value_usdt > allocated_usdt:
            raise ValueError("本金不足以开仓")

        self.size = allocated_usdt / min_value_usdt
        self.td_mode = td_mode

        try:
            self.okx_manager.okx_exchange.create_order(
                symbol=self.okx_manager.instId,
                type=ord_type,
                side=self.side,
                amount=self.size,
                params={'tdMode': td_mode,
                        'positionSide': self.pos_side,
                        'clOrdId': self.order_id})

            order_info = self.get_trade_order()
            db_manager = DatabaseManager(mysql_db='QuantCoinHub', mysql_table='OpenOrds', table_type='ords',
                                         use_type='write')
            db_manager.insert_table(order_info)

        except Exception as e:
            logging.error(
                f"开仓 第 {self.order_id} 单 失败:{e}")
            raise

    # 执行平仓
    @backoff.on_exception(backoff.expo, Exception, max_time=60)
    def close_position(self):
        try:
            self.okx_manager.okx_exchange.privatePostTradeClosePosition(
                params={
                    'posSide': self.pos_side,
                    'instId': self.okx_manager.instId,
                    'clOrdId': self.order_id,
                    "mgnMode": self.td_mode,
                })
            print(
                f"成功 平仓 第 {self.order_id} 单 "
                f"订单id: {self.order_id} "
                f"数量: {self.size} 张 "
                f"仓位方向: {self.pos_side}"
            )

            db_manager = DatabaseManager(mysql_db='QuantCoinHub', mysql_table='OpenOrds', table_type='ords',
                                         use_type='write')
            query = f"DELETE FROM OpenOrds WHERE clOrdId = %s"
            db_manager.cursor.execute(query, (self.order_id,))
            db_manager.connect.commit()
            self.is_closed = True
        except Exception as e:
            logging.error(f"平仓 第 {self.order_id} 单 失败:{e}")
            raise

        PositionManager._id_counter -= 1  # 平仓时减少计数器

    # 设置杠杆
    @backoff.on_exception(backoff.expo, Exception, max_time=60)
    def set_leverage(self, leverage, td_mode):
        try:
            self.okx_manager.okx_exchange.set_leverage(symbol=self.okx_manager.instId, leverage=leverage,
                                                       params={'posSide': self.pos_side, 'mgnMode': td_mode})
            self.leverage = leverage
            print(f"成功设置第 {self.order_id} 单 杠杆倍数为 {leverage}")
        except Exception as e:
            logging.error(f"设置第 {self.order_id} 单杠杆倍数失败:{e}")
            raise

    # 获取当前仓位信息
    @backoff.on_exception(backoff.expo, Exception, max_time=60)
    def get_trade_order(self):
        try:
            trade_order = self.okx_manager.okx_exchange.private_get_trade_order(
                params={
                    'instId': self.okx_manager.instId,
                    'clOrdId': self.order_id})
            self.size = trade_order['data'][0]['sz']
            print(
                f"成功 获取 第 {self.order_id} 单 "
                f"交易对:{trade_order['data'][0]['instId']} "
                f"仓位方向:{trade_order['data'][0]['side']}/{trade_order['data'][0]['posSide']} "
                f"仓位数量:{trade_order['data'][0]['sz']} 张 "
                f"价格:{trade_order['data'][0]['avgPx']} "
                f"杠杆倍数:{self.leverage} "
            )
            order_info = {
                'trade_order': trade_order,
                'leverage': self.leverage,
            }
            return order_info
        except Exception as e:
            logging.error(f"获取第 {self.order_id} 单仓位信息失败:{e}")
            raise


# 检查值是否为正数
def validate_positive(value, name):
    """检查值是否为正数，如果不是，则抛出ValueError"""
    if not value:
        raise ValueError(f"{name}不能为空")
    elif value <= 0:
        raise ValueError(f"{name}大小必须为正数")
