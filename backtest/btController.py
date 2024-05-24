from flask import Flask, request, jsonify, Blueprint
from flask_cors import CORS
from utils.responseUtil import ResponseUtil
from btManager import BackTestManager
from datetime import datetime as dt
import logging
import strategy

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

backtest_blueprint = Blueprint('backtest', __name__, url_prefix='/backtest')


# 执行回测
@backtest_blueprint.route('/getBackTestInfo', methods=['POST'])
def executeBackTest():
    logging.info('executeBackTest 执行回测')
    response = request.json

    print('startDate', response['startDate'], 'endDate', response['endDate'])
    # 处理请求参数
    symbol = response['symbol']
    strategy = response['strategy']
    startDate = dt.strptime(response['startDate'], "%Y-%m-%d %H:%M:%S")
    endDate = dt.strptime(response['endDate'], "%Y-%m-%d %H:%M:%S")
    interval = response['interval']

    logging.info(
        f'executeBackTest 品种: {symbol} 策略: {strategy} 开始时间: {startDate} 结束时间: {endDate} 周期: {interval}')

    try:
        # 执行回测
        backTestManager = BackTestManager(symbol, interval)
        backtestInfo = backTestManager.executeByDataBase(strategy, startDate, endDate)
    except Exception as e:
        logging.error(f"executeBackTest 执行回测失败: {e}")
        return ResponseUtil.error(errors=f"executeBackTest 执行回测失败: {e}")

    return ResponseUtil.success(data=backtestInfo)


# # 获取回测信息
# @app.route('/backtest/getBackTestInfo', methods=['GET'])
# def getBackTestInfo():
#     pass

app = Flask(__name__)

app.register_blueprint(backtest_blueprint)

CORS(app)

if __name__ == '__main__':
    app.run(debug=True, port=3330)
