import backtrader as bt
import importlib
import inspect
import pkgutil


class StrategyManager:
    def __init__(self):
        self.strategies = []
        self.load_strategies()

    def load_strategies(self):
        package_name = 'strategy'
        package = importlib.import_module(package_name)

        # 遍历策略包中的所有模块
        for loader, module_name, is_pkg in pkgutil.iter_modules(package.__path__, package_name + '.'):
            if not is_pkg:
                # 导入模块
                module = importlib.import_module(module_name)
                # 获取模块中的所有类
                for name, obj in inspect.getmembers(module, inspect.isclass):
                    if issubclass(obj, bt.Strategy) and obj is not bt.Strategy:
                        self.strategies.append(obj)
