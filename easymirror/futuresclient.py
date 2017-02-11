from .client import BaseClient


class FuturesClient(BaseClient):
    """期货行情客户端"""

    # ----------------------------------------------------------------------
    def callback(self, topic, data):
        """回调函数"""
        print("回调函数")
        print(data)
