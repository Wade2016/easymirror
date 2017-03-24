"""

"""

from .client import BaseClient
from .baseapi import BaseApi
from .server import BaseServer
from .mirror import Mirror


class EasyctpServer(BaseServer):
    """RPC服务器"""


class EasyctpClient(BaseClient):
    """期货行情客户端"""

    # ----------------------------------------------------------------------
    def callback(self, topic, data):
        """回调函数"""
        print("回调函数")
        print(data)


class EasyctpApi(BaseApi):
    """
    供 easyctp 使用的 api
    """
    name = "easyctp"


class Easymirror(Mirror):
    """
    镜像服务
    """
    NAME = "easyctp"

    def _stmptime(self, ticker):
        """

        将 Ticker 数据转为时间戳

        :return:
        """

        return {
            "time": ticker["time"],
            "instrument_id": ticker["time"],
        }
