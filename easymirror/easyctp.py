"""

"""

from .client import BaseClient
from .baseapi import BaseApi
from .server import BaseServer


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
