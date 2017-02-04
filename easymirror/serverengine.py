import os
import json
from multiprocessing import Process, Queue

from .futuresserver import FuturesServer


class ServerEngine(object):
    """
    服务端引擎，用于启动多个服务类型的子进程
    """

    def __init__(self, confPath):
        """

        """
        conf = os.path.join(confPath, 'server.json')
        with open(conf, 'r') as f:
            self.conf = json.load(f)

        # 子进程通信队列
        self.q = Queue()

        # 期货行情
        conf = self.conf["futures"]
        self.futuresProcess = Process(target=FuturesServer.process, args=[self.q], kwargs=conf)

    def start(self):
        """
        启动所有服务
        :return:
        """
        self.futuresProcess.start()


if __name__ == "__main__":
    ServerEngine("./conf").start()
