import os
import sys
import json
from multiprocessing import Process, Queue
from logbook.queues import ZeroMQSubscriber
from logbook import StreamHandler

# from .futuresclient import FuturesClient
from .easyctp import EasyctpClient


class ClientEngine(object):
    """
    客户端引擎，跟服务端引擎类似
    """

    def __init__(self, confPath):
        """

        :param confPath: 配置文件的路径
        """

        conf = os.path.join(confPath, 'client.json')
        with open(conf, 'r') as f:
            conf = json.load(f)
            self.conf = conf

        # 子进程通信队列
        self.q = Queue()

        # 订阅子进程中的屏幕日志
        for conf in self.conf.values():
            subscriber = ZeroMQSubscriber('tcp://{}'.format(conf["logzmqhost"]))
            self.streamHandler = StreamHandler(sys.stdout, level="DEBUG")
            subscriber.dispatch_in_background(self.streamHandler)

        # 期货行情
        conf = self.conf["easyctp"]
        self.easyctpProcess = Process(target=EasyctpClient.process, args=[self.q], kwargs=conf)

    def start(self):
        """
        启动所有服务
        :return:
        """
        self.easyctpProcess.start()


if __name__ == "__main__":
    ClientEngine("./conf").start()
