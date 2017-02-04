# encoding: utf-8

from multiprocessing import Process, Queue

from easymirror.client import runClient


class ClientEngine(object):
    """
    服务端引擎，用于启动多个服务类型的子进程
    """

    def __init__(self):
        """

        """
        # 子进程通信队列
        self.q = Queue()

        self.p = Process(target=runClient, args=[self.q, ])

    def start(self):
        """

        :return:
        """
        self.p.start()


if __name__ == "__main__":
    ClientEngine().start()
