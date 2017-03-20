"""
模拟数据源，用以向客户端插入索引
"""
import logging
import os
import json
from queue import Queue, Empty
from threading import Thread

import zmq


class BaseDataResource(object):
    """
    数据源的基类
    """

    name = None

    def __init__(self, confPath):
        """

        """
        # 创建 Logger
        self.log = logging.getLogger("Inserter")

        # 输出到命令行的句柄
        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)

        # 日志内容格式
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        ch.setFormatter(formatter)

        # 给 Logger 添加句柄
        self.log.addHandler(ch)
        if __debug__:
            # 调试状态下的输出等级
            ch.setLevel(logging.DEBUG)
            self.log.setLevel(logging.DEBUG)

        # confPath 应该为一个文件夹路径而非文件
        if not os.path.isdir(confPath):
            raise ValueError("confPath:{} should be a path but not file.".format(confPath))

        conf = os.path.join(confPath, 'client.json')

        if not os.path.exists(conf):
            raise FileExistsError("{} not exists." % conf)

        with open(conf, 'r') as f:
            # 获取对应的配置
            conf = json.load(f)[self.name]

        # 待插入的时间序列队列
        self.queue = Queue()

        # 开始启动
        self.__active = False

        # 建立插入数据的 zmq
        self.__insertAddress = conf["insertAddress"]
        self.__context = zmq.Context()
        self.__socketREQ = self.__context.socket(zmq.REQ)  # 请求发出socket

        # 并发的时间序列插入线程
        self.__thread = Thread(name="{}Inserter", target=self.run)

    def start(self):
        # ZMQ 链接建立
        self.__socketREQ.connect(self.__insertAddress)
        # 启动向客户端插入时间序列的循环
        self.__active = True
        if not self.__thread.isAlive():
            self.__thread.start()

    def stop(self):
        self.__active = False
        if self.__thread.isAlive():
            self.__thread.join()

        try:
            # 清理完队列中的时间序列
            while True:
                self._run()
        except Empty:
            pass

    def run(self):
        while self.__active:
            self._run()

    def _run(self):
        # 时间序列汇报
        if __debug__: self.log.debug("等待时间序列……")
        index = self.queue.get()

        if __debug__: self.log.debug("得到时间索引: {}".format(index))

        self.__socketREQ.send_string(index)
        if __debug__: self.log.debug("时间序列汇报")

        repb = self.__socketREQ.recv_string()
        if __debug__: self.log.debug("收到回应")
        if __debug__: self.log.debug(str(repb))

    def putIndex(self, index):
        """
        向队列中推入时间索引，时间索引格式：


        :param index:
        :return:
        """

        index = json.dumps(index)

        if __debug__: self.log.debug("时间序列插入%s" % index)
        self.queue.put_nowait(index)

        if __debug__: self.log.debug("插入完成")
