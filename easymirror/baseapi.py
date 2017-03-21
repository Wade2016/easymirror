"""
模拟数据源，用以向客户端插入索引
"""
import logging
import os
import traceback
import json
from queue import Queue, Empty, Full
from threading import Thread

import zmq


class BaseApi(object):
    """
    外部接口的基类
    """

    name = None

    def __init__(self, confPath):
        """

        """
        # 创建 Logger
        self.log = logging.getLogger("TickerPUB")

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
        self.queue = Queue(1000)

        # 开始启动
        self.__tickerPUBActive = False

        # ticker 数据广播的逻辑
        self.__tickerAddress = conf["tickerAddress"]
        self.__context = zmq.Context()
        self.__socketPUBTicker = self.__context.socket(zmq.PUB)  # 数据广播socket
        self.__socketPUBTicker.bind(self.__tickerAddress)

        # 并发的时间序列插入线程
        self.__thread = Thread(name="{}TickerPUB".format(self.name, ), target=self.run)

    def start(self):
        # ZMQ 链接建立
        # self.__socketREQ.connect(self.__insertAddress)

        # 启动广播循环
        self.__tickerPUBActive = True
        if not self.__thread.isAlive():
            self.__thread.start()

    def stop(self):
        self.__tickerPUBActive = False
        if self.__thread.isAlive():
            self.__thread.join()

        try:
            # 清理完队列中的时间序列
            while True:
                self._run()
        except Empty:
            pass

    def run(self):
        while self.__tickerPUBActive:
            try:
                self._run()
            except:
                self.log.error(traceback.format_exc())

    def _run(self):
        # 时间序列汇报
        if __debug__: self.log.debug("等待Ticker数据……")
        ticker = self.queue.get()

        if __debug__: self.log.debug("得到 Ticker : {}".format(ticker))

        self.__socketPUBTicker.send_json(ticker)
        if __debug__: self.log.debug("广播 Ticker 完成")

    def brocastcastTicker(self, ticker):
        """

        外部调用，对 ticker 数据进行广播

        :param ticker:
        :return:
        """
        try:
            if __debug__: self.log.debug("接口广播ticker%s" % ticker)
            self.queue.put_nowait(ticker)
            if __debug__:
                self.log.debug("插入完成")
        except Full:
            self.log.warning("easymirror pull is Full!")

