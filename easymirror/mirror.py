import json
import logging
import socket
import threading
import time

import redis


# 猴子补丁


class Mirror(object):
    """
    数据对其服务的基类
    使用该接口，将会生成一个子进程，用于汇报Ticker时间戳和对齐Ticker数据
    """
    NAME = None

    @property
    def name(self):
        return self.NAME

    def __init__(self, conf, queue):
        self.queue = queue
        with open(conf, 'r') as f:
            conf = json.load(f)
        self.conf = conf["easyctp"]
        # 初始化日志
        self._initLog()

        # 本地主机名，同时也是在Server-Redis上的标志，不能存在相同的主机名，尤其在使用Docker部署时注意重名
        self.localhostname = self.conf or socket.gethostname()

        redisConf = conf["redis"]

        self.redis = redis.StrictRedis(
            **redisConf
        )
        # TODO 检查Redis上的链接配置

        # 循环逻辑
        self.__active = False
        self.service = [
            threading.Thread(target=self.pubTickerIndex),
            threading.Thread(target=self.subTickerIndex),
        ]

    def _initLog(self):
        """
        初始化日志
        :return:
        """
        self.log = logging.getLogger(self.name)
        self.log.setLevel("INFO")

        sh = logging.FileHandler(self.conf["log"])
        sh.setLevel("INFO")
        sh.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
        self.log.addHandler(sh)

        if __debug__:
            self.log.setLevel("INFO")
            sh.setLevel("INFO")
            self.log.debug("初始化日志完成")

    def start(self):
        """

        :return:
        """
        self.__active = True
        for s in self.service:
            if not s.isAlive():
                s.start()

    def stop(self):
        """

        :return:
        """
        self.__active = False
        self.log.info("服务结束")

        for s in self.service:
            if s.isAlive():
                s.join()

    def subTickerIndex(self):
        self.log.info("开始订阅")
        sub = self.redis.pubsub()
        sub.subscribe(self.name)
        while self.__active:
            self._subTickerIndex(sub)

    def _subTickerIndex(self, sub):
        """

        订阅时间戳

        :return:
        """
        if __debug__: self.log.debug("订阅中")
        timestamp = sub.parse_response()
        if __debug__: self.log.debug("收到时间戳 {}".format(timestamp))

    def pubTickerIndex(self):
        self.log.info("开始发布")
        while self.__active:
            self._pubTickerIndex()

    def _pubTickerIndex(self):
        """
        汇报Ticker时间戳

        :return:
        """

        if __debug__: self.log.debug("等待时间戳")
        ticker = self.queue.get()
        timestamp = self._stmptime(json.loads(ticker))
        # 主机名
        timestamp["hostname"] = self.localhostname
        self.redis.publish(self.name, json.dumps(timestamp))
        if __debug__: self.log.debug("汇报时间戳")

    def _stmptime(self, ticker):
        """

        将 Ticker 数据转为时间戳

        :return: index
        """
        raise NotImplementedError()
