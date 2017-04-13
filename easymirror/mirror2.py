# encoding: UTF-8
import json
import logging
import socket
import threading
import time
from queue import Queue

try:
    import cPickle as pickle
except ImportError:
    import pickle

import redis

from .timestampecache import TimestampeCache
from .pubtickerindex import PubTickerIndex


class Mirror(object):
    """
    数据对其服务的基类
    使用该接口，将会生成一个子进程，用于汇报Ticker时间戳和对齐Ticker数据
    """
    NAME = None
    ASK_CHANNEL_MODLE = 'ask:{}:{}'
    RECEIVE_CHANNEL_MODLE = 'receive:{}:{}'

    def __init__(self, conf, queue):
        self.queue = queue
        with open(conf, 'r') as f:
            conf = json.load(f)
        self.conf = conf[self.name]
        # 初始化日志
        self._initLog()

        # 本地主机名，同时也是在Server-Redis上的标志，不能存在相同的主机名，尤其在使用Docker部署时注意重名
        self.localhostname = self.conf['localhostname'] or socket.gethostname()
        if __debug__:
            self.localhostname = str(time.time())[-3:]

        self.log.info('localhostname {}'.format(self.localhostname))

        redisConf = conf["redis"]

        self.redis = redis.StrictRedis(
            **redisConf
        )
        # TODO 检查Redis上的链接配置

        # 请求对齐用到的两个队列
        self.askchannel = self.ASK_CHANNEL_MODLE.format(self.name, self.localhostname)

        self.receivechannel = self.RECEIVE_CHANNEL_MODLE.format(self.name, self.localhostname)
        # self.askQueue = RedisQueue(self.name, client=self.redis)

        # 订阅得到时间戳
        self.subTickerQueue = Queue()
        # 接受到请求对齐的队列
        self.revAskQueue = Queue()
        # 接受到补齐的数据队列
        self.makeupTickerQueue = Queue()

        # 循环逻辑
        self.__active = False
        self.service = [
            threading.Thread(target=self.pubTickerIndex, name=self.pubTickerIndex.__name__),
            threading.Thread(target=self.subTickerIndex, name=self.subTickerIndex.__name__),
            threading.Thread(target=self.handlerSubTicker, name=self.handlerSubTicker.__name__),
            threading.Thread(target=self.recAsk, name=self.recAsk.__name__),
            threading.Thread(target=self.handlerAsk, name=self.handlerAsk.__name__),
            # threading.Thread(target=self.recvTicker, name=self.recvTicker.__name__),
            # threading.Thread(target=self.makeup, name=self.makeup.__name__),
        ]

        # 忽略的主机名，一般在订阅行情中忽略自己发布的行情
        self.filterHostnames = {self.localhostname, }

        # 索引的缓存，用来对比是否缺失数据
        self.tCache = TimestampeCache()

        self.counts = [0, 0, 0, 0, 0]

    def indexLike(self):
        """
        对齐索引的格式
        :return:
        """
        raise NotImplemented()

    @property
    def name(self):
        return self.NAME

    @property
    def timename(self):
        """
        时间戳的key
        默认一般是  'time'
        :return: str
        """
        raise NotImplemented()

    @property
    def itemname(self):
        """
        品种名的key
        股票一般是 code, 期货是 symbol
        :return:
        """
        raise NotImplemented()

    @property
    def keys(self):
        """
        ticker 数据中所有的 key

        :return: [str(key1), str(key2), ...]
        """
        raise NotImplemented()

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
            self.log.debug = print

            self.log.setLevel("DEBUG")
            # sh.setLevel("DEBUG")
            self.log.debug("初始化日志完成")

    def start(self):
        """

        :return:
        """
        self.__active = True
        for s in self.service:
            if not s.isAlive():
                self.log.info('{} 服务开始'.format(s.name))
                s.start()

                # self.run()

    def stop(self):
        """

        :return:
        """
        self.__active = False
        self.log.info("服务结束")

        self.log.info('广播:{} 收:{} 索取:{} 被索取:{} 接受:{}'.format(*self.counts))

        for s in self.service:
            if s.isAlive():
                self.log.info('{} 服务结束中……'.format(s.name))
                s.join()
                self.log.info('{} 服务结束!'.format(s.name))

    @property
    def tickerchannel(self):
        return 'ticker:{}'.format(self.name)

    def subTickerIndex(self):
        self.log.info("开始订阅频道 {}".format(self.tickerchannel))
        sub = self.redis.pubsub()
        sub.subscribe(self.tickerchannel)
        while self.__active:
            self._subTickerIndex(sub)

    def _subTickerIndex(self, sub):
        """

        订阅时间戳

        :return:
        """
        msg = sub.get_message(ignore_subscribe_messages=True)

        if not msg:
            return

        if __debug__: self.log.debug("收到时间戳 {}".format(msg))

        # 堆入本地时间戳队列等待处理
        self.subTickerQueue.put(msg)

    def handlerSubTicker(self):
        """
        处理订阅得到的时间戳
        :return:
        """
        while self.__active:
            self._handlerSubTicker()

    def _handlerSubTicker(self):
        msg = self.subTickerQueue.get()
        channel = msg["channel"].decode('utf8')
        if self.tickerchannel != channel:
            # 不是ticker广播数据
            self.log.info("不是 ticker 广播数据 {} ".format(channel))
            return

        # 格式化
        index = self.unpackage(msg["data"])

        # 忽略黑名单
        if index.get("hostname") in self.filterHostnames:
            return

        self.counts[1] = self.counts[1] + 1

        # 检查数据是否缺失
        if self.tCache.isHave(index[self.timename], index[self.itemname]):
            return

        # 请求对齐数据
        # index 格式见 indexLike
        self.ask(index)

    def pubTickerIndex(self):
        self.log.info("开始发布")
        while self.__active:
            self._pubTickerIndex()

    def _pubTickerIndex(self):
        """
        汇报Ticker时间戳

        :return:
        """

        if __debug__: self.log.debug("等待Ticker数据")
        ticker = self.queue.get()

        if __debug__: self.log.debug("收到Ticker数据 {}".format(ticker))

        timestamp = self._stmptime(ticker)
        # 主机名
        timestamp["hostname"] = self.localhostname

        # 校验数据数据格式
        assert self.timename in timestamp

        # 缓存数据
        self.tCache.put(timestamp[self.timename], timestamp[self.itemname])

        self.counts[0] = self.counts[0] + 1

        timestamp = self.package(timestamp)
        self.redis.publish(self.tickerchannel, timestamp)

        if __debug__: self.log.debug("汇报时间戳 {}".format(timestamp))

    def _stmptime(self, ticker):
        """

        将 Ticker 数据转为索引

        :return: {'time': 123456, 'symbol': 'rb1710'}
        """
        raise NotImplemented()

    def handlerTickerIndex(self, msg):
        """

        处理订阅到的时间戳

        :param msg:
        :return:
        """
        raise NotImplemented()

    # def run(self):
    #     """
    #     主进程的操作
    #     :return:
    #     """
    #     while self.__active:
    #         pass



    def recAsk(self):
        self.log.info('listen ask : {}'.format(self.askchannel))
        # 阻塞，获取请求对齐
        # ask = self.askQueue.get()
        while self.__active:
            msg = self.redis.blpop(self.askchannel, timeout=5)
            if not msg:
                return

            self.revAskQueue.put(msg)

    def handlerAsk(self):
        """
        处理收到的请求对齐
        :return:
        """
        self.log.info('begin to handler ask ...')
        while self.__active:
            msg = self.revAskQueue.get()
            self.counts[3] = self.counts[3] + 1
            ask = msg[1]
            ask = self.unpackage(ask)
            # 查询本地的 Ticker 数据
            ticker = self.getTickerByAsk(ask)
            # 返回 Ticker 数据
            if ticker:
                self._donator(ask, ticker)

    def ask(self, index):
        """
        发起请求对齐
        :return:
        """

        ask = index.copy()
        ask['hostname'] = self.localhostname
        ask = self.getAskMsg(ask)

        if __debug__:
            self.log.debug("发起请求 {}".format(ask))
        ask = self.package(ask)
        # 对方频道
        channel = self.ASK_CHANNEL_MODLE.format(self.name, index['hostname'])
        self.counts[2] = self.counts[2] + 1
        self.redis.rpush(channel, ask)

        # self.askQueue.put(ask)

    def getAskMsg(self, index):
        """

        :param index:
        :return:
        """
        raise NotImplemented()

    def getTickerByAsk(self, ask):
        """

        :param ask:
        :return:
        """
        raise NotImplemented()

    def package(self, data):
        """
        将数据打包
        :return:
        """
        # return json.dumps(data)
        return pickle.dumps(data)

    def unpackage(self, data):
        """

        :param data:
        :return:
        """
        # return json.loads(data)
        return pickle.loads(data)

    def _donator(self, ask, ticker):
        """

        响应对齐

        :param ask:
        :param ticker:
        :return:
        """

        hostname = ask['hostname']
        # 将 ticker 数据堆入补齐数据队列中
        channel = self.RECEIVE_CHANNEL_MODLE.format(self.name, hostname)
        self.redis.rpush(channel, self.package(ticker))

    def recvTicker(self):
        """
        接受补齐的数据
        :return:
        """
        self.log.info('recvTicker start ...')
        while self.__active:
            msg = self.redis.blpop(self.receivechannel, timeout=5)
            if not msg:
                return
            self.makeupTickerQueue.put(msg)

    def makeup(self):
        self.log.info('makeup start ...')
        while self.__active:
            msg = self.makeupTickerQueue.get()
            self.counts[4] = self.counts[4] + 1
            ticker = msg[1]
            ask = self.unpackage(ticker)
            self.makeupTicker(ask)

    def makeupTicker(self, ticker):
        """
        将补齐的ticker 数据保存到数据库中
        :param ticker:
        :return:
        """
        raise NotImplemented()

    def loadToday(self):
        """
        加载今天交易日的ticker数据并生成缓存
        :return:
        """
        raise NotImplemented()
