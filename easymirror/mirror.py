# encoding: UTF-8
import json
import logging
import socket
import time
import asyncio
import sys
import contextlib
import traceback
import queue
from asyncio import sleep
import functools
import collections

try:
    import cPickle as pickle
except ImportError:
    import pickle
import redis
from .timestampecache import TimestampeCache


# from .pubtickerindex import PubTickerIndex

# mirror = None
#
# def logerr(func):
#     @functools.wraps(func)
#     def wrapper(*args, **kw):
#         try:
#             return func(*args, **kw)
#         except:
#             print(121212)
#             if __debug__:
#                 traceback.print_exc()
#             global mirror
#             mirror.log.error(traceback.format_exc())
#
#     return wrapper


class Mirror(object):
    """
    数据对其服务的基类
    使用该接口，将会生成一个子进程，用于汇报Ticker时间戳和对齐Ticker数据
    """
    NAME = None
    ASK_CHANNEL_MODLE = 'ask:{}:{}'
    RECEIVE_CHANNEL_MODLE = 'receive:{}:{}'

    def __init__(self, conf, queue):
        """

        :param conf: 配置文件的路径
        :param queue: 进程通信队列
        """
        global mirror
        mirror = self
        self.queue = queue

        with open(conf, 'r') as f:
            conf = json.load(f)
        self.conf = conf[self.name]
        # 初始化日志
        self.log = self._initLog()

        # 建立协程池
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        if __debug__:
            if sys.platform == 'win32':  # pragma: no cover
                assert isinstance(self.loop, asyncio.windows_events._WindowsSelectorEventLoop)
            else:
                assert isinstance(self.loop, asyncio.unix_events._UnixSelectorEventLoop)

        # 本地主机名，同时也是在Server-Redis上的标志，不能存在相同的主机名，尤其在使用Docker部署时注意重名
        self.localhostname = self.conf['localhostname'] or socket.gethostname()
        if __debug__:
            self.localhostname = str(time.time())[-3:]

        self.log.info('localhostname {}'.format(self.localhostname))

        redisConf = conf["redis"]

        self.redis = redis.StrictRedis(
            **redisConf
        )

        # 请求对齐用到的两个队列
        self.askchannel = self.ASK_CHANNEL_MODLE.format(self.name, self.localhostname)

        self.receivechannel = self.RECEIVE_CHANNEL_MODLE.format(self.name, self.localhostname)

        # 订阅得到时间戳
        self.subTickerQueue = asyncio.Queue()
        # 接受到请求对齐的队列
        self.revAskQueue = asyncio.Queue()
        # 接受到补齐的数据队列
        self.makeupTickerQueue = asyncio.Queue()

        # 循环逻辑
        self.__active = False

        self.services = [
            self.pubTickerIndex(),
            self.subTickerIndex(),
            self.handlerSubTicker(),
            self.recAsk(),
            self.handlerAsk(),
            self.recvTicker(),
            self.makeup(),
        ]

        # self.services = [
        # threading.Thread(target=self.pubTickerIndex, name=self.pubTickerIndex.__name__),
        # threading.Thread(target=self.subTickerIndex, name=self.subTickerIndex.__name__),
        # threading.Thread(target=self.handlerSubTicker, name=self.handlerSubTicker.__name__),
        # threading.Thread(target=self.recAsk, name=self.recAsk.__name__),
        # threading.Thread(target=self.handlerAsk, name=self.handlerAsk.__name__),
        # threading.Thread(target=self.recvTicker, name=self.recvTicker.__name__),
        # threading.Thread(target=self.makeup, name=self.makeup.__name__),
        # ]

        # 忽略的主机名，一般在订阅行情中忽略自己发布的行情
        self.filterHostnames = {self.localhostname, }

        # 索引的缓存，用来对比是否缺失数据
        self.tCache = TimestampeCache()

        self.counts = collections.OrderedDict(
            (('广播',  0), ('收听',  0), ('索取',  0), ('被索取',  0), ('补齐',  0))
        )

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
        log = logging.getLogger(self.name)
        log.setLevel("INFO")

        logFormatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

        fh = logging.FileHandler(self.conf["log"])
        fh.setLevel("INFO")
        fh.setFormatter(logFormatter)
        log.addHandler(fh)

        if __debug__:
            # self.log.debug = print
            # 屏幕输出
            sh = logging.StreamHandler(sys.stdout)
            sh.setFormatter(logFormatter)
            sh.setLevel('DEBUG')
            log.addHandler(sh)

            # log.setLevel("DEBUG")
            log.debug("初始化日志完成")
        return log

    def start(self):
        """

        :return:
        """
        self.__active = True
        self.loop.run_until_complete(
            asyncio.wait(self.services)
        )
        self.loop.close()

        # for s in self.services:
        #     assert asyncio.iscoroutine(s)
        #         self.log.info('{} 服务开始'.format(s.name))

        # self.run()

    def close(self):
        self.__active = False

        self.log.info("即将关闭服务……")

    #     self.loop.close()

    @property
    def tickerchannel(self):
        return 'ticker:{}'.format(self.name)

    async def subTickerIndex(self):
        self.log.info("开始订阅频道 {}".format(self.tickerchannel))

        with contextlib.closing(self.redis.pubsub()) as sub:
            sub.subscribe(self.tickerchannel)
            try:
                while self.__active:
                    msg = sub.get_message(ignore_subscribe_messages=True)
                    if msg is None:
                        await sleep(0)
                        continue

                    if __debug__: self.log.debug("收到时间戳 {}".format(msg))

                    # 堆入本地时间戳队列等待处理
                    await self.subTickerQueue.put(msg)
            except:
                if __debug__:
                    traceback.print_exc()
                self.log.error(traceback.format_exc())

    async def handlerSubTicker(self):
        """
        处理订阅得到的时间戳
        :return:
        """
        try:
            while self.__active:
                await self._handlerSubTicker()
        except:
            if __debug__:
                traceback.print_exc()
            self.log.error(traceback.format_exc())

    async def _handlerSubTicker(self):
        msg = await self.subTickerQueue.get()
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

        self.counts['收听'] = self.counts['收听'] + 1

        # 检查数据是否缺失
        if self.tCache.isHave(index[self.timename], index[self.itemname]):
            return

        # 请求对齐数据
        # index 格式见 indexLike
        self.ask(index)

    async def pubTickerIndex(self):
        self.log.info("开始发布")
        try:
            while self.__active:
                self._pubTickerIndex()
                await sleep(0)
        except:
            if __debug__:
                traceback.print_exc()
            self.log.error(traceback.format_exc())

    def _pubTickerIndex(self):

        try:
            # 进程的通信队列，不能用 await
            ticker = self.queue.get_nowait()
        except queue.Empty:
            return

        if __debug__: self.log.debug("收到Ticker数据 {}".format(ticker))

        timestamp = self._stmptime(ticker)
        # 主机名
        timestamp["hostname"] = self.localhostname

        # 校验数据数据格式
        assert self.timename in timestamp

        # 缓存数据
        self.tCache.put(timestamp[self.timename], timestamp[self.itemname])

        self.counts['广播'] = self.counts['广播'] + 1

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

    async def recAsk(self):
        self.log.info('listen ask : {}'.format(self.askchannel))
        try:
            while self.__active:
                # 非阻塞，获取请求对齐
                msg = self.redis.lpop(self.askchannel)
                if msg is None:
                    await sleep(0)
                    continue

                await self.revAskQueue.put(msg)
        except:
            if __debug__:
                traceback.print_exc()
            self.log.error(traceback.format_exc())

    async def handlerAsk(self):
        """
        处理收到的请求对齐
        :return:
        """
        self.log.info('begin to handler ask ...')
        try:
            while self.__active:
                # 非阻塞，获取请求对齐
                await self._handlerAsk()
        except:
            if __debug__:
                traceback.print_exc()
            self.log.error(traceback.format_exc())

    async def _handlerAsk(self):
        msg = await self.revAskQueue.get()
        self.counts['被索取'] = self.counts['被索取'] + 1
        ask = self.unpackage(msg)

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
        self.counts['索取'] = self.counts['索取'] + 1
        self.redis.rpush(channel, ask)

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

    async def recvTicker(self):
        """
        接受补齐的数据
        :return:
        """
        self.log.info('recvTicker start ...')
        try:
            while self.__active:
                # 非阻塞，获取请求对齐
                await self._recvTicker()
        except:
            if __debug__:
                traceback.print_exc()
            self.log.error(traceback.format_exc())

    async def _recvTicker(self):

        msg = self.redis.lpop(self.receivechannel)
        if msg is None:
            await sleep(0)
            return
        else:
            await self.makeupTickerQueue.put(msg)

    async def makeup(self):
        self.log.info('makeup start ...')
        try:
            while self.__active:
                # 非阻塞，获取请求对齐
                await self._makeup()
        except:
            if __debug__:
                traceback.print_exc()
            self.log.error(traceback.format_exc())

    async def _makeup(self):
        ticker = await self.makeupTickerQueue.get()
        self.counts['补齐'] = self.counts['补齐'] + 1

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

    def dailyMakeup(self, pushTickerIndex):
        """
        插入广播数据的接口
        :param pushTickerIndex:
        :return:
        """
        # 加载当日缓存数据
        tickers = self.loadToday()

        self.services.append(self.pushTicker2Makeup(tickers, pushTickerIndex))
        self.start()

    async def pushTicker2Makeup(self, tickers, pushTickerIndex):
        # tickers = tickers[:1000]
        total = len(tickers)
        num = 0

        await sleep(5)

        # 开始广播数据并进行对齐
        for t in tickers:
            pushTickerIndex(t)
            num += 1
            await sleep(0)

        self.log.info('广播结束 {} / {}'.format(num, total))

        try:
            while self.counts['索取'] > 0 and self.counts['索取'] != self.counts['补齐']:
                self.log.info(str(self.counts))
                await sleep(10)
            self.log.info(str(self.counts))
            self.close()
        except:
            traceback.print_exc()

