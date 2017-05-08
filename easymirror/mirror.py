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
import collections
import signal
import datetime

try:
    import cPickle as pickle
except ImportError:
    import pickle
import redis
from .timestampecache import TimestampeCache

# from .pubtickerindex import PubTickerIndex




class Mirror(object):
    """
    数据对其服务的基类
    使用该接口，将会生成一个子进程，用于汇报Ticker时间戳和对齐Ticker数据
    """
    NAME = None
    ASK_CHANNEL_MODLE = 'ask:{}:{}'
    RECEIVE_CHANNEL_MODLE = 'receive:{}:{}'

    CORO_TIME_OUT = 1  # seconds

    PRE_DAYS = 2  # 对齐几天内的数据

    def __init__(self, conf, queue):
        """

        :param conf: 配置文件的路径
        :param queue: 进程通信队列
        """
        global mirror
        mirror = self
        self.queue = queue

        # 加载配置文件
        with open(conf, 'r') as f:
            conf = json.load(f)
        self.conf = conf[self.name]

        # 初始化日志
        self.log = self._initLog()

        # 建立协程池
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)

        # 退出信号的调用
        def myHandler(signum, frame):
            self.stop()

        for sig in [signal.SIGINT]:
            # self.loop.add_signal_handler(sig, self.stop)
            signal.signal(sig, myHandler)

        if __debug__:
            if sys.platform == 'win32':  # pragma: no cover
                assert isinstance(self.loop, asyncio.windows_events._WindowsSelectorEventLoop)
            else:
                assert isinstance(self.loop, asyncio.unix_events._UnixSelectorEventLoop)

        # 本地主机名，同时也是在Server-Redis上的标志，不能存在相同的主机名，尤其在使用Docker部署时注意重名
        self.localhostname = self.conf['localhostname'] or socket.gethostname()

        if __debug__:
            # 随机构建不重名的主机名
            self.localhostname = str(time.time())[-3:]

        self.log.info('localhostname {}'.format(self.localhostname))

        # 建立 redis 链接
        self.redisConf = conf["redis"]
        self.redis = redis.StrictRedis(
            **self.redisConf
        )
        self.log.info('redis connect to {}:{}'.format(self.redisConf['host'], self.redisConf['port']))

        # 请求对齐用到的两个频道名
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
            self.funcRunForever(self.pubTickerIndex),
            self.subTickerIndex(),
            self.coroRunForever(self.handlerSubTicker),
            self.coroRunForever(self.recAsk),
            self.coroRunForever(self.handlerAsk),
            self.coroRunForever(self.recvTicker),
            self.coroRunForever(self.makeup),
        ]

        # 忽略的主机名，一般在订阅行情中忽略自己发布的行情
        self.filterHostnames = {self.localhostname, }

        # 索引的缓存，用来对比是否缺失数据
        self.tCache = TimestampeCache()

        # 计数
        self.counts = collections.OrderedDict(
            (('广播', 0), ('收听', 0), ('索取', 0), ('被索取', 0), ('补齐', 0))
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

            log.setLevel("DEBUG")
            log.debug("初始化日志完成")
        return log

    def start(self):
        """

        :return:
        """
        self.__active = True
        for s in self.services:
            asyncio.ensure_future(s, loop=self.loop)

        self.loop.run_forever()
        self.log.warning('服务器关闭!!!')
        self.loop.close()

        # for s in self.services:
        #     assert asyncio.iscoroutine(s)
        #         self.log.info('{} 服务开始'.format(s.name))

        # self.run()

    def stop(self):
        # 停止循环
        self.log.warning("即将停止服务……")
        self.__active = False
        self.loop.call_later(self.CORO_TIME_OUT + 1, self.loop.stop)

    @property
    def tickerchannel(self):
        return 'ticker:{}'.format(self.name)

    async def subTickerIndex(self):
        self.log.info("开始订阅频道 {}".format(self.tickerchannel))

        r = self.redis
        # r = redis.StrictRedis(**self.redisConf)

        with contextlib.closing(r.pubsub()) as sub:
            sub.subscribe(self.tickerchannel)
            try:
                while self.__active:
                    msg = sub.get_message(ignore_subscribe_messages=True)
                    if msg is None:
                        await sleep(0.1)
                        continue

                    # 堆入本地时间戳队列等待处理
                    await asyncio.wait_for(self.subTickerQueue.put(msg), timeout=self.CORO_TIME_OUT)
            except:
                if __debug__:
                    traceback.print_exc()
                self.log.error(traceback.format_exc())
            finally:
                self.log.info('订阅 {} 结束 '.format(self.tickerchannel))


    async def handlerSubTicker(self):
        """
        处理订阅得到的时间戳
        :return:
        """
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

    async def coroRunForever(self, func):
        """
        将一个协程封装为永久运行

        :param coro:
        :return:
        """
        coro = func()
        assert asyncio.iscoroutine(coro) or isinstance(coro, asyncio.futures.Future)

        try:
            while self.__active:
                # 设定超时
                with contextlib.suppress(asyncio.futures.TimeoutError):
                    await asyncio.wait_for(func(), timeout=self.CORO_TIME_OUT)
        except:
            self.log.error(traceback.format_exc())

    async def funcRunForever(self, func):
        """

        将一个 函数 封装为永久运行

        :param func:
        :return:
        """
        # 对象是一个函数
        assert callable(func)

        try:
            while self.__active:
                func()
                await sleep(0)
        except:
            self.log.error(traceback.format_exc())

    def pubTickerIndex(self):

        try:
            # 进程的通信队列，不能用 await
            ticker = self.queue.get_nowait()
        except queue.Empty:
            return

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
        # 非阻塞，获取请求对齐
        msg = self.redis.lpop(self.askchannel)
        if msg is None:
            await sleep(0.1)
            return

        await self.revAskQueue.put(msg)

    async def handlerAsk(self):
        """
        处理收到的请求对齐
        :return:
        """
        msg = await self.revAskQueue.get()
        self.counts['被索取'] = self.counts['被索取'] + 1
        ask = self.unpackage(msg)

        # 查询本地的 Ticker 数据

        async def gd(ask):
            ticker = await self.getTickerByAsk(ask)
            # 返回 Ticker 数据
            if ticker:
                self._donator(ask, ticker)

        asyncio.ensure_future(gd(ask), loop=self.loop)

    def ask(self, index):
        """
        发起请求对齐
        :return:
        """

        ask = index.copy()
        ask['hostname'] = self.localhostname
        ask = self.getAskMsg(ask)

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

    async def getTickerByAsk(self, ask):
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
        msg = self.redis.lpop(self.receivechannel)
        if msg is None:
            await sleep(0.1)
            return
        else:
            await self.makeupTickerQueue.put(msg)

    async def makeup(self):
        ticker = await self.makeupTickerQueue.get()
        self.counts['补齐'] = self.counts['补齐'] + 1

        ask = self.unpackage(ticker)
        asyncio.ensure_future(self.makeupTicker(ask), loop=self.loop)

    async def makeupTicker(self, ticker):
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
        # 获得当日数据的生成器
        tickers = self.loadToday()

        if __debug__:
            self._makeupBeginTime = datetime.datetime.now()

        asyncio.ensure_future(self.pushTicker2Makeup(tickers, pushTickerIndex), loop=self.loop)
        self.start()

    async def pushTicker2Makeup(self, tickers, pushTickerIndex):
        try:
            # tickers = tickers[:1000]
            total = len(tickers)
            num = 0

            now = datetime.datetime.now()
            waitTime = now - self.startBroadcastTime()

            self.log.info('开始广播……')

            if __debug__:
                b = self._makeupBeginTime + datetime.timedelta(seconds=60*2)
                while datetime.datetime.now() < b:
                    await sleep(1)
            else:
                # 等待 n 秒之后开始广播
                await sleep(waitTime.total_seconds())

            # 开始广播数据并进行对齐
            for t in tickers:
                pushTickerIndex(t)
                num += 1
                if __debug__:
                    if not num % 10000:
                        self.log.debug(str(self.counts))
                await sleep(0)

            self.log.info('广播结束 {} / {}'.format(num, total))
            await sleep(1)
            self.log.info(str(self.counts))

            if __debug__:
                endTime = datetime.datetime.now() + datetime.timedelta(seconds=60 * 60)
            else:
                # 关闭服务的时间
                endTime = self.endMakeupTime()

            self.log.info(str(self.counts))
            while datetime.datetime.now() < endTime:
                await sleep(10)
                if __debug__:
                    self.log.debug(str(self.counts))

            self.log.info(str(self.counts))
            if self.__active:
                self.stop()
        except:
            self.log.error(traceback.format_exc())

    # 开始盘后广播的时间
    AFTER_MAKEUP_BROADCAST_TIME = [
        datetime.time(4),  # 凌晨4点
        datetime.time(16),  # 下午4点
    ]

    def startBroadcastTime(self):
        """
        开始广播的时间
        :return:
        """
        now = datetime.datetime.now()
        today = datetime.date.today()

        for t in self.AFTER_MAKEUP_BROADCAST_TIME:
            if now.time() < t:
                # 当天
                return datetime.datetime.combine(today, t)
        else:
            # 次日凌晨
            tomorrow = today + datetime.timedelta(days=1)
            t = self.AFTER_MAKEUP_BROADCAST_TIME[0]
            return datetime.datetime.combine(tomorrow, t)

    # 盘后对齐结束时间
    AFTER_MAKEUP_END_TIME = [
        datetime.time(8),  # 早上8点
        datetime.time(20),  # 晚上8点
    ]

    def endMakeupTime(self):
        """
        盘后对齐关闭的时间
        :return:
        """
        now = datetime.datetime.now()
        today = datetime.date.today()

        for t in self.AFTER_MAKEUP_END_TIME:
            if now.time() < t:
                # 当天
                return datetime.datetime.combine(today, t)
        else:
            # 次日凌晨
            tomorrow = today + datetime.timedelta(days=1)
            t = self.AFTER_MAKEUP_END_TIME[0]
            return datetime.datetime.combine(tomorrow, t)
