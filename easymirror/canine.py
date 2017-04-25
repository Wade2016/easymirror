import sys
import logging
import json
import datetime
import redis
import time
import socket
from threading import Thread
from queue import Queue
import traceback
import random


class BroadcastException(BaseException):
    pass


class Canine(object):
    NAME = None

    # 设定时间戳集合过期时间
    TIMESTAMP_SET_EXPIRE = 60 * 60 * 10
    if __debug__:
        TIMESTAMP_SET_EXPIRE = 60 * 60

    PRE_DAYS = 1  # 对齐 2 天内的 tick 数据

    # 对齐时的获取数据超时
    MAKEUP_TIMEOUT = 60  # seconds
    if __debug__:
        MAKEUP_TIMEOUT = 10  # seconds

    BUFF_SIZE = 100000

    SAVE_TIMEOUT_UNIT = 0.0001  # 每个 tick 允许超时的瞬间

    def __init__(self, conf):
        with open(conf, 'r') as f:
            conf = json.load(f)
        self.conf = conf[self.name]
        self.redisConf = conf['redis']
        self.dbn = self.conf['TickerDB']  # mongoDB 中 Tick 数据的数据库名
        self._riseTime = datetime.datetime.now()

        self.log = self._initLog()

        # 本地主机名，同时也是在Server-Redis上的标志，不能存在相同的主机名，尤其在使用Docker部署时注意重名
        self.localhostname = self.conf['localhostname'] or socket.gethostname()
        if __debug__:
            # 随机构建不重名的主机名
            self.localhostname = str(time.time())[-3:]
            self.log.debug('host {}'.format(self.localhostname))

        # self.redisConnectionPool = redis.ConnectionPool(
        self.redisConnectionPool = redis.BlockingConnectionPool(
            host=self.redisConf['host'],
            port=self.redisConf['port'],
            password=self.redisConf['password'],
            decode_responses=True,
        )
        self.redis = self.getRedis()

        # 时间戳集合 timestamp:vnpy:myhostname
        self.rk_timestamp_name = 'timestamp:{}'.format(self.name)
        self.rk_timestamp_name_localhostname = '{}:{}'.format(self.rk_timestamp_name, self.localhostname)

        # 时间戳缓存
        # self.tCache = TimestampeCache()
        self.cache = set()

        # 缺少的时间戳
        self.diffence = set()  # {timestamp, }
        self.diffhost = None

        # 请求对齐队列 ask:vnpy:localhostname
        self.rk_ask_name = 'ask:{}'.format(self.name)
        self.rk_ask_name_localhost = self.rk_ask_name + ':{}'.format(self.localhostname)

        self.__active = False
        self.popAskThread = Thread(target=self.popAsk, name=self.popAsk.__name__)

        # 请求的队列
        self.askQueue = Queue(1000)
        self.queryAskThread = Thread(target=self.queryAsk, name=self.queryAsk.__name__)

        # 接受tick数据
        self.rk_makeuptick = 'makeup:{}'.format(self.name)
        self.rk_makeuptick_localname = self.rk_makeuptick + ':' + self.localhostname
        self.makeupQueue = Queue()  # 返回需要对齐的 tick

        # 子线程逻辑
        self.threads = [
            self.popAskThread,
            self.queryAskThread,
        ]

        # 服务关闭时间
        self.closeTime = self.endMakeupTime()
        self.log.info('对齐 {} 关闭时间: {}'.format(self.name, self.closeTime))

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
        股票一般是 code, 期货是 item
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

    def run(self):
        """
        对齐的流程
        :return:
        """
        try:
            # 开启线程
            self.start()

            # 开启主线逻辑
            self._run()

            # 等待服务结束
            restTime = self.closeTime - datetime.datetime.now()
            time.sleep(restTime.total_seconds())
            self.stop()

        except:
            self.log.error(traceback.format_exc())
        finally:
            self.stop()

    def _run(self):

        # 检查 redis 情况
        self.checkRedis()

        # 加载要对齐的时间戳
        self.log.info('加载要对齐的时间戳')
        self.loadToday()

        # if not self.cache:
        #     self.log.warning('没有数据需要对齐')
        #     return

        # 上传到 redis 服务器上
        self.log.info('上传数据')
        self.broadcast()

        # 加载完缓存数据，等待到开始广播的时间
        self.log.info('等待开始对比')
        self.waiStartDiffent()

        self.log.info('对比差异')
        self.doDiff()

        while self.diffence:
            self.log.info('========')
            # # 对比其他节点的时间戳，获得差异

            # # 进行对齐
            self.log.info('进行对齐')
            self.makeup()

            self.log.info('重新对比')
            self.doDiff()

        self.log.info('收尾')
        # 收尾工作
        self.afterRun()

        time.sleep(60 * 10)

    def loadToday(self):
        """

        :return:
        """
        raise NotImplementedError()

    def broadcast(self):
        """
        将缓存上传到 redis 服务器
        设定超时
        :return:
        """
        # 等待广播时间

        cache = list(self.cache)
        self._broadcastTimestampe(cache)
        # 设定集合过期时间, 一小时后过期
        self.redis.expireat(self.rk_timestamp_name_localhostname, int(time.time() + self.TIMESTAMP_SET_EXPIRE))

    def _broadcastTimestampe(self, cache):
        """

        上传 时间戳

        :param cache:
        :return:
        """
        assert isinstance(cache, list)

        channel = self.rk_timestamp_name_localhostname

        if __debug__:
            preScard = self.redis.scard(channel)

        self.log.info('上传时间戳到 {} 共 {}'.format(channel, len(cache)))

        p = self.redis.pipeline()

        if len(cache) < 10:
            for ts in cache:
                print(self.redis.sismember(channel, ts))

        # 管道堆入
        buff_size = self.BUFF_SIZE
        size = len(cache)

        def buffg(cache):
            b, e = 0, 0
            while e < size:
                b, e = e, e + buff_size
                yield cache[b:e]

        for buff in buffg(cache):
            p.sadd(channel, *buff)

        p.execute()

        if __debug__:
            time.sleep(1)
            afterScard = self.redis.scard(channel)
            self.log.debug('上传前后数量 {} {}'.format(preScard, afterScard))

    def _2timestamp(self, tick):
        """
        生成唯一的时间戳
        tick 是完整的 Tick数据 dict类型，或者致至少包含以下两个关键的 keys

        :param tick: {self.itamename: 'rb1710', self.timename: datetime.datetime() }
        :param t:
        :return: "rb1710:2017-04-18H22:34:36.5"
        """
        dt = tick[self.timename]
        assert isinstance(dt, datetime.datetime)
        # 可读的时间戳
        # return '{},{}'.format(tick[self.itemname], dt.strftime(self.DATETIME_FORMATE))
        # 浮点时间戳
        return '{},{}'.format(tick[self.itemname], dt.timestamp())

    DATETIME_FORMATE = "%Y-%m-%d %H:%M:%S.%f"

    def _4timestampe(self, ts):
        """

        :param ts:
        :return:
        """
        print(161616, ts)
        item, t = ts.split(',')

        # 可读的时间戳
        # return item, datetime.datetime.strptime(t, self.DATETIME_FORMATE)
        # 浮点时间戳
        print(17171717, item, t)
        print(datetime.datetime.fromtimestamp(float(t)))
        return item, datetime.datetime.fromtimestamp(float(t))

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

    def doDiff(self):
        """

        对比与其他节点时间戳的差异

        :return:
        """
        self.diffence = set()
        self.diffhost = None

        # 本地的时间戳频道
        localchannel = self.rk_timestamp_name_localhostname
        timestampeChannels = self.redis.keys(self.rk_timestamp_name + ':*')
        self.log.info('在线的时间戳频道 {}'.format(', '.join(timestampeChannels)))
        # 忽略自己的时间戳
        try:
            timestampeChannels.remove(localchannel)
        except ValueError:
            pass

        # 检查遗漏，随机排序分摊压力
        for other in random.sample(timestampeChannels, len(timestampeChannels)):
            hostname = other.split(':')[-1]
            # 差集
            self.log.info('对比 {} - {}'.format(other, localchannel))
            if __debug__:
                self._other = other
                self._localchannel = localchannel
            diff = self.redis.sdiff(other, localchannel)

            if diff:
                # 跟某一个节点存在差异
                self.diffence = diff
                self.diffhost = hostname
                self.log.debug('差异主机 {} 数量 {}'.format(hostname, len(diff)))
                break
        else:
            self.log.info('对比所有主机无差异')

    def _2askmsg(self, ts):
        """
        格式化为请求队列
        :return: ts:localhostname
        """
        return ts + ',' + self.localhostname

    def _4askmsg(self, msg):
        """

        从 askmsg 解析

        :param msg:
        :return: item, datetime, localhostname
        """
        s, t, n = msg.split(',')
        return s, datetime.datetime.fromtimestamp(float(t)), n

    # def ask(self):
    #     """
    #     请求对齐，逐个主机进行请求对齐
    #     :return:
    #     """
    #
    #     # 对方的请求队列
    #     channel = self.rk_ask_name + ":{}".format(self.diffhost)
    #     p = self.redis.pipeline()
    #
    #     # 生成请求信息
    #     asgmsgs = [self._2askmsg(ts) for ts in self.diffence]
    #
    #     # 推送进去
    #     if __debug__:
    #         self.log.debug('对齐请求')
    #     self.log.info('请求channel: {}'.format(channel))
    #
    #     # 阻塞 将数据放入队列，等待响应
    #     result = p.lpush(channel, json.dumps(asgmsgs)).execute()
    #
    #     tmp = []
    #     while 0 in set(result):
    #         self.log.info('重新发送请求信息')
    #         for i, r in result:
    #             if r == 0:
    #                 msg = asgmsgs[i]
    #                 tmp.append(msg)
    #
    #         result = p.lpush(channel, tmp)
    #         asgmsgs = tmp

    def start(self):
        """

        :return:
        """
        self.__active = True
        for t in self.threads:
            if not t.isAlive():
                t.start()

    def stop(self):
        """
        停止
        :return:
        """
        self.__active = False
        for t in self.threads:
            if t.isAlive():
                t.join()

    def getRedis(self):
        """

        :return:
        """
        # return self.redis or redis.StrictRedis(**self.redisConf, connection_pool=self.redisConnectionPool, decode_responses=True)
        return redis.StrictRedis(**self.redisConf, connection_pool=self.redisConnectionPool, decode_responses=True)

    def popAsk(self):
        """

        监听请求的频道，查询、发送对齐队列

        :return:
        """
        r = self.getRedis()
        channel = self.rk_ask_name_localhost
        self.log.info('开始监听请求频道 {}'.format(channel))
        while self.__active:
            # 阻塞方式获取
            c, msg = r.blpop(channel)
            # 将其放到待处理队列，阻塞
            self.askQueue.put(msg)

    def queryAsk(self):
        """

        查询请求中需要的数据

        :return:
        """
        r = self.getRedis()
        while self.__active:
            # 获取对齐请求，阻塞
            askmsg = self.askQueue.get()
            item, dt, host = self._4askmsg(askmsg)
            tick = self.queryTick2makeup(item, dt)

            if tick is None:
                print(191919, askmsg)
                print(self._4askmsg(askmsg))
                print('===========')
                continue

            try:
                tick = self._2makeuptick(tick)
            except:
                self.log.error(traceback.format_exc())
                continue

            # 对方的接受频道
            makeupChannel = self.rk_makeuptick + ':' + host
            while not r.lpush(makeupChannel, tick):
                self.log.info('发送对齐tick失败，重发...')
                time.sleep(0.1)

    def _2makeuptick(self, tick):
        """

        格式化用于补给对方的 tick 数据

        :param tick:
        :return:
        """
        raise NotImplementedError()

    def _4makeuptick(self, tick):
        """

        :param tick:
        :return:
        """
        raise NotImplementedError()

    def queryTick2makeup(self, item, timestampe):
        """

        查询用于对齐的数据

        :return:
        """
        raise NotImplementedError()

    def makeup(self):
        """

        等待对齐的数据

        :return:
        """
        r = self.redis

        assert self.diffhost is not None

        askChannel = self.rk_ask_name + ':' + self.diffhost
        makeupChannel = self.rk_makeuptick_localname
        size = len(self.diffence)

        self.log.info('请求频道: {}'.format(askChannel))
        self.log.info('本地对齐: {}'.format(makeupChannel))

        # 使用线程接受 tick 并存库
        _saveTick = Thread(target=self._saveTick, args=(size,))
        _saveTick.start()

        for ts in self.diffence:
            while True:
                askmsg = self._2askmsg(ts)
                # 发送请求
                self.redis.lpush(askChannel, askmsg)
                try:
                    # 接受对齐数据
                    c, msg = r.blpop(makeupChannel, self.MAKEUP_TIMEOUT)
                    tick = self._4makeuptick(msg)
                    self.makeupQueue.put(tick)
                except TypeError:
                    self.log.debug(askmsg)
                    self.log.info('超时, 重新发送')
                    if __debug__:
                        traceback.print_exc()
                    continue
                break

        saveTimeout = max(self.SAVE_TIMEOUT_UNIT * size, 10)
        self.log.info('等待存库完成 ')
        _saveTick.join(timeout=saveTimeout)

    def _saveTick(self, size):
        """

        :param size: 这一批要对齐的 tick 数量
        :return:
        """
        timestampes = []

        num = 0

        for i in range(size):
            tick = self.makeupQueue.get()
            self.saveTick(tick)
            ts = self._2timestamp(tick)
            timestampes.append(ts)
            if __debug__:
                num += 1
                if num % 10 == 0:
                    self.log.debug('导入 {}/{}'.format(num, size))

        self.cache |= set(timestampes)

        # 对齐Tick 后,补上时间戳
        self.log.info('补充对齐后的时间戳')
        self._broadcastTimestampe(list(timestampes))

    def saveTick(self, tick):
        """

        将缺少的 tick 数据保存

        :param tick:
        :return: bool(保存成功)
        """
        raise NotImplementedError()

    def afterRun(self):
        """
        收尾工作

        :return:
        """

        raise NotImplementedError()

    # 开始盘后广播的时间
    AFTER_MAKEUP_BROADCAST_TIME = [
        datetime.time(4),  # 凌晨4点
        datetime.time(16),  # 下午4点
    ]

    def startDiffentTime(self):
        """
        开始对比的时间
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

    def waiStartDiffent(self):
        """
        等待广播开始
        :return:
        """
        startTime = self.startDiffentTime()

        now = datetime.datetime.now()
        if __debug__:
            # seconds = 5
            seconds = 60
            rest = (self._riseTime + datetime.timedelta(seconds=seconds)) - now
        else:
            # 等到开始
            rest = startTime - now

        seconds = max(rest.total_seconds(), 0)
        if __debug__:
            self.log.debug('{} 秒后开始对比'.format(seconds))

        time.sleep(seconds)

    def checkRedis(self):
        """
        清除所有 redis 的数据
        :return:
        """
        # 自动清除过期的 key
        self.redis.keys()
        if __debug__:
            self.redis.flushdb()