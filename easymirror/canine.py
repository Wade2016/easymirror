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


class BroadcastException(BaseException):
    pass


class Canine(object):
    NAME = None

    # 设定时间戳集合过期时间
    TIMESTAMP_SET_EXPIRE = 60 * 60 * 10
    if __debug__:
        TIMESTAMP_SET_EXPIRE = 60 * 5

    PRE_DAYS = 2  # 对齐 2 天内的 tick 数据

    # 对齐时的获取数据超时
    MAKEUP_TIMEOUT = 60  # seconds
    if __debug__:
        MAKEUP_TIMEOUT = 10  # seconds

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
        self.redis = None
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

        # 子线程逻辑
        self.threads = [
            self.popAskThread,
            self.queryAskThread,
        ]

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
        except:
            self.log.error(traceback.format_exc())
        finally:
            self.stop()

    def _run(self):

        # 加载要对齐的时间戳
        self.log.info('加载要对齐的时间戳')
        self.loadToday()

        if not self.cache:
            self.log.warning('没有数据需要对齐')
            return

        # 上传到 redis 服务器上
        self.log.info('上传数据')
        self.broadcast()

        # 加载完缓存数据，等待到开始广播的时间
        self.log.info('等待开始对比')
        self.waiStartDiffent()

        start = True
        while start or self.diffence:
            start = False
            # # 对比其他节点的时间戳，获得差异
            self.log.info('对比差异')
            self.diff()

            self.log.info('请求对齐')
            # 请求对齐
            self.ask()

            # 收集对齐数据
            self.log.info('接受对齐数据')
            self.makeup()

            # 等待数据上传
            time.sleep(20)
            self.log.info('========')

        self.log.info('收尾')
        # 收尾工作
        self.afterRun()

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

        # TODO 测试代码
        if self.diffhost:
            print(1414141)
            test_c = self.rk_timestamp_name + ':' + self.diffhost
            print(test_c, self.rk_timestamp_name_localhostname)
            d = self.redis.sdiff(test_c, self.rk_timestamp_name_localhostname)
            print(list(d))
            print(cache)

            print(161616)
            d = self.redis.sdiff(self._other, self._localchannel)
            print(list(d))
            print(cache)

        p = self.redis.pipeline()

        self.log.info('上传时间戳到 {} 共 {}'.format(channel, len(cache)))
        p.sadd(channel, *cache)
        # 管道堆入
        r = p.execute()

        if __debug__:
            afterScard = self.redis.scard(channel)
            print(r)
            self.log.debug('上传前后数量 {} {}'.format(preScard, afterScard))


    def _2timestamp(self, tick):
        """
        生成唯一的时间戳

        :param tick: {self.itamename: 'rb1710', self.timename: datetime.datetime() }
        :param t:
        :return: "rb1710:2017-04-18H22:34:36.5"
        """
        dt = tick[self.timename]
        assert isinstance(dt, datetime.datetime)
        return '{},{}'.format(tick[self.itemname], dt.strftime(self.DATETIME_FORMATE))

    DATETIME_FORMATE = "%Y-%m-%d %H:%M:%S.%f"

    def _4timestampe(self, ts):
        """

        :param ts:
        :return:
        """
        item, t = ts.split(',')
        return item, datetime.datetime.strptime(t, self.DATETIME_FORMATE)

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

    def diff(self):
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

        # 检查遗漏
        for other in timestampeChannels:
            hostname = other.split(':')[-1]
            # 差集
            self.log.info('对比 {} - {}'.format(other, localchannel))
            if __debug__:
                self._other =  other
                self._localchannel = localchannel
            diff = self.redis.sdiff(other, localchannel)

            if diff:
                # 跟某一个节点存在差异
                self.diffence = diff
                self.diffhost = hostname
                self.log.debug('差异主机 {} 数量 {}'.format(hostname, len(diff)))
                break

    def _2askmsg(self, ts):
        """
        格式化为请求队列
        :return: ts:localhostname
        """
        return ts + ':' + self.localhostname

    def _4askmsg(self, msg):
        """

        从 askmsg 解析

        :param msg:
        :return: item, datetime, localhostname
        """
        s, t, n = msg.split(':')
        s, dt = self._4timestampe(msg.strip(':' + n))
        return s, dt, n

    def ask(self):
        """
        请求对齐，逐个主机进行请求对齐
        :return:
        """

        # 对方的请求队列
        channel = self.rk_ask_name + ":{}".format(self.diffhost)
        p = self.redis.pipeline()

        # 生成请求信息
        asgmsgs = list(self.diffence)

        # 推送进去
        if __debug__:
            self.log.debug('对齐请求')
        self.log.info('请求channel: {}'.format(channel))

        # 阻塞 将数据放入队列，等待响应
        result = p.lpush(channel, json.dumps(asgmsgs)).execute()

        tmp = []
        while 0 in set(result):
            self.log.info('重新发送请求信息')
            for i, r in result:
                if r == 0:
                    msg = asgmsgs[i]
                    tmp.append(msg)

            result = p.lpush(channel, tmp)
            asgmsgs = tmp

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
        return self.redis or redis.StrictRedis(**self.redisConf, connection_pool=self.redisConnectionPool, decode_responses=True)
        # return redis.StrictRedis(**self.redisConf, connection_pool=self.redisConnectionPool, decode_responses=True)

    def popAsk(self):
        """

        监听请求的频道，查询、发送对齐队列

        :return:
        """
        redis = self.getRedis()
        channel = self.rk_ask_name_localhost
        while self.__active:
            # 阻塞方式获取
            msg = redis.blpop(channel)
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
            fromChannel, msgs = self.askQueue.get()
            msgs = json.loads(msgs)
            hostname = fromChannel.split(':')[-1]

            for msg in msgs:
                item, dt = self._4timestampe(msg)

                assert isinstance(dt, datetime.datetime)

                tick = self.queryTick2makeup(item, dt)
                try:
                    msg = self._2makeuptick(tick)
                except:
                    if __debug__:
                        self.log.debug(str(msg))
                        traceback.print_exc()
                    continue

                # 对方的接受频道
                channel = self.rk_makeuptick + ':' + hostname
                while not r.lpush(channel, msg):
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
        channel = self.rk_makeuptick + ':' + self.localhostname
        lackNum = len(self.diffence)
        makeupNum = 0

        newTicks = set()
        if lackNum == 0:
            self.log.warning('没有需要对齐的数据')
            return

        while makeupNum < lackNum:
            try:
                channel, tick = r.blpop(channel, self.MAKEUP_TIMEOUT)
            except TypeError:
                # 超时
                self.log.info('等待对齐时间结束 共计 {}'.format(makeupNum))
                break
            tick = self._4makeuptick(tick)

            makeupNum += 1
            # 保存该条tick
            ts = self._2timestamp(tick)
            if __debug__:
                if ts not in self.diffence:
                    print(181818)
                    print(ts)
                    print(self.diffence)
            if self.saveTick(tick):
                # 填入缓存
                self.cache.add(ts)
                newTicks.add(ts)
            else:
                self.log.debug('保存失败 {}'.format(ts))

            if __debug__:
                if makeupNum % 100 == 0:
                    self.log.debug('保存了 {} tick'.format(len(newTicks)))
        else:
            self.log.info('共对齐 {}'.format(makeupNum))

        # 补上新增的时间戳

        if newTicks:
            self._broadcastTimestampe(list(newTicks))

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

    def waiStartDiffent(self):
        """
        等待广播开始
        :return:
        """
        startTime = self.startDiffentTime()

        now = datetime.datetime.now()
        if __debug__:
            seconds = 5
            # seconds = 60
            rest = (self._riseTime + datetime.timedelta(seconds=seconds)) - now
        else:
            # 等到开始
            rest = startTime - now

        seconds = max(rest.total_seconds(), 0)
        if __debug__:
            self.log.debug('{} 秒后开始广播'.format(seconds))

        time.sleep(seconds)
