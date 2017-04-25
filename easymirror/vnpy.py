# encoding: UTF-8
import json
import datetime

from .mirror import Mirror
import pymongo
import motor.motor_asyncio


class Easymirror(Mirror):
    """
    镜像服务
    """
    NAME = "vnpy"

    def __init__(self, conf, queue):
        """

        """
        super(Easymirror, self).__init__(conf, queue)
        # 初始化本地数据库链接
        self.counts['存库'] = 0
        self.log.info('建立 MongoDB 连接 {host}:{port}'.format(**self.conf))
        self.client = motor.motor_asyncio.AsyncIOMotorClient(
            host=self.conf['host'],
            port=self.conf['port']
        )

        self.dbn = self.conf["TickerDB"]
        self.db = self.client[self.dbn]

        self.pymongoCLient = pymongo.MongoClient(
            host=self.conf['host'],
            port=self.conf['port']
        )

    @property
    def indexLike(self):
        """
        对齐索引的格式
        :return:
        """
        return {
            'datetime': datetime.datetime(),
            'symbol': "rb1710"
        }

    def columns(self):
        return ['datetime', 'askPrice1', 'askPrice2', 'askPrice3', 'askPrice4', 'askPrice5',
                'askVolume1', 'askVolume2', 'askVolume3', 'askVolume4', 'askVolume5',
                'bidPrice1', 'bidPrice2', 'bidPrice3', 'bidPrice4', 'bidPrice5',
                'bidVolume1', 'bidVolume2', 'bidVolume3', 'bidVolume4', 'bidVolume5',
                'date', 'exchange', 'lastPrice', 'lowerLimit',
                'openInterest', 'symbol', 'time', 'upperLimit', 'volume', 'vtSymbol']

    @property
    def timename(self):
        return "datetime"

    @property
    def itemname(self):
        """
        品种名的key
        股票一般是 code, 期货是 symbol
        :return:
        """
        return 'symbol'

    DATETIME_FORMATE = "%Y-%m-%d %H:%M:%S.%f"

    def _stmptime(self, ticker):
        """

        将 Ticker 数据转为时间戳

        :return:
        """
        return {
            "datetime": ticker["datetime"],
            "symbol": ticker["symbol"],
        }

    def handlerTickerIndex(self, msg):
        """

        处理订阅到的时间戳

        :param msg:
        :return:
        """

        return json.loads(msg)

    async def getTickerByAsk(self, ask):
        """
        从本地查询需要对齐的ticker数据给对方
        :param ask:
        :return:
        """
        symbol = ask["symbol"]

        cmd = {
            "datetime": datetime.datetime.fromtimestamp(ask["datetime"]),
        }
        # ticker 格式为 [{}]
        ticker = await self.db[symbol].find_one(cmd)

        if ticker:
            ticker.pop('_id')

        return ticker

    def getAskMsg(self, index):
        """

        :param index:
        :return:
        """
        index["hostname"] = self.localhostname
        return index

    async def makeupTicker(self, ticker):
        """



        :param ticker:
        :return:
        """
        query = {
            self.timename: ticker[self.timename],
        }

        # 如果不存在，保存ticker数据
        # await self.db[ticker[self.itemname]].update_one(query, {'$set': ticker}, upsert=True)
        self.counts['存库'] += 1

    def loadToday(self):
        """
        加载今天交易日的ticker数据并生成缓存
        :return:
        """

        db = self.db

        # 协程
        allCollectionNames = self.loop.run_until_complete(db.collection_names())

        preDates = []
        today = datetime.date.today()
        preDate = today.strftime('%Y%m%d')
        for d in range(self.PRE_DAYS):
            preDate = today - datetime.timedelta() - datetime.timedelta(days=d)
            preDate = preDate.strftime('%Y%m%d')
            preDates.append(preDate)

        # 两天内的数据进行对齐，比如今天是4月16日，那么对4月15日和16日的数据进行对齐
        collectionNames = []
        for colName in allCollectionNames:
            for d in preDates:
                if self.loop.run_until_complete(db[colName].find_one({'date': d})):
                    # 这个集合有需要对齐的数据
                    collectionNames.append(colName)
                    break

        self.log.info('对齐以下合约: {}'.format(','.join(collectionNames)))

        tickers = []

        colsNum = len(collectionNames)
        num = 0
        db = self.pymongoCLient[self.dbn]
        for n, colName in enumerate(collectionNames):
            if __debug__:
                self.log.debug('加载合约 {} {}/{}'.format(colName, n + 1, colsNum))
            with db[colName].find({'date': {'$gte': preDate}}) as cursor:
                for t in cursor.distinct(self.timename):

                    if __debug__:
                        num += 1
                        if num % 10000 == 0:
                            self.log.debug('已经加载 %s 条数据' % num)
                    timestamp = t.timestamp()
                    tickers.append({self.timename: timestamp, self.itemname: colName})

                    # 生成缓存
                    self.tCache.put(
                        timestamp,
                        colName,
                    )
        self.log.info('加载了 {} 条ticker数据'.format(str(len(tickers))))
        return tickers
