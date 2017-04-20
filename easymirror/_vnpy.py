import sys
import datetime
import pymongo
from .canine import Canine
import json
import traceback
import contextlib


class Easycanine(Canine):
    NAME = 'vnpy'

    def __init__(self, conf):
        """

        :param conf:
        """
        super(Easycanine, self).__init__(conf)

        self.collectionNames = []  # 今天需要对齐的合约
        self.pymongo = pymongo.MongoClient(host=self.conf['mongohost'], port=self.conf['mongoport'])
        self.log.info('Mongodb {host}:{port}'.format(host=self.conf['mongohost'], port=self.conf['mongoport']))

    def loadToday(self):
        """
        加载今天交易日的ticker数据并生成缓存
        :return:
        """

        db = self.pymongo[self.dbn]

        allCollectionNames = db.collection_names()

        # 筛选需要对齐的合约，PRE_DAYS 日内有变动的合约都要进行对齐
        preDates, preDate = self.getPreDates()

        # 两天内的数据进行对齐，比如今天是4月16日，那么对4月15日和16日的数据进行对齐
        collectionNames = []
        for colName in allCollectionNames:
            for d in preDates:
                if db[colName].find_one({'date': d}):
                    # 这个集合有需要对齐的数据
                    collectionNames.append(colName)
                    break

        self.log.info('对齐以下合约: {}'.format(','.join(collectionNames)))

        self.collectionNames = collectionNames
        colsNum = len(collectionNames)
        num = 0
        db = self.pymongo[self.dbn]
        for n, colName in enumerate(collectionNames):
            # if __debug__:
            #     self.log.debug('加载合约 {} {}/{}'.format(colName, n + 1, colsNum))

            with db[colName].find({'date': {'$gte': preDate}}) as cursor:
                for t in cursor.distinct(self.timename):
                    if __debug__:
                        import random
                        if random.randint(0, 1000) == 1:
                            continue
                        num += 1
                        if num % 1000 == 0:
                            self.log.debug('已经加载 %s 条数据' % num)

                    # 生成缓存
                    ts = self._2timestamp({self.itemname: colName, self.timename: t})

                    self.cache.add(ts)

        self.log.info('加载了 {} 条ticker数据'.format(str(len(self.cache))))

    def getPreDates(self):
        preDates = []
        today = datetime.date.today()
        preDate = today.strftime('%Y%m%d')
        for d in range(self.PRE_DAYS):
            preDate = today - datetime.timedelta() - datetime.timedelta(days=d)
            preDate = preDate.strftime('%Y%m%d')
            preDates.append(preDate)

        return preDates, preDate

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

    def _2makeuptick(self, tick):
        """

        格式化用于补给对方的 tick 数据

        :param tick:
        :return:
        """
        assert isinstance(tick[self.timename], datetime.datetime)
        tick[self.timename] = tick[self.timename].timestamp()

        return json.dumps(tick)

    def _4makeuptick(self, msg):
        """

        从对齐过来的 tick 数据解析

        :param msg:
        :return:
        """
        tick = json.loads(msg)

        tick[self.timename] = datetime.datetime.fromtimestamp(tick[self.timename])
        return tick

    def queryTick2makeup(self, symbol, dt):
        """

        查询用于对齐的数据

        :return:
        """
        self.log.debug('12121')
        print(symbol, dt)
        db = self.pymongo[self.dbn]
        query = {self.timename: dt}

        print(131313, query)
        tick = db[symbol].find_one(query)

        print(141414)
        print(tick)

        with contextlib.suppress(KeyError):
            tick.pop('_id')

        return tick

    def saveTick(self, tick):
        """

        :param tick:
        :return:
        """
        # 合约编码 'rb1710'
        symbol = tick[self.itemname]
        collection = self.pymongo[self.dbn][symbol]

        # 更新插入，有重复的时间戳不再重复插入数据
        try:
            return collection.insert_one(tick)
        except:
            self.log.error(traceback.format_exc())
            return False

    def afterRun(self):
        """

        :return:
        """
        # 根据时间戳建立索引，剔除重复的tick 数据
        pass
