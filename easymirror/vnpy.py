# encoding: UTF-8
import json
import time
import datetime
import arrow

# from .client import BaseClient
# from .baseapi import BaseApi
# from .server import BaseServer
from .mirror import Mirror
import pymongo


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
        self.mongodb = pymongo.MongoClient(
            host=self.conf['host'],
            port=self.conf['port']
        )
        self.dbn = self.conf["TickerDB"]

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

    def getTickerByAsk(self, ask):
        """
        从本地查询需要对齐的ticker数据给对方
        :param ask:
        :return:
        """
        symbol = ask["symbol"]

        cmd = {
            "datetime": ask["datetime"],
        }
        # ticker 格式为 [{}]
        ticker = self.mongodb[self.dbn][symbol].find_one(cmd)

        if ticker:
            ticker.pop('_id')

        print(1212, ticker['datetime'])

        return ticker

    def getAskMsg(self, index):
        """

        :param index:
        :return:
        """
        index["hostname"] = self.localhostname
        return index

    def makeupTicker(self, ticker):
        """

        :param ticker:
        :return:
        """
        query = {
            self.timename: ticker[self.timename],
        }

        print(1313, ticker[self.timename], ticker[self.itemname])

        # 如果不存在，保存ticker数据
        # TODO 测试中，暂时不保存
        # self.mongodb[self.dbn][ticker[self.itemname]].update_one(ticker, query, upsert=True)

    def loadToday(self):
        """
        加载今天交易日的ticker数据并生成缓存
        :return:
        """

        # TODO 获取所有表，调试中，暂时只读取rb1710
        tickers = []
        for t in self.mongodb[self.dbn]['rb1710'].find():
            tickers.append(t)
            # 生成缓存
            self.tCache.put(
                t[self.timename],
                t[self.itemname],
            )

        return tickers
