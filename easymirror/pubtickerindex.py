# encoding: UTF-8
from threading import Thread


class PubTickerIndex(Thread):
    def __init__(self, mirror):
        self.mirror = mirror
        super(PubTickerIndex, self).__init__(name=self.__class__.__name__)
        self.__active = False

    def _run(self):
        while self.__active:
            if __debug__: self.mirror.log.debug("等待Ticker数据")
            ticker = self.mirror.queue.get()

            if __debug__: self.mirror.log.debug("收到Ticker数据 {}".format(ticker))

            timestamp = self.mirror._stmptime(ticker)
            # 主机名
            timestamp["hostname"] = self.mirror.localhostname

            # 校验数据数据格式
            assert self.mirror.timename in timestamp

            # 缓存数据
            self.mirror.tCache.put(timestamp[self.mirror.timename], timestamp[self.mirror.itemname])

            self.mirror.counts[0] = self.mirror.counts[0] + 1

            timestamp = self.mirror.package(timestamp)
            self.mirror.redis.publish(self.mirror.tickerchannel, timestamp)

            if __debug__: self.mirror.log.debug("汇报时间戳 {}".format(timestamp))
