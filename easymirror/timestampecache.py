# encoding: UTF-8


class TimestampeCache(object):
    """
    本地时间戳的缓存
    """

    def __init__(self):
        self.cache = {}  # {时间戳: set("品种名")}

    def put(self, timestamp, name):
        """
        时间戳和品种名
        :param timestamp:
        :param name:
        :return:
        """
        try:
            self.cache[timestamp].add(name)
        except KeyError:
            # 没有这个品种
            self.cache[timestamp] = {name}

    def isHave(self, timestamp, name):
        """
        已经有该条 ticker

        :param timestamp:
        :param name:
        :return:
        """
        try:
            return name in self.cache[timestamp]
        except KeyError:
            return False
