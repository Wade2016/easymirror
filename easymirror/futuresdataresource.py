import doctest
import json

from .dataresource import BaseDataResource
from .dealutils import *


def getFuturesInserter(confPath=None):
    """

    返回接口

    :return:
    """

    confPath = confPath or getConfPath()

    return FuturesIndexInserter(confPath)


class FuturesIndexInserter(BaseDataResource):
    """
    数据源采集用的接口实例，用来把时间索引插入到客户端中
    """
    name = "futures"

#


if __name__ == "__main__":
    doctest.testmod()
