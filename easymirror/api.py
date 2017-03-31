# encoding: UTF-8
import importlib
from multiprocessing import Process, Queue
import time
import os
from .dealutils import getConfPath

# 子进程通信队列
queue = Queue()


def getMirror(_service, conf=None):
    '''
    获得对应的服务配套的接口

    :param serivce:
    :return:
    '''
    conf = conf or os.path.join(getConfPath(), 'conf.json')

    if __debug__:
        from threading import Thread as Process
        from queue import Queue

    return Process(target=_startMirror, args=[_service, conf, queue])


def _testQueue(queue):
    with open('./tmp/testqueue.txt', 'w') as f:
        while 1:
            f.write(queue.get())
            f.write("\n")
            f.flush()


def pushTickerIndex(tickerIndex):
    '''

    :param tickerIndex:
    :param _format: 将原始数据进行转化处理的函数
    :return:
    '''

    queue.put(
        tickerIndex
    )


def _startMirror(service, conf, queue):
    '''
    运行 easymirror 的子进程

    :return:
    '''

    m = importlib.import_module('easymirror.{}'.format(service))
    em = m.Easymirror(conf, queue)
    em.start()
    while True:
        pass


def makeup(_service, conf=None):
    '''
    获得对应的服务配套的接口

    :param serivce:
    :return:
    '''
    conf = conf or os.path.join(getConfPath(), 'conf.json')

    # if __debug__:
    #     from threading import Thread as Process
    #     from queue import Queue

    return Process(target=_makeup, args=[_service, conf, queue])


def _makeup(service, conf, queue):
    '''
    盘后对齐

    :return:
    '''
    m = importlib.import_module('easymirror.{}'.format(service))
    em = m.Easymirror(conf, queue)

    # TODO 加载当日缓存数据
    tickers = em.loadToday()

    em.start()

    time.sleep(2)

    total = len(tickers)
    num = 0

    # 开始广播数据并进行对齐
    for t in tickers:
        pushTickerIndex(t)
        num += 1
        time.sleep(0.05)

    print('广播结束 {} / {}'.format(num, total))
    time.sleep(10)
    em.stop()

