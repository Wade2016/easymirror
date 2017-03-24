import importlib
from multiprocessing import Process, Queue
import json

# 子进程通信队列
queue = Queue()


def getMirror(_service, conf):
    '''
    获得对应的服务配套的接口

    :param serivce:
    :return:
    '''
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
    :return:
    '''

    queue.put(json.dumps(tickerIndex))


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