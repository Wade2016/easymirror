# encoding: UTF-8
import doctest
import logbook

logbook.set_datetime_format("local")
from logbook import *
from logbook.queues import ZeroMQHandler
import functools

__all__ = [
    "initLog",
    "file",
    "stdout",
]

"""
    服务日志实例
    提供句柄包括
    1. 日志文件
    2. 屏幕输出
    3. 微信方糖通知
    4. 邮件
"""

# 文件日志句柄
logFileHandler = None

streamHandler = None


def initLog(logfile, logzmqhost):
    """
    用于子进程中的服务的日志

    :param logfile:
    :param logzmqhost:
    :return:
    """
    global logFileHandler, streamHandler
    logFileHandler = FileHandler(logfile, bubble=True, level='NOTICE')
    # 子进程中的屏幕输出需要通过 ZMQ 来提交，ServerEngine 中订阅并显示
    if __debug__:
        print("日志zmq接口 {}".format(logzmqhost))
    streamHandler = ZeroMQHandler("tcp://{}".format(logzmqhost), level="DEBUG", bubble=True)
    if __debug__:
        streamHandler.applicationbound()


def file(func):
    """
    >>> initLog("../log/test.log", "127.0.0.1:24001")
    >>> @file
    ... def output(a):
    ...     notice(a)
    ...
    >>> output("测试内容")

    :param func:
    :return:
    """

    @functools.wraps(func)
    def wrapper(*args, **kw):
        global logFileHandler
        with logFileHandler.threadbound():
            return func(*args, **kw)

    return wrapper


def stdout(func):
    """
    使用ZMQ来从子进程中向主进程中推送屏幕输出的日志

    :param func:
    :return:
    """

    @functools.wraps(func)
    def wrapper(*args, **kw):
        global streamHandler
        with streamHandler.threadbound():
            return func(*args, **kw)

    return wrapper


if __name__ == "__main__":
    doctest.testmod()
