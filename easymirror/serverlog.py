import sys
import doctest
from logbook import *
from logbook.queues import ZeroMQHandler
import functools

__all__ = [
    "ServerLog",
]


class ServerLog(object):
    """
    服务日志实例
    提供句柄包括
    1. 日志文件
    2. 屏幕输出
    3. 微信方糖通知
    4. 邮件
    """

    def __init__(self, logfile, logzmqhost):
        self.logFileHandler = FileHandler(logfile, bubble=True, level='NOTICE')

        self.streamHandler = ZeroMQHandler("tcp://{}".format(logzmqhost))

        if __debug__:
            self.streamHandler.applicationbound()

    def file(self, func):
        """
        >>> log = ServerLog(logfile="../log/test.log")
        >>> @log.file
        ... def output(a):
        ...     notice(a)
        ...
        >>> output("测试内容")

        :param func:
        :return:
        """

        @functools.wraps(func)
        def wrapper(*args, **kw):
            with self.logFileHandler.threadbound():
                return func(*args, **kw)

        return wrapper

    def stdout(self, func):
        """
        >>> log = ServerLog(logfile="../log/test.log")
        >>> @log.stdout
        ... def output(a):
        ...     notice(a)
        ...
        >>> output("测试内容")
        [2017-02-04 08:27:50.096738] NOTICE: Generic: 测试内容

        :param func:
        :return:
        """

        @functools.wraps(func)
        def wrapper(*args, **kw):
            with self.streamHandler.threadbound():
                return func(*args, **kw)

        return wrapper


if __name__ == "__main__":
    doctest.testmod()
