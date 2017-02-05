# encoding: utf-8

import os
import threading
import signal

from logbook import Logger
import zmq

from easymirror.rpc import *
from easymirror.serverlog import *

# 实现Ctrl-c中断recv
signal.signal(signal.SIGINT, signal.SIG_DFL)

def logfile():


class BaseServer(RpcObject):
    """RPC服务器"""
    name = None

    # ----------------------------------------------------------------------
    def __init__(self, logdir='.', vaild=True, repAddress="tcp://*:23001", pubAddress='tcp://*:23002', logzmqhost=None):
        """Constructor"""
        # 设置日志
        self.log = None
        self.__initLog(logdir, logzmqhost)

        # 是否启用这个服务
        self.vaild = vaild

        super(BaseServer, self).__init__()
        self.useMsgpack()

        # 保存功能函数的字典，key是函数名，value是函数对象
        self.__functions = {
            self.foo.__name__: self.foo,
        }

        # subprocess Queue
        self.pq = None

        self.repAddress = repAddress
        self.pubAddress = pubAddress

        # zmq端口相关
        self.__context = zmq.Context()

        self.__socketREP = self.__context.socket(zmq.REP)  # 请求回应socket
        self.__socketREP.bind(self.repAddress)

        self.__socketPUB = self.__context.socket(zmq.PUB)  # 数据广播socket
        self.__socketPUB.bind(self.pubAddress)

        # 工作线程相关
        self.__active = False  # 服务器的工作状态
        self.__thread = threading.Thread(target=self.run)  # 服务器的工作线程

    def __initLog(self, logdir, logzmqhost):
        """
        初始化日志配置
        - 打印到屏幕仅在调试模式下可用
        - 使用服务名作为日志的来源名
        :return:
        """
        global log
        logfile = os.path.join(logdir, '{}.log'.format(self.name))
        log = ServerLog(logfile, logzmqhost)

        # 使用服务名作为日志的来源名
        self.log = Logger(self.name)
        self.log.debug("初始化日志完成")

    def start(self):
        """启动服务器"""
        if not self.vaild:
            # 不启用这个服务
            self.log.notice("{name}服务不启用".format(name=self.name))
            return

        self.log.notice("{name}服务启动".format(name=self.name))

        # 将服务器设为启动
        self.__active = True

        # 启动工作线程
        if not self.__thread.isAlive():
            self.__thread.start()

    # ----------------------------------------------------------------------
    def stop(self):
        """停止服务器"""
        # 将服务器设为停止
        self.__active = False

        # 等待工作线程退出
        if self.__thread.isAlive():
            self.__thread.join()

    @log.stdout
    @log.file
    def run(self):
        """服务器运行函数"""
        while self.__active:
            self.log.debug("等待收包")
            # 使用poll来等待事件到达，等待1秒（1000毫秒）
            if not self.__socketREP.poll(1000):
                continue
            # 从请求响应socket收取请求数据
            reqb = self.__socketREP.recv()

            # 序列化解包
            req = self.unpack(reqb)
            if __debug__:
                self.log.debug("socket 信息: {}".format(req))

            # 获取函数名和参数
            name, args, kwargs = req

            # 获取引擎中对应的函数对象，并执行调用，如果有异常则捕捉后返回
            try:
                func = self.__functions[name]
                r = func(*args, **kwargs)
                rep = [True, r]
            except Exception as e:
                err = traceback.format_exc()
                print(err)
                rep = [False, err]

            # 序列化打包
            repb = self.pack(rep)

            # 通过请求响应socket返回调用结果
            self.__socketREP.send(repb)

    # ----------------------------------------------------------------------
    def publish(self, topic, data):
        """
        广播推送数据
        topic：主题内容
        data：具体的数据
        """
        # 序列化数据
        datab = self.pack(data)

        # 通过广播socket发送数据
        self.__socketPUB.send_multipart([topic, datab])

    # ----------------------------------------------------------------------
    def register(self, func):
        """注册函数"""
        self.__functions[func.__name__] = func

    # ----------------------------------------------------------------------
    def eventHandler(self, event):
        """事件处理"""
        self.publish(event.type_, event)

    # ----------------------------------------------------------------------
    def stopServer(self):
        """停止服务器"""

        # 停止服务器线程
        self.stop()

    def foo(self, t, s=None):
        """

        :return:
        """

        return '{} {s}'.format(t, s=s)

    @staticmethod
    def process(queue=None, *args, **kwargs):
        """
        子进程中将要运行的函数
        :param args:
        :param kwargs:
        :return:
        """
        # 创建实例
        server = BaseServer(*args, **kwargs)

        # 开始启动服务
        server.start()
        if queue is not None:
            queue.get()
        else:
            while True:
                if input("输入exit退出:") != "exit":
                    continue

                if input("是否退出(yes/no):") == "yes":
                    break
        server.stopServer()
