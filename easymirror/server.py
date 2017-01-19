# encoding: utf-8

import signal
import threading
import time
from datetime import datetime

import zmq

from easymirror.rpc import *

# 实现Ctrl-c中断recv
signal.signal(signal.SIGINT, signal.SIG_DFL)

class Server(RpcObject):
    """RPC服务器"""

    # ----------------------------------------------------------------------
    def __init__(self, repAddress, pubAddress):
        """Constructor"""
        super(Server, self).__init__()
        self.useMsgpack()

        # 保存功能函数的字典，key是函数名，value是函数对象
        self.__functions = {
            self.foo.__name__: self.foo,
        }

        # zmq端口相关
        self.__context = zmq.Context()

        self.__socketREP = self.__context.socket(zmq.REP)  # 请求回应socket
        self.__socketREP.bind(repAddress)

        self.__socketPUB = self.__context.socket(zmq.PUB)  # 数据广播socket
        self.__socketPUB.bind(pubAddress)

        # 工作线程相关
        self.__active = False  # 服务器的工作状态
        self.__thread = threading.Thread(target=self.run)  # 服务器的工作线程

    # ----------------------------------------------------------------------
    def start(self):
        """启动服务器"""
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

    # ----------------------------------------------------------------------
    def run(self):
        """服务器运行函数"""
        while self.__active:
            # 使用poll来等待事件到达，等待1秒（1000毫秒）
            if not self.__socketREP.poll(1000):
                continue
            # 从请求响应socket收取请求数据
            reqb = self.__socketREP.recv()

            # 序列化解包
            req = self.unpack(reqb)

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


# ----------------------------------------------------------------------
def printLog(content):
    """打印日志"""
    print(datetime.now().strftime("%H:%M:%S"), '\t', content)


# ----------------------------------------------------------------------
def runServer(q=None):
    """运行服务器"""
    repAddress = 'tcp://*:8889'
    pubAddress = 'tcp://*:8890'

    # 创建并启动服务器
    server = Server(repAddress, pubAddress)
    server.start()

    printLog('-' * 50)
    printLog(u'easymirror 服务器已启动')

    # # 进入主循环
    # while True:
    #     printLog(u'请输入exit来关闭服务器')
    #     if input() != 'exit':
    #         continue
    #
    #     printLog(u'确认关闭服务器？yes|no')
    #     if input() == 'yes':
    #         break

    while 1:
        time.sleep(1)


    server.stopServer()


if __name__ == '__main__':
    runServer()
