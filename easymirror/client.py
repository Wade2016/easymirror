# encoding: utf-8

import os
import threading
import traceback

from logbook import Logger
import zmq

from easymirror.rpc import RpcObject, RemoteException
import easymirror.log as log


########################################################################
class BaseClient(RpcObject):
    """RPC客户端"""

    @property
    def name(self):
        return self.__class__.__name__

    def __init__(self, reqAddress, subAddress, vaild=True, logdir="../log", logzmqhost=None, insertAddress=None):
        """Constructor"""

        self.__initLog(logdir, logzmqhost)

        super(BaseClient, self).__init__()

        assert isinstance(vaild, bool)
        self.vaild = vaild

        self.logdir = logdir

        # 使用 JSON 解包
        self.useMsgpack()

        # zmq端口相关
        self.__reqAddress = reqAddress
        self.__subAddress = subAddress
        self.__insertAddress = insertAddress

        self.__context = zmq.Context()
        self.__socketREQ = self.__context.socket(zmq.REQ)  # 请求发出socket
        self.__socketSUB = self.__context.socket(zmq.SUB)  # 广播订阅socket

        # 工作线程相关，用于处理服务器推送的数据
        self.__active = False  # 客户端的工作状态
        self.__thread = threading.Thread(target=self.run)  # 客户端的工作线程

        # 数据源插入数据索引
        self.__socketInsertREP = self.__context.socket(zmq.REP)  # 响应数据索引插入
        self.__activeInsert = False  # 客户端的工作状态
        self.__threadInsert = threading.Thread(target=self.insert)  # 客户端的工作线程

    # ----------------------------------------------------------------------
    def __initLog(self, logdir, logzmqhost):
        """
        在子进程中初始化客户端的句柄

        :param logdir:
        :param logzmqhost:
        :return:
        """

        logfile = os.path.join(logdir, '{}.log'.format(self.name))
        log.initLog(logfile, logzmqhost)

        # 使用服务名作为日志的来源名
        self.log = Logger(self.name)

    # ----------------------------------------------------------------------
    def __getattr__(self, name):
        """实现远程调用功能"""

        # 执行远程调用任务
        def dorpc(*args, **kwargs):
            # 生成请求
            req = [name, args, kwargs]

            # 序列化打包请求
            reqb = self.pack(req)

            # 发送请求并等待回应
            self.__socketREQ.send(reqb)
            repb = self.__socketREQ.recv()

            # 序列化解包回应
            rep = self.unpack(repb)

            # 若正常则返回结果，调用失败则触发异常
            if rep[0]:
                return rep[1]
            else:
                raise RemoteException(rep[1])

        return dorpc

    # ----------------------------------------------------------------------
    def start(self):
        """启动客户端"""
        # 连接端口
        self.__socketREQ.connect(self.__reqAddress)
        self.__socketSUB.connect(self.__subAddress)

        # 插入数据
        self.__socketInsertREP.bind(self.__insertAddress)

        # 将服务器设为启动
        self.__active = True

        # 启动工作线程
        if not self.__thread.isAlive():
            self.__thread.start()

        self.__activeInsert = True
        if not self.__threadInsert.isAlive():
            self.__threadInsert.start()

    # ----------------------------------------------------------------------
    def stop(self):
        """停止客户端"""
        # 将客户端设为停止
        self.__active = False

        # 等待工作线程退出
        if self.__thread.isAlive():
            self.__thread.join()

        if self.__threadInsert.isAlive():
            self.__threadInsert.join()

    @log.stdout
    @log.file
    def run(self):
        while self.__active:
            # 接受广播
            self.subRev()
            # 发送数据
            try:
                self.reqSend()
            except:
                traceback.print_exc()

    # ----------------------------------------------------------------------
    def subRev(self):
        """客户端运行函数"""
        # 使用poll来等待事件到达，等待1秒（1000毫秒）
        if not self.__socketSUB.poll(1000):
            return

        # 从订阅socket收取广播数据
        topic, datab = self.__socketSUB.recv_multipart()

        # 序列化解包
        data = self.unpack(datab)

        self.log.debug("获得订阅消息{}".format(str(data)))

        # 调用回调函数处理
        self.callback(topic, data)

    @log.stdout
    def reqSend(self):
        """
        发送数据
        :return:
        """
        # req = ['foo', ('argg'), {"kwargs": 1}]
        #
        # # 序列化
        # reqb = self.pack(req)
        #
        # self.log.debug("往服务器端发送数据")
        #
        # self.__socketREQ.send(reqb)
        #
        # self.log.debug("等待数据返回")

        # datab = self.__socketREQ.recv_json()

        # # 序列化解包
        # rep = self.unpack(datab)
        #
        # if rep[0]:
        #     return rep[1]
        # else:
        #     raise RemoteException(rep[1])

    def callback(self, topic, data):
        """回调函数，必须由用户实现"""
        raise NotImplementedError

    # ----------------------------------------------------------------------
    def subscribeTopic(self, topic):
        """
        订阅特定主题的广播数据

        可以使用topic=''来订阅所有的主题
        """
        self.__socketSUB.setsockopt(zmq.SUBSCRIBE, topic)

    @classmethod
    def process(cls, queue=None, *args, **kwargs):
        """客户端主程序入口"""

        # 创建客户端
        client = cls(*args, **kwargs)

        client.subscribeTopic(b'')
        client.start()
        if queue is not None:
            queue.get()
        else:
            while True:
                if input("输入exit退出:") != "exit":
                    continue
                if input("是否退出(yes/no):") == "yes":
                    break
        client.stopServer()

    def insert(self):
        """
        插入数据
        :return:
        """
        while self.__activeInsert:
            self._insert()

    @log.stdout
    @log.file
    def _insert(self):
        if not self.__socketInsertREP.poll(1000):
            return
        # 数据提交的数据索引
        reqb = self.__socketInsertREP.recv_string()

        self.log.debug("收到时间索引 {}".format(reqb))

        self.__socketInsertREP.send_string("OK")
