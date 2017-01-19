# encoding: UTF-8

import traceback

try:
    from msgpack import packb, unpackb
except ImportError:
    traceback.print_exc()

import pickle

pDumps = pickle.dumps
pLoads = pickle.loads

from json import dumps, loads


########################################################################
class RpcObject(object):
    """
    RPC对象

    提供对数据的序列化打包和解包接口，目前提供了json、msgpack、cPickle三种工具。

    msgpack：性能更高，但通常需要安装msgpack相关工具；
    json：性能略低但通用性更好，大部分编程语言都内置了相关的库。
    cPickle：性能一般且仅能用于Python，但是可以直接传送Python对象，非常方便。

    因此建议尽量使用msgpack，如果要和某些语言通讯没有提供msgpack时再使用json，
    当传送的数据包含很多自定义的Python对象时建议使用cPickle。

    如果希望使用其他的序列化工具也可以在这里添加。
    """

    # ----------------------------------------------------------------------
    def __init__(self):
        """Constructor"""
        # 默认使用msgpack作为序列化工具
        # self.useMsgpack()
        self.usePickle()

    # ----------------------------------------------------------------------
    def pack(self, data):
        """打包"""
        pass

    # ----------------------------------------------------------------------
    def unpack(self, data):
        """解包"""
        pass

    # ----------------------------------------------------------------------
    def __jsonPack(self, data):
        """使用json打包"""
        return dumps(data)

    # ----------------------------------------------------------------------
    def __jsonUnpack(self, data):
        """使用json解包"""
        return loads(data)

    # ----------------------------------------------------------------------
    def __msgpackPack(self, data):
        """使用msgpack打包"""
        return packb(data)

    # ----------------------------------------------------------------------
    def __msgpackUnpack(self, data):
        """使用msgpack解包"""
        return unpackb(data)

    # ----------------------------------------------------------------------
    def __picklePack(self, data):
        """使用cPickle打包"""
        return pDumps(data)

    # ----------------------------------------------------------------------
    def __pickleUnpack(self, data):
        """使用cPickle解包"""
        return pLoads(data)

    # ----------------------------------------------------------------------
    def useJson(self):
        """使用json作为序列化工具"""
        self.pack = self.__jsonPack
        self.unpack = self.__jsonUnpack

    # ----------------------------------------------------------------------
    def useMsgpack(self):
        """使用msgpack作为序列化工具"""
        self.pack = self.__msgpackPack
        self.unpack = self.__msgpackUnpack

    # ----------------------------------------------------------------------
    def usePickle(self):
        """使用cPickle作为序列化工具"""
        self.pack = self.__picklePack
        self.unpack = self.__pickleUnpack


########################################################################
class RemoteException(Exception):
    """RPC远程异常"""

    # ----------------------------------------------------------------------
    def __init__(self, value):
        """Constructor"""
        self.__value = value

    # ----------------------------------------------------------------------
    def __str__(self):
        """输出错误信息"""
        return self.__value
