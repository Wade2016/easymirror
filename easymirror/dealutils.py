# encoding: UTF-8
import os


def getConfPath():
    """

    >>> getConfPath() == os.path.join(os.getcwd(), '../conf')
    True

    :return:
    """
    pwd = os.path.split(__file__)[0]

    return os.path.join(pwd, "../conf")
