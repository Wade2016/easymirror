import os

from easymirror import ClientEngine
from argparse import ArgumentParser

path = os.path.abspath(os.path.dirname(__file__))

opt = ArgumentParser(
    prog="easymirror",
    description="Args of easymirror.",
)

# UI开关
opt.add_argument("--conf", default="./conf", help="项目的配置文件路径")

# 生成参数实例
cmdArgs = opt.parse_args()

if __name__ == "__main__":
    ClientEngine(cmdArgs.conf).start()
