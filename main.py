import easymirror
import time
import datetime
import json

# 获得 em 服务实例
process = easymirror.getMirror("easyctp", conf="./conf/conf.json")
process.start()

while 1:
    easymirror.pushTickerIndex({'time': time.time()})
    time.sleep(1)

