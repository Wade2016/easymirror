import time
import os
from easymirror import api as emapi
import datetime

confDir = os.path.join(os.getcwd(), 'conf/conf.json')
emapi.getMirror("vnpy", confDir).start()
while True:
    emapi.pushTickerIndex({
        "datetime": datetime.datetime(2017, 3, 30, 14, 30, 25, 500000),
        "symbol": "rb1710"
    })
    time.sleep(3)
