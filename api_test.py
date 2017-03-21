import os
import time
from easymirror import EasyctpApi

api = EasyctpApi(os.path.join(os.getcwd(), "conf"))
api.start()

n = 0
while True:
    api.brocastcastTicker({"time": time.time(), "msg": "test"})
    n += 1
    time.sleep(5)
