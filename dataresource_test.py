from easymirror import getFuturesInserter
import time

inserter = getFuturesInserter()
inserter.start()
n = 0
while True:
    index = {
        "time": time.time(),
        "symbol": "rb1705",
    }
    inserter.putIndex(index)
    time.sleep(5)
