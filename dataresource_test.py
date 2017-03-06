from easymirror import getFuturesInserter
import time


inserter = getFuturesInserter()
inserter.start()
n = 0
while n < 10:

    inserter.putIndex('%s' % n)
    time.sleep(5)
    n += 1

