# encoding: UTF-8
# import time
import os
from easymirror import api as emapi
import datetime
import pymongo

# confDir = os.path.join(os.getcwd(), 'tmp/conf.json')
confDir = None
em = emapi.makeup("vnpy", confDir)
# time.sleep(1)
# em.start()
