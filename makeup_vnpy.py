# encoding: UTF-8
import os
from easymirror import api as emapi

# confDir = os.path.join(os.getcwd(), 'tmp/conf.json')
confDir = None
em = emapi.makeup("vnpy", confDir)
