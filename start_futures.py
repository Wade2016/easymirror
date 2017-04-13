import sys
import json

from easymirror import FuturesServer
from logbook import *
from logbook.queues import *

with open("./conf/server.json", 'r') as f:
    conf = json.load(f)

# 订阅屏幕日志
subscriber = ZeroMQSubscriber('tcp://{}'.format(conf["futures"]["logzmqhost"]))
streamHandler = StreamHandler(sys.stdout, level="DEBUG")
subscriber.dispatch_in_background(streamHandler)

futures = FuturesServer.process(**conf["futures"])
