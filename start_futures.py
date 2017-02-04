from easymirror import FuturesServer

import json
import os
print(os)
with open("./conf/server.json", 'r') as f:
    conf = json.load(f)
futures = FuturesServer.process(**conf["futures"])
