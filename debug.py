import easymirror
import time

easymirror.api._startMirror("easyctp", "./conf/conf.json")
while True:
    time.sleep(1)