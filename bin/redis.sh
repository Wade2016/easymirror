#/bin/bash
if [ -d ../tmp/redis/23002.pid ]; then
    kill $(cat ../tmp/redis/23002.pid)
fi
redis-server ../conf/23002.conf