#/bin/bash
if [ -f ../tmp/redis/23002.pid ]; then
    kill $(cat ../tmp/redis/23002.pid)
fi;
sleep 1
redis-server ../conf/redis.conf