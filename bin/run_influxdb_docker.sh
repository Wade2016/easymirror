#!/bin/bash


# 重启容器，没有容器则启用镜像重新生成容器
docker restart easymirror || docker run --name easymirror \
    -p 8083:8083 -p 8086:8086 \
    -v $PWD/../conf/influxdb.conf:/etc/influxdb/influxdb.conf:ro \
    -v $PWD/../tmp/influxdb/:/var/lib/influxdb \
    -d influxdb -config /etc/influxdb/influxdb.conf