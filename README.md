# easymirror
进行时间序列数据库的索引同步

## 运行
运行整个服务
```
python server_main.py
```

单独运行一个服务，如单独运行期货行情的索引同步:
```
python start_futures.py
```

运行客户端
```
python client_main.py
```

## 服务端
- 每个服务使用 ```server.py``` 中的类作为基类，进一步进行定制开发。
- 每个服务一个独立的进程。
- 主进程中```serverEngine.py``` 来启动并管理其他进程，包括启动参数传递之类。
- 运行```./reverse.sh```可以生成架构中类依赖的UML图 __classes_mirror.png__ 和 __packages_miooro.png__

## 客户端
- 架构类似服务端，使用 ```clientengine.py``` 来管理各个 ```server``` 对应的 ```client```。


## 日志功能
- 日志文件默认存放到 ```log/``` 文件夹下。
- 日志分成 ```serverlog.py``` 和 ```clientslog.py```。
- 在每个进程中有独立的 ```serverlog.py``` 模块可以保存日志句柄。
- ```logbook.Logger``` 仅仅是标记日志来源，并不重要。
- 为了使用装饰器，直接使用 ```serverlog.py``` 中的函数作为装饰器

## 参数
- 配置文件默认存放到```conf/``` 文件夹下。
- 在```server_main.py```中可以定制启动参数。

## 索引同步
1. 本地其他的数据采集服务向```client``` 插入索引数据。
2. 本地指定索引同步策略。如每分钟校验一次前一分钟的所有索引；每天凌晨三点开始校验前一天的全部索引；手动一次性校验所有索引。