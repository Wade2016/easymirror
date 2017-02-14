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
- 日志模块为 ```log.py```。
- 在每个进程中有独立的 ```log.py``` 模块可以保存日志句柄。
- ```logbook.Logger``` 仅仅是标记日志来源，并不重要。
- 为了使用装饰器，直接使用 ```log.py``` 中的函数作为装饰器。
- 服务器端和客户端均使用 ```log.py``` 模块，注意兼容问题。

## 参数
- 配置文件默认存放到```conf/``` 文件夹下。
- 在```server_main.py```中可以定制启动参数。

## 索引同步
- 本地其他的数据采集服务向```client``` 插入索引数据。
- 本地指定索引同步策略。如每分钟校验一次前一分钟的所有索引；每天凌晨三点开始校验前一天的全部索引；手动一次性校验所有索引。

### 索引同步实现规范
1. 数据采集服务作为ZMQ下req/rep模式的req端，在采集到新数据后，提供一个队列原始数据队列，供客户端(req端)来获取。
2. 客户端根据同步策略，将数据转换成索引并堆入服务器同步队列。
3. 无论何时，同步队列内只要有索引，客户端都会将其推送到服务器端。
4. 将品种和时间段生成```补齐任务对象```，并推入补齐任务队列。
5. 无论何时，补齐任务队列内只要有任务，客户端都会将其切割为秒的片段，并向服务器查询。
6. 服务器对缺失的数据的索引进行广播，其他客户端则根据索引一齐汇报缺失的数据。
7. 逐条缺失数据回报给客户端，客户端向数据采集提交完整数据。


### 索引同步策略
1. 高实时策略
    - 要求补齐最近2分钟内的数据。
2. 全历史补齐策略
    - 要求补全历史数据。

### 索引格式规范
#### 期货数据采集
服务器端使用```Redis```的```set```数据格式来存储。其格式为
```
"rb1705": {"1486802646.5", "1486802647.", } # 品种编号:{"时间戳", "时间戳"}
```
