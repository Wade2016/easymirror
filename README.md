# easymirror
进行时间序列数据库的索引同步

## 运行
运行整个服务
```
python main.py
```

单独运行一个服务，如单独运行期货行情的索引同步:
```
python start_futures.py
```


## 服务
- 每个服务使用 ```server.py``` 中的类作为基类，进一步进行定制开发。
- 每个服务一个独立的进程。
- 主进程中```serverEngine.py``` 来启动并管理其他进程，包括启动参数传递之类。
- 运行```./reverse.sh```可以生成架构中类依赖的UML图 __classes_mirror.png__ 和 __packages_miooro.png__

## 日志功能
- 日志文件默认存放到 ```log/``` 文件夹下。
- 日志分成 ```serverlog.py``` 和 ```clientlog.py```。
- 在进程中生成日志对象，然后通过装饰器使用对应的句柄。
- ```logbook.Logger``` 仅仅是标记日志来源，并不重要。
- 为了使用装饰器，将```ServerLog``` 实例直接保存在 ```server.py``` 的全局变量中。

## 参数
- 配置文件默认存放到```conf/``` 文件夹下。
- 在```main.py```中可以定制启动参数。
