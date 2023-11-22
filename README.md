# 对比两个基于Mysql数据库的数据
通过在两个集群指定的ｄｂ　查询相同的SQL，对SQL 返回数据进行对比

# 使用
1. 建议创建一个新的python3 运行环境
```
python3 -m venv new_py_env
source ~/new_py_env/bin/active
```
2. 修改conf.json
```json
{
    
    // souce cluster
    "host_source": "tx-db.cqshokmqgqfv.ap-northeast-1.rds.amazonaws.com:3306",
    // target cluster
    "host_target": "tx-db.cqshokmqgqfv.ap-northeast-1.rds.amazonaws.com:3306",
    // 最多对比多少条数据，要根据程序运行环境的内存，和返回的sql数据大小为参考配置
    "max_count": 100000,
    "dbs": [
        // 配置对比
        // 每个对象里面配置一个数据库
        {
            "db": "business",
            "user": "demo",
            "password": "Demo1234",
            "tables": [
                {
                    "sql": "select * from goods"
                },
                {
                    "sql": "select * from order_event"
                }
            ]
        }
    ]
}

```
3. 保证程序的运行环境和两个数据库集群网络通
```
telnet xxxx 3306
```

4. 运行代码
```shell
python3 statm.py conf.json
```