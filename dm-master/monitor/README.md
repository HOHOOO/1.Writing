

# 推荐监控

## 接口监控

```bash
# 机器: logtash机器10.9.95.215
# 程序: /data/webroot/dm/monitor/shell/rec_api.sh
# 执行： crontab 方式，每分钟执行一次
```

## 数据同步监控

```bash
# 机器： logstash机器/data/webroot/dm/monitor/shell/monitor_home_sync.sh
# 程序： /data/webroot/dm/monitor/shell/monitor_home_sync.sh
# 执行： crontab 方式，每2分钟执行一次
```

## 业务监控

```bash
# 机器: 在接口服务器10.10.125.169 上
# 程序: python consume_msg.py ./config_online
```
