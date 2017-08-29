# 接口开发规范

- 写清楚程序部署(尤其是注意事项)
- 所有依赖的包需要写入requirements.txt文件： 注明依赖名称和版本号
- 在server.py中的url需要写注释：接口用途，wiki地址，redmine地址
- debug日志：关键的框架或者业务信息需要有debug日志，程序中全部使用from tornado.log import gen_log来实现日志记录
- error日志：在关键性错误的地方，需要有error的日志
- 代码评审，不合格要重写
- 接口上线告知全员

# 环境部署
```sh
git clone https://gitlab-team.smzdm.com/smzdm/dm.git
cd dm/smzdm_recommend
virtualenv /data/env
source /data/env/bin/activate
pip install -r ./requirements.txt


```
# recommend_system 域名对应的接口服务

| 域名              | port   |  接口描述  |负责人  |状态  |
| :--------------:  | :---:  | :------:   | :----: | :---: |
| recommend_system | 8817  | 明星数据模型服务              | 王伟   | 上线 |




# 目录说明
- config_local: 本机开发配置目录
- config_test: 测试环境配置目录
- config_online: 线上环境配置目录
- util: 和业务相关的工具类目录
- base: 框架使用的基础目录

# mysql 驱动使用

- 压测效果： TorMysqlClient > TornadoMysqlClient > MySQLdb
- 驱动使用： 接口的程序在使用mysql时，建议使用TorMysqlClient进行操作


# redis，mysql测试

```sh
# 启动程序
python server.py --config=./config_local --port=9999 --process=1 --log_file_prefix=/data/logs/smzdm_recommend_test --logging=debug --log-rotate-mode=time

# 测试redis，mysql连接
http://localhost:9999/recommend_system/test?a=guog&db=tornado
http://localhost:9999/recommend_system/test?a=guog&db=tor
http://localhost:9999/recommend_system/test?a=guog&db=mysql
http://localhost:9999/recommend_system/test?a=guog&cache=redis

```


# 设备id获取注意

> 设备id需要将首尾以及中间空格替换为+号, python中需要按着如下语句获取
> device_id = urllib.unquote(self.get_argument("device_id", '', False))
> device_id = device_id.replace(" ", "+")

# 模型启动说明
## 明星数据
> python -m crontab.run_star_data --config=./config_online --log-file-prefix=/data/logs/model_service/run_star_data.log --log-to-stderr=0 --logging=info --process=10 --interval_minutes=10

上面命令可以启动明星数据离线计算服务，其中log-file-prefix表示日志文件，log-to-stderr=0表示日志不输出在屏幕上，logging表示日志级别为info，process表示启动的多进程个数，interval_minutes表示离线计算的间隔
## 相似度模型
> python -m crontab.run_simi_youhui --config=./config_online --log-file-prefix=/data/logs/model_service/run_simi_youhui.log --log-to-stderr=0 --logging=info --process=10 --interval_minutes=30 --timeout=10 --start_day=1 --end_day=0

上面命令可以启动明星数据离线计算服务，其中log-file-prefix表示日志文件，log-to-stderr=0表示日志不输出在屏幕上，logging表示日志级别为info，process表示启动的多进程个数，interval_minutes表示离线计算的间隔，timeout表示计算一篇文章推荐结果时的超时时间（单位为s，超过改时间则不做推荐），start_day表示源文章距离现在最远的时间（单位为day），end_day表示
源文章距离现在最近的时间（单位为day），上面start_day和end_day组合之后表示只为过去1天内的源文章计算推荐结果。

如果想要为过去3天~1天内的源文章计算推荐结果，可以将start_day=3，end_day=1。需要注意的是，如果再次启动了用于计算不同时间段内源文章的推荐结果，需要将log-file-prefix设置为不同的值。
