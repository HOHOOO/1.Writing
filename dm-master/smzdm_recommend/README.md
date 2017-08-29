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
|  | 8819  | 详情页推荐（目前包含好物）                        | 王伟   |
|  | 8818  | 找相似接口                        | 王伟   |
|  | 8817  | 明星数据模型接口                        | 王伟   |
|  | 8816  |                         |    |
|  | 8815  |                         |    |
|  | 8814  |                         |    |
|  | 8813  |                         |    |
|  | 8812  | 先发后审                        | 万歆   |
| recommend_system | 9000  | 详情页推荐数据回传              | 国强   |
|  | 8811  | 用户浮层问卷调查                | 王伟   |
|  | 8810  | 合集后台管理                    | 王伟   | 已下线
|  | 8809  | 分词                            | 万歆   |
|  | 8808  | 推荐合辑                        | 王伟   | 已下线
|  | 8807  | 个性化广告                      | 万歆   |
|  | 8806  | 详情页推荐服务                  | 王伟   |
|  | 8805  | 浮层接口                        | 万歆   |已下线
|  | 8804  | 原首页关联规则个性化feed流      | 德禄   |已下线
|  | 8801  | 原首页个性化feed流不感兴趣接口  | 王伟   |已下线
|  | 8800  | 未知                            | 未知   |
|  | 8000  | 未知                            | 未知   |



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

# 首页编辑精选千人千面接口逻辑
```sh
1. php 接口携请求推荐的接口。如果是ab测试的，注意ab_test key的处理
2. 推荐侧查询该用户画像的偏好
3. 取新数据。从es中按最近时间拉取3页数据，根据用户的偏好等数据进行加权操作，并存到缓存中；（注意是编辑的数据还是推荐侧的数据）
4. 取旧数据。从es中起始时间开始拉取3页数据，根据用户的偏好等数据进行加权操作，并存到缓存中；（注意是编辑的数据还是推荐侧的数据）
5. 存量／增量过期售罄数据同步每个用户的缓存

```

# 设备id获取注意

> 设备id需要将首尾以及中间空格替换为+号, python中需要按着如下语句获取
> device_id = urllib.unquote(self.get_argument("device_id", '', False))
> device_id = device_id.replace(" ", "+")
