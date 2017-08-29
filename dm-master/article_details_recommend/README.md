# 安装依赖

```bash
pip install -r ./requirements.txt
```

## 启动接口服务

测试环境请运行:
```bash
$PRO_HOME/$ python run_server.py --config=./config_test --port=8806 --process=0 --log_file_prefix=/data/logs/article_details/server.log --log-rotate-mode=time --logging=info
```

线上环境请运行:

```bash
$PRO_HOME/$ screen -S article_details_recommend # 创建一个名为article_details_recommend的screen session
$PRO_HOME/$ sudo python run_server.py --config=./config_online --port=8806 --process=0 --log_file_prefix=/data/logs/article_details/server.log --log-rotate-mode=time --logging=info

Ctrl+A+D # 将当前的screen session放到后台执行
```


## 部署接口服务


## 启动模型

正常情况下模型不需要手动来启动的，因为会将它配置在crontab中来定时执行。
```bash
$PRO_HOME/$ sudo python run_model.py --config=./config_test --log-file-prefix=/data/logs/article_details/model/model.log --logging=debug
```

## 删除存入mysql中的过多的数据

正常情况下脚本不需要手动来启动的，因为会将它配置在crontab中来定时执行。
```bash
$PRO_HOME/$ sh delete_recommend_data.sh
```