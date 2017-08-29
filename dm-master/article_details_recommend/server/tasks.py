# coding=utf-8

import time
import pandas as pd

# from celery import Celery

# reload(sys)
# sys.setdefaultencoding("utf-8")
# sys.path.append("../")
# from celery import Celery
# from celery.schedules import crontab
# from datetime import timedelta
# from tornado.log import access_log
from tornado import gen
from tornado.log import access_log

from base.dotdict import DotDict
from base.utils import create_mysql_connect, create_redis_connect
from base.config import load
from model.yh_model import SimilaryModel, CombineModel, features_with_weight, combine_weights
from model.preprocessing import PreProcess


@gen.coroutine
def rpush_redis(config, key, data, expire_time):

    try:
        if isinstance(config, dict):
            config = DotDict(config)

        redis_connect = create_redis_connect(config)
        access_log.info("key: %s" % key)

        redis_connect.delete(key)
        redis_connect.rpush(key, *data)
        redis_connect.expire(key, expire_time)

    except Exception as e:
        return e
    else:
        return True


@gen.coroutine
def query_and_insert_data(config, key, sql, expire_time):
    """
    查询redis中的key，若存在结果，则直接返回结果；
    不存在结果，则查询mysql中的sql，并将结果组装后存入redis，并返回组装后的结果
    :param config: 包含redis和mysql连接的配置文件
    :param key: redis查询的key
    :param sql: mysql查询的sql
    :param fields: mysql查询结果拼接成dict时所需的key
    :param expire_time: 设置redis中key的过期时间
    :return:
    """

    if isinstance(config, dict):
        config = DotDict(config)

    redis_connect = create_redis_connect(config)

    # logger.info("type: %s, key: %s" % (type(key), key))
    res = redis_connect.lrange(key, 0, -1)
    # logger.info("res: %s" % res)

    if res:  # redis中存在数据则返回
        return res

    mysql_connect = create_mysql_connect(config)
    cursor = mysql_connect.cursor()

    # print "sql: %s" % sql

    cursor.execute(sql)
    data_tuples = cursor.fetchall()
    mysql_connect.close()

    if not data_tuples:  # mysql中没有没有数据则返回None
        return None

    # print "data_tuples:", data_tuples

    def float2str(arr): return map(lambda x: unicode(x), arr)
    value = map(lambda arr: "_".join(float2str(arr)), data_tuples)  # 将推荐列表里的每一项中的列表内容进行拼接
    # logger.info("value: %s" % value)

    # 将组装后的结果存入redis中
    rpush_redis(config, key, value, expire_time)
    return value


@gen.coroutine
def transform_trace_id(config, expire_time, trace_id, trace_id_map_file):
    if isinstance(config, dict):
        config = DotDict(config)

    redis_connect = create_redis_connect(config)

    trans_trace_key = config["redis_key.trace_id_count_key"]
    trans_trace_id = unicode(redis_connect.incr(trans_trace_key))
    redis_connect.expire(trans_trace_key, expire_time)

    # save_trace_map_id(trace_id, trans_trace_id, trace_id_map_file)
    with open(trace_id_map_file, "a") as f:
            f.write(trace_id + ":" + trans_trace_id + "\n")

    return trans_trace_id


# @gen.coroutine
def save_trace_map_id(trace_id, trans_trace_id, trace_id_map_file):
    with open(trace_id_map_file, "a") as f:
            f.write(trace_id + ":" + trans_trace_id + "\n")


@gen.coroutine
def calculate_history_article_res(config, sql, article_id, key, expire_time):
    if isinstance(config, dict):
        config = DotDict(config)

    # print "sql: %s" % sql

    mysql_connect = create_mysql_connect(config)
    data = pd.read_sql(sql, mysql_connect)
    mysql_connect.close()

    # print "type: %s, data['article_id'][0]: %s" % (type(data["article_id"][0]), data["article_id"][0])
    if data["article_id"][0] != int(article_id):
        return None

    pp = PreProcess()
    data = pp(data)
    title_fc_extra_weight = 8
    similary_model = SimilaryModel(data, features_with_weight, src_article_num=None,
                                   rec_article_num=9, title_fc_extra_weight=title_fc_extra_weight,
                                   ndigits=2)

    combine_model = CombineModel(data, combine_weights, similary_model,
                                 src_article_num=1,
                                 rec_article_num=9, ndigits=2)
    combine_res = combine_model.map_articles()

    rec_list = combine_res.values()[0]

    def arr_element2str(arr): return map(lambda x: str(x), arr)
    value = map(lambda arr: "_".join(arr_element2str(arr)), rec_list)

    # 异步将组装后的结果存入redis中
    rpush_redis(config, key, value, expire_time)
    return value



if __name__ == "__main__":
    pass
    # Config = load(path="../config.test")  # load configures
    # app.config_from_object("../config.test/celeryconfig.py")
    # app.start("celery worker --app=celery_app.tasks  --loglevel=info --logfile=/data/logs/article_details/celery_work.log".split())
    # app.start("celery worker --app=celery_app.tasks  --loglevel=info  --logfile=/data/logs/article_details/celery_worker.log --events".split())
