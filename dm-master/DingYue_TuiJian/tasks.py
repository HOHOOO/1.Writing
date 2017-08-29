# coding=utf-8

import datetime
import time
import copy
# from celery import Celery

# reload(sys)
# sys.setdefaultencoding("utf-8")
# sys.path.append("../")
# from celery import Celery
# from celery.schedules import crontab
# from datetime import timedelta
# from tornado.log import access_log
import MySQLdb
import json
from tornado import gen
from tornado.log import access_log

from dotdict import DotDict
from utils import create_mysql_connect, create_redis_connect, create_mysql_UserDB_connect,create_mysql_158_connect
from config import load


# from model.combine import StartModel

@gen.coroutine
def rpush_redis(config, key, data, time):
    try:
        if isinstance(config, dict):
            config = DotDict(config)

        redis_connect = create_redis_connect(config)
        access_log.info("key: %s" % key)

        redis_connect.delete(key)
        redis_connect.rpush(key, *data)
        redis_connect.expire(key, time)

    except Exception as e:
        return e
    else:
        return True

@gen.coroutine
def user_is_follow_data(config, key, user_is_follow_sql, user_id, time):
    if isinstance(config, dict):
        config = DotDict(config)
    # print key
    redis_connect = create_redis_connect(config)
    res = redis_connect.lrange(key, 0, -1)

    if res:
        return res
    mysql_connect = create_mysql_connect(config)
    cursor = mysql_connect.cursor()
    # print "sql: %s" % user_is_follow_sql
    sql = user_is_follow_sql + " " + str(user_id)
    # print "sql: %s" % sql
    cursor.execute(sql)
    date_tuples = cursor.fetchall()
    mysql_connect.close()
    if not date_tuples:  # mysql中没有没有数据则返回None
        return None

    # print  date_tuples
    def float2str(arr):
        return map(lambda x: unicode(x), arr)

    # value = json.dumps(date_tuples)
    value = map(lambda arr: "_".join(float2str(arr)), date_tuples)
    rpush_redis(config, key, value, time)
    return value

@gen.coroutine
def select_data(config, key, sql, offset, limit, time):
    if isinstance(config, dict):
        config = DotDict(config)
    # redis_connect = create_redis_connect(config)
    # res = redis_connect.lrange(key, offset, limit)
    #
    # if res:
    #     return res
    mysql_connect = create_mysql_connect(config)
    cursor = mysql_connect.cursor()
    select_data_sql = sql + " " + "limit " + str(limit) + " " + "offset " + str(offset)
    # print "select_data_sql: %s" % select_data_sql
    cursor.execute(select_data_sql)
    date_tuples = cursor.fetchall()
    mysql_connect.close()
    if not date_tuples:  # mysql中没有没有数据则返回None
        return None

    def float2str(arr):
        return map(lambda x: unicode(x), arr)

    # print date_tuples
    value = map(lambda arr: "_".join(float2str(arr)), date_tuples)
    # rpush_redis(config, key, value, time)
    return value


@gen.coroutine
def insert_select_data(config, key, user_is_follow_sql, time, limit):
    if isinstance(config, dict):
        config = DotDict(config)
    # print key
    redis_connect = create_redis_connect(config)
    res = redis_connect.lrange(key, 0, -1)

    if res:
        return res
    mysql_connect = create_mysql_connect(config)
    cursor = mysql_connect.cursor()
    # print "sql: %s" % user_is_follow_sql
    sql = user_is_follow_sql + " " + "limit " + str(limit)
    # print "sql: %s" % sql
    cursor.execute(sql)
    date_tuples = cursor.fetchall()
    mysql_connect.close()
    if not date_tuples:  # mysql中没有没有数据则返回None
        return None

    # print  date_tuples
    def float2str(arr):
        return map(lambda x: unicode(x), arr)

    # value = json.dumps(date_tuples)
    value = map(lambda arr: "_".join(float2str(arr)), date_tuples)
    rpush_redis(config, key, value, time)
    return value


@gen.coroutine
def one_key_concern_158(config, key,sql,time):
    if isinstance(config, dict):
        config = DotDict(config)

    redis_connect = create_redis_connect(config)

    mysql_connect = create_mysql_158_connect(config)
    cursor = mysql_connect.cursor()
    cursor.execute(sql)
    data_tuples = cursor.fetchall()

    mysql_connect.close()

    if not  data_tuples:  # mysql中没有没有数据则返回None
        return None

    # print "data_tuples:",

    def float2str(arr):
        return map(lambda x: unicode(x), arr)

    value = map(lambda arr: "_".join(float2str(arr)), data_tuples)  # 将推荐列表里的每一项中的列表内容进行拼接
    # logger.info("value: %s" % value)

    # 将组装后的结果存入redis中
    # rpush_redis(config, key, value, time)
    return value

@gen.coroutine
def select_UserDB(config, key,sql,time):
    if isinstance(config, dict):
        config = DotDict(config)

    # redis_connect = create_redis_connect(config)

    mysql_connect = create_mysql_UserDB_connect(config)
    cursor = mysql_connect.cursor()
    cursor.execute(sql)
    data_tuples = cursor.fetchall()

    mysql_connect.close()

    if not  data_tuples:  # mysql中没有没有数据则返回None
        return None

    # print "data_tuples:",

    def float2str(arr):
        return map(lambda x: unicode(x), arr)

    value = map(lambda arr: "_".join(float2str(arr)), data_tuples)  # 将推荐列表里的每一项中的列表内容进行拼接
    # logger.info("value: %s" % value)

    # 将组装后的结果存入redis中
    # rpush_redis(config, key, value, time)
    return value

@gen.coroutine
def one_key_concern(config, key,sql,time):
    if isinstance(config, dict):
        config = DotDict(config)

    redis_connect = create_redis_connect(config)

    mysql_connect = create_mysql_connect(config)
    cursor = mysql_connect.cursor()
    cursor.execute(sql)
    data_tuples = cursor.fetchall()

    mysql_connect.close()

    if not  data_tuples:  # mysql中没有没有数据则返回None
        return None

    # print "data_tuples:",

    def float2str(arr):
        return map(lambda x: unicode(x), arr)

    value = map(lambda arr: "_".join(float2str(arr)), data_tuples)  # 将推荐列表里的每一项中的列表内容进行拼接
    # logger.info("value: %s" % value)

    # 将组装后的结果存入redis中
    # rpush_redis(config, key, value, time)
    return value

if __name__ == "__main__":
    pass
    # Config = load(path="config.test")  # load configures
    # app.config_from_object("../config.test/celeryconfig.py")
    # app.start("celery worker --app=celery_app.tasks  --loglevel=info --logfile=/data/logs/article_details/celery_work.log".split())
    # app.start("celery worker --app=celery_app.tasks  --loglevel=info  --logfile=/data/logs/article_details/celery_worker.log --events".split())
