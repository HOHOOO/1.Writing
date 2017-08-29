# coding=utf-8
from random import choice

import MySQLdb
import logging
import redis
import tornado.gen
import tornado.web
# import tornado_mysql
# import tornadoredis

from base.config import Config
# from config.log import *


logger = logging.getLogger("tornado.access")

# redis_connect = None
# tornado_redis_connect = None


def create_mysql_connect(config):

    host = choice(config["mysql.hosts_s"].split(","))  # 随机选择一个host
    connect = MySQLdb.connect(host=host, user=config["mysql.user"],
                                    passwd=config["mysql.password"],
                                    db=config["mysql.db"],
                                    port=int(config["mysql.port"]),
                                    charset="utf8")
    return connect


def create_redis_connect(config):
    redis_connect = redis.Redis(host=config["redis.host"],
                                port=int(config["redis.port"]),
                                db=int(config["redis.db"]))

    # logger.info("redis_connect: %s" % redis_connect)
    return redis_connect


def create_mysql_master_connect(config):
    connect = MySQLdb.connect(host=config["mysql.host_m"], user=config["mysql.user"],
                              passwd=config["mysql.password"],
                              db=config["mysql.db"],
                              port=int(config["mysql.port"]),
                              charset="utf8")
    return connect


# def create_tornado_mysql_connect(config):
#     host = choice(config["mysql.hosts_s"].split(","))
#     connect = tornado_mysql.connect(host=host, user=config["mysql.user"],
#                                     passwd=config["mysql.password"],
#                                     db=config["mysql.db"],
#                                     port=config["mysql.port"],
#                                     charset="utf8")
#     return connect


# def create_tornado_redis_connect(config):
#     """
#     see https://github.com/leporo/tornado-redis
#     """
#     tornado_redis_connect = tornadoredis.Client(host=Config["redis.host"],
#                                                 port=int(Config["redis.port"]),
#                                                 selected_db=int(Config["redis.db"]))
#     tornado_redis_connect.connect()
#     # logger.info("tornado_redis_connect: %s" % tornado_redis_connect)
#     return tornado_redis_connect


if __name__ == "__main__":
    # create_mysql_connect()
    pass