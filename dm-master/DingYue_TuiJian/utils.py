# coding=utf-8
from random import choice

import MySQLdb
import logging
import redis
import MySQLdb.cursors
import tornado.gen
import tornado.web


from config import Config
# from config.log import *


logger = logging.getLogger("tornado.access")



def create_mysql_connect(config):

    connect = MySQLdb.connect(host=config["mysql.hosts_s"], user=config["mysql.user"],
                                    passwd=config["mysql.password"],
                                    db=config["mysql.db"],
                                    port=int(config["mysql.port"]),
                                    charset="utf8")

    return connect

def create_mysql_158_connect(config):

    connect = MySQLdb.connect(host=config["mysql_158.hosts_s_158"], user=config["mysql_158.user_158"],
                                    passwd=config["mysql_158.password_158"],
                                    db=config["mysql_158.db_158"],
                                    port=int(config["mysql_158.port_158"]),
                                    charset="utf8")

    return connect

def create_mysql_UserDB_connect(config):

    connect = MySQLdb.connect(host=config["mysql_user.host_userdb"], user=config["mysql_user.user_userdb"],
                                    passwd=config["mysql_user.password_userdb"],
                                    db=config["mysql_user.db_userdb"],
                                    port=int(config["mysql_user.port_userdb"]),
                                    charset="utf8")
    return connect

def create_redis_connect(config):
    redis_connect = redis.Redis(host = config["redis.host"],
                                port = int(config["redis.port"]),
                                db = int(config["redis.db"]))
    return redis_connect



if __name__ == "__main__":
    # create_mysql_connect(Config)
    # create_mysql_UserDB_connect(Config)
    # create_redis_connect(Config)
    pass