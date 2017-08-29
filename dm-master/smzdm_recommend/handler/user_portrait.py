# coding=utf-8
import json
import urllib
import urllib2

import datetime
import requests
import tornado
import tornado.web
import tornado.gen
import tornado.httpclient
from redis import Redis
from tornado.httpclient import HTTPRequest
from tornado.log import access_log

from base.basehandler import BaseHandler
from base.config import Config
from util.mysql import TornadoMysqlClient, MySQL
from util.redis_client import RedisClient


class CatePreferenceHandler(BaseHandler):
    """
    twiki: http://twiki.team.bq.com:8081/twiki/Main/User_portrait
    redmine: http://redmine.team.bq.com:1080/issues/46421
    author: wangwei
    """

    def prepare(self):
        config = Config.flatten()
        d = (datetime.datetime.now() - datetime.timedelta(days=2)).strftime("%Y%m%d")
        # d = "20170417"
        self.base_device_table = config["mysql_table.user_portrait_cate_preference_base_device_table_prefix"] + d
        self.base_user_table = config["mysql_table.user_portrait_cate_preference_base_user_table_prefix"] + d

    @tornado.web.asynchronous
    @tornado.gen.coroutine
    def get(self):
        # 共有参数
        offset = int(self.get_argument("offset", "0"))
        size = int(self.get_argument("size", "1"))
        base = str(self.get_argument("base", "1"))
        threshold = str(self.get_argument("threshold", "0"))
        reverse = str(self.get_argument("reverse", "1"))

        # 查询对某一品类喜好程度在特定阈值之上的用户的特有的参数
        cate_id = str(self.get_argument("cate_id", ""))
        res_type = str(self.get_argument("res_type", ""))

        # 查询一个用户品类偏好特有的参数
        device_id = str(urllib.unquote(self.get_argument("device_id", "", False)).replace(" ", "+"))  # 空格替换为+
        user_id = str(self.get_argument("cate_id", ""))
        unique_user = str(self.get_argument("unique_user", ""))

        msg = {
            "error_code": "0",
            "error_msg": "data",
            "data": ""
        }

        # 参数检查
        if (reverse == "0" and ((not device_id and not user_id) or unique_user not in ("device", "user", "both"))) or (
                reverse == "1" and not cate_id and res_type not in ("device", "user")):
            msg["error_code"] = "1"
            msg["error_msg"] = "parameter format not correct!"
            msg["data"] = "0"
            self.jsonify(msg)
            return

        cate_id = cate_id if cate_id else "0"
        res_type = res_type if res_type else "user"
        user_id = user_id if user_id else "-1"
        unique_user = unique_user if unique_user else "device"

        key = ""
        sql = ""
        args = ()
        # 根据品类id和阈值来反查用户id
        if reverse == "1" and res_type == "user":
            # key = "user_cate_preference_id_%s" % cate_id
            key = ":".join(("user_cate_preference_id", cate_id, threshold, base))
            sql = """select user_id from {table}
                where level_standardization>=%s
                and level_id=%s
                and user_id!='-1'
                and article_channel_id=%s""".format(table=self.base_user_table)
            args = (threshold, cate_id, base)
        # 根据品类id和阈值来反查设备id
        elif reverse == "1" and res_type == "device":
            # key = "device_cate_preference_id_%s" % cate_id
            key = ":".join(("device_cate_preference_id", cate_id, threshold, base))
            sql = """select device_id from {table}
                where level_standardization>=%s
                and level_id=%s
                and article_channel_id=%s""".format(table=self.base_device_table)
            args = (threshold, cate_id, base)
        # 根据设备id查询偏好
        elif reverse == "0" and unique_user == "device":
            # key = "cate_preference_device_%s" % device_id
            key = ":".join(("cate_preference_device", device_id, threshold, base))
            sql = """select level_id as cate_id, level_standardization as score from {table}
                # where device_id=%s
                and level_standardization>=%s
                and article_channel_id=%s""".format(table=self.base_device_table)
            args = (device_id, threshold, base)
        # 根据用户id查询偏好
        elif reverse == "0" and unique_user == "user":
            # key = "cate_preference_user_%s" % user_id
            key = ":".join(("cate_preference_user", user_id, threshold, base))
            sql = """select level_id as cate_id, level_standardization as score from {table}
                where user_id=%s
                and level_standardization>=%s
                and article_channel_id=%s""".format(table=self.base_user_table)
            args = (user_id, threshold, base)
        # 根据设备id+用户id查询偏好
        elif reverse == "0" and unique_user == "both":
            key = "cate_preference_d_%s_u_%s" % (device_id, user_id)
            key = ":".join(("cate_preference", device_id, user_id, threshold, base))
            sql = """select level_id as cate_id, level_standardization as score from {table}
                where device_id=%s
                and user_id=%s
                and level_standardization>=%s
                and article_channel_id=%s""".format(table=self.base_device_table)
            args = (device_id, user_id, threshold, base)

        # con = MySQL.create_mysql_connect()
        # cursor = con.cursor()
        # cursor.execute(sql , args)

        redis_client = yield RedisClient().get_redis_client()

        # 如果是反查用户ID和设备ID，则直接取出结果中的值，否则反序列化成
        if reverse == "1":
            res_split = redis_client.lrange(key, offset, offset + size - 1)
        else:
            res_split = map(lambda s: json.loads(s), redis_client.lrange(key, offset, offset + size - 1))

        access_log.debug("res_split:%s" % res_split)

        # redis 中不存在结果
        if not res_split:
            try:
                res = yield TornadoMysqlClient().fetchall(sql, args)
                # res = cursor.fetchall()
                # [{"score": "0.2636","cate_id": 35},{"score": "0.8346","cate_id": 183}]
                # [{"user_id": "1545591777"},{"user_id": "8757781970"},{"user_id": "7026518331"}]
                # 如果是反查用户ID和设备ID，则直接取出结果中的值，否则序列化成一个字符串
                access_log.debug("res lenght: %s, type: %s" % (len(res), type(res)))
                if reverse == "1":
                    access_log.debug("value: %s" % res[0].values() if res else [])
                    res_total = map(lambda d: d.values()[0], res) if res else []
                    access_log.debug("res_total lenght: %s" % len(res_total))
                    access_log.debug("offset: %s, offset + size: %s" % (offset, offset + size))
                    res_split = res_total[offset: offset + size]
                    access_log.debug("res_split: %s" % res_split)
                else:
                    res_total = map(lambda d: json.dumps(d), res) if res else []
                    res_split = res[offset: offset + size]
                if res_total:
                    redis_client.delete(key)
                    redis_client.rpush(key, *res_total)
                    redis_client.expire(key, 30 * 60)  # 10 min 缓存
            except Exception as e:
                access_log.error(e)
                msg["error_code"] = "1"
                msg["error_msg"] = str(e)
                msg["data"] = "0"

        msg["data"] = res_split
        self.jsonify(msg)

