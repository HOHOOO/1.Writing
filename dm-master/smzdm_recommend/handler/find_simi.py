# coding=utf-8
import urllib

import tornado

from tornado import web, gen
from base.basehandler import BaseHandler
from biz.find_simi import FindSimi
from util.mysql import TorMysqlClient
from util.redis_client import RedisClient


class FindSimiHandler(BaseHandler):
    """
    twiki: http://twiki.team.bq.com:8081/twiki/Main/Find_simi
    """
    @tornado.web.asynchronous
    @tornado.gen.coroutine
    def get(self):
        user_id = self.get_argument("user_id", "0")
        device_id = urllib.unquote(self.get_argument("device_id", "", False)).replace(" ", "+")
        article_ids = self.get_argument("article_ids", "-1")
        channel_id = self.get_argument("channel_id", "-1")
        scene = self.get_argument("scene", "")

        redis_client = yield RedisClient().get_redis_client()

        result = {
            "error_code": 0,
            "error_msg": "data"
        }

        fs = FindSimi(redis_client, article_ids, scene)

        result["data"] = yield fs.simi_data

        self.jsonify(result)

