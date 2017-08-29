# coding = utf-8
import tornado

from tornado import web, gen
from tornado.log import gen_log

from base.basehandler import BaseHandler
from biz.subscribe_add_follow import AddFollow
from util.redis_client import RedisClient


class AddFollowHandler(BaseHandler):
    """"
    wiki: http://twiki.team.bq.com:8081/twiki/Main/Subscribe_add_follow

    """
    @tornado.web.asynchronous
    @tornado.gen.coroutine
    def get(self):
        user_id = self.get_argument("user_id", "0")
        offset = int(self.get_argument("offset", "0"))
        limit = int(self.get_argument("limit", "60"))
        platform = self.get_argument("platform", "")

        # redis_client = yield RedisClient().get_redis_client()

        result = {
            "error_code": 0,
            "error_msg": "",
            "data": []
        }

        try:
            af = AddFollow(user_id, offset, limit, platform)
            follow_content = yield af.follow_content

            result["data"] = follow_content
        except Exception, e:
            result["data"] = 0
            result["error_code"] = 1
            result["error_msg"] = str(e)
            import traceback
            gen_log.error(traceback.format_exc())

        self.jsonify(result)
