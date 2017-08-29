# coding = utf-8
import tornado

from tornado import web, gen
from tornado.log import gen_log

from base.basehandler import BaseHandler
from biz.subscribe_follow_banner import FollowBanner
from util.redis_client import RedisClient


class BannerHandler(BaseHandler):
    """"
    wiki: http://twiki.team.bq.com:8081/twiki/Main/Subscribe_banner
    redmine: http://redmine.team.bq.com:1080/issues/53056

    """
    @tornado.web.asynchronous
    @tornado.gen.coroutine
    def get(self):
        user_id = self.get_argument("user_id", "0")
        limit = int(self.get_argument("limit", "6"))
        # platform = self.get_argument("platform", "")

        redis_client = yield RedisClient().get_redis_client()

        result = {
            "error_code": 0,
            "error_msg": "",
            "data": []
        }

        try:
            af = FollowBanner(redis_client, user_id, limit)
            follow_content = yield af.follow_content

            result["data"] = follow_content
        except Exception, e:
            result["data"] = 0
            result["error_code"] = 1
            result["error_msg"] = str(e)
            import traceback
            gen_log.error(traceback.format_exc())

        self.jsonify(result)
