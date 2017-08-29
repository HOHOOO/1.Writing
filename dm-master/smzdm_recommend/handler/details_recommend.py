# coding=utf-8
import urllib

import tornado

from tornado import web, gen
from tornado.log import gen_log

from base.basehandler import BaseHandler
from biz.details_recommend import DetailsRecommend
from biz.details_recommend_ab import ABModule
from biz_config.details_recommend import *
from util.mysql import TorMysqlClient, MySQL
from util.redis_client import RedisClient


class DetailsRecommendHandler(BaseHandler):
    @tornado.web.asynchronous
    @tornado.gen.coroutine
    def get(self):

        device_id = urllib.unquote(self.get_argument("device_id", "", False)).replace(" ", "+")  # 空格替换为+
        article_id = self.get_argument("article_id", "-1")
        channel_id = int(self.get_argument("channel_id", "-1"))
        user_id = self.get_argument("user_id", "")
        trace_id = self.get_argument("trace_id", "")
        version = self.get_argument("version", "")

        row1_min_num, row2_min_num, row3_min_num = 0, 0, 0
        row1_max_num, row2_max_num, row3_max_num = 0, 0, 0
        if channel_id in YOU_HUI_CHANNEL_ID_LIST:
            row1_min_num = int(self.get_argument("row1_min_num", YOU_HUI_ROW1_MIN_NUM))
            row1_max_num = int(self.get_argument("row1_max_num", YOU_HUI_ROW1_MAX_NUM))
            row2_min_num = int(self.get_argument("row2_min_num", YOU_HUI_ROW2_MIN_NUM))
            row2_max_num = int(self.get_argument("row2_max_num", YOU_HUI_ROW2_MAX_NUM))
            row3_min_num = int(self.get_argument("row3_min_num", YOU_HUI_ROW3_MIN_NUM))
            row3_max_num = int(self.get_argument("row3_max_num", YOU_HUI_ROW3_MAX_NUM))
        elif channel_id == YUAN_CHUANG_CHANNEL_ID:
            row1_min_num = int(self.get_argument("row1_min_num", YUAN_CHUANG_ROW1_MIN_NUM))
            row1_max_num = int(self.get_argument("row1_max_num", YUAN_CHUANG_ROW1_MAX_NUM))
            row2_min_num = int(self.get_argument("row2_min_num", YUAN_CHUANG_ROW2_MIN_NUM))
            row2_max_num = int(self.get_argument("row2_max_num", YUAN_CHUANG_ROW2_MAX_NUM))
            row3_min_num = int(self.get_argument("row3_min_num", YUAN_CHUANG_ROW3_MIN_NUM))
            row3_max_num = int(self.get_argument("row3_max_num", YUAN_CHUANG_ROW3_MAX_NUM))
        elif channel_id == HAO_WU_CHANNEL_ID:
            row1_min_num = int(self.get_argument("row1_min_num", HAO_WU_ROW1_MIN_NUM))
            row1_max_num = int(self.get_argument("row1_max_num", HAO_WU_ROW1_MAX_NUM))
            row2_min_num = 0
            row2_max_num = 0
            row3_min_num = 0
            row3_max_num = 0

        result = {
            "error_code": 0,
            "error_msg": "data"
        }

        try:
            redis_client = yield RedisClient().get_redis_client()
            mysql_client = TorMysqlClient()
            # mysql_client = MySQL()
            ab_module = ABModule(device_id, trace_id, channel_id)

            gen_log.info("ab_module: %s" % ab_module)

            dr = DetailsRecommend(mysql_client, redis_client, device_id, article_id, channel_id, trace_id, version,
                                  ab_module, row1_min_num, row1_max_num, row2_min_num, row2_max_num, row3_min_num,
                                  row3_max_num)
            data = yield dr.rec_data
            result["data"] = data
        except Exception, e:
            result["data"] = 0
            result["error_code"] = 1
            result["error_msg"] = str(e)
            import traceback
            gen_log.error(traceback.format_exc())

        self.jsonify(result)
